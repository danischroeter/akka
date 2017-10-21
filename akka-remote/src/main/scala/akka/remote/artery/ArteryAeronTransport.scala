/**
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.Done
import akka.actor.Address
import akka.actor.Cancellable
import akka.actor.ExtendedActorSystem
import akka.event.Logging
import akka.remote.RemoteActorRefProvider
import akka.remote.artery.compress._
import akka.stream.KillSwitches
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.OptionVal

import io.aeron.Aeron
import io.aeron.AvailableImageHandler
import io.aeron.CncFileDescriptor
import io.aeron.CommonContext
import io.aeron.Image
import io.aeron.UnavailableImageHandler
import io.aeron.driver.MediaDriver
import io.aeron.driver.ThreadingMode
import io.aeron.exceptions.ConductorServiceTimeoutException
import io.aeron.exceptions.DriverTimeoutException
import org.agrona.ErrorHandler
import org.agrona.IoUtil
import org.agrona.concurrent.BackoffIdleStrategy

/**
 * INTERNAL API
 */
private[remote] class ArteryAeronTransport(_system: ExtendedActorSystem, _provider: RemoteActorRefProvider)
  extends ArteryTransport(_system, _provider) {
  import AeronSource.ResourceLifecycle
  import ArteryTransport._
  import Decoder.InboundCompressionAccess
  import FlightRecorderEvents._

  private[this] val mediaDriver = new AtomicReference[Option[MediaDriver]](None)
  @volatile private[this] var aeron: Aeron = _
  @volatile private[this] var aeronErrorLogTask: Cancellable = _
  @volatile private[this] var areonErrorLog: AeronErrorLog = _

  private val taskRunner = new TaskRunner(system, settings.Advanced.IdleCpuLevel)

  private def inboundChannel = s"aeron:udp?endpoint=${bindAddress.address.host.get}:${bindAddress.address.port.get}"
  private def outboundChannel(a: Address) = s"aeron:udp?endpoint=${a.host.get}:${a.port.get}"

  override protected def startTransport(): Unit = {
    startMediaDriver()
    startAeron()
    topLevelFREvents.loFreq(Transport_AeronStarted, NoMetaData)
    startAeronErrorLog()
    topLevelFREvents.loFreq(Transport_AeronErrorLogStarted, NoMetaData)
    taskRunner.start()
    topLevelFREvents.loFreq(Transport_TaskRunnerStarted, NoMetaData)
  }

  private def startMediaDriver(): Unit = {
    if (settings.Advanced.EmbeddedMediaDriver) {
      val driverContext = new MediaDriver.Context
      if (settings.Advanced.AeronDirectoryName.nonEmpty) {
        driverContext.aeronDirectoryName(settings.Advanced.AeronDirectoryName)
      } else {
        // create a random name but include the actor system name for easier debugging
        val uniquePart = UUID.randomUUID().toString
        val randomName = s"${CommonContext.AERON_DIR_PROP_DEFAULT}-${system.name}-$uniquePart"
        driverContext.aeronDirectoryName(randomName)
      }
      driverContext.clientLivenessTimeoutNs(settings.Advanced.ClientLivenessTimeout.toNanos)
      driverContext.imageLivenessTimeoutNs(settings.Advanced.ImageLivenessTimeout.toNanos)
      driverContext.driverTimeoutMs(settings.Advanced.DriverTimeout.toMillis)

      val idleCpuLevel = settings.Advanced.IdleCpuLevel
      if (idleCpuLevel == 10) {
        driverContext
          .threadingMode(ThreadingMode.DEDICATED)
          .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
          .receiverIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
          .senderIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
      } else if (idleCpuLevel == 1) {
        driverContext
          .threadingMode(ThreadingMode.SHARED)
          .sharedIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
      } else if (idleCpuLevel <= 7) {
        driverContext
          .threadingMode(ThreadingMode.SHARED_NETWORK)
          .sharedNetworkIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
      } else {
        driverContext
          .threadingMode(ThreadingMode.DEDICATED)
          .receiverIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
          .senderIdleStrategy(TaskRunner.createIdleStrategy(idleCpuLevel))
      }

      val driver = MediaDriver.launchEmbedded(driverContext)
      log.info("Started embedded media driver in directory [{}]", driver.aeronDirectoryName)
      topLevelFREvents.loFreq(Transport_MediaDriverStarted, driver.aeronDirectoryName().getBytes("US-ASCII"))
      if (!mediaDriver.compareAndSet(None, Some(driver))) {
        throw new IllegalStateException("media driver started more than once")
      }
    }
  }

  private def aeronDir: String = mediaDriver.get match {
    case Some(driver) ⇒ driver.aeronDirectoryName
    case None         ⇒ settings.Advanced.AeronDirectoryName
  }

  private def stopMediaDriver(): Unit = {
    // make sure we only close the driver once or we will crash the JVM
    val maybeDriver = mediaDriver.getAndSet(None)
    maybeDriver.foreach { driver ⇒
      // this is only for embedded media driver
      driver.close()

      try {
        if (settings.Advanced.DeleteAeronDirectory) {
          IoUtil.delete(new File(driver.aeronDirectoryName), false)
          topLevelFREvents.loFreq(Transport_MediaFileDeleted, NoMetaData)
        }
      } catch {
        case NonFatal(e) ⇒
          log.warning(
            "Couldn't delete Aeron embedded media driver files in [{}] due to [{}]",
            driver.aeronDirectoryName, e.getMessage)
      }
    }
  }

  // TODO: Add FR events
  private def startAeron(): Unit = {
    val ctx = new Aeron.Context

    ctx.driverTimeoutMs(settings.Advanced.DriverTimeout.toMillis)

    ctx.availableImageHandler(new AvailableImageHandler {
      override def onAvailableImage(img: Image): Unit = {
        if (log.isDebugEnabled)
          log.debug(s"onAvailableImage from ${img.sourceIdentity} session ${img.sessionId}")
      }
    })
    ctx.unavailableImageHandler(new UnavailableImageHandler {
      override def onUnavailableImage(img: Image): Unit = {
        if (log.isDebugEnabled)
          log.debug(s"onUnavailableImage from ${img.sourceIdentity} session ${img.sessionId}")

        // freeSessionBuffer in AeronSource FragmentAssembler
        streamMatValues.get.valuesIterator.foreach {
          case InboundStreamMatValues(resourceLife, _) ⇒
            resourceLife.foreach(_.onUnavailableImage(img.sessionId))
        }
      }
    })

    ctx.errorHandler(new ErrorHandler {
      private val fatalErrorOccured = new AtomicBoolean

      override def onError(cause: Throwable): Unit = {
        cause match {
          case e: ConductorServiceTimeoutException ⇒ handleFatalError(e)
          case e: DriverTimeoutException           ⇒ handleFatalError(e)
          case _: AeronTerminated                  ⇒ // already handled, via handleFatalError
          case _ ⇒
            log.error(cause, s"Aeron error, ${cause.getMessage}")
        }
      }

      private def handleFatalError(cause: Throwable): Unit = {
        if (fatalErrorOccured.compareAndSet(false, true)) {
          if (!isShutdown) {
            log.error(cause, "Fatal Aeron error {}. Have to terminate ActorSystem because it lost contact with the " +
              "{} Aeron media driver. Possible configuration properties to mitigate the problem are " +
              "'client-liveness-timeout' or 'driver-timeout'. {}",
              Logging.simpleName(cause),
              if (settings.Advanced.EmbeddedMediaDriver) "embedded" else "external",
              cause.getMessage)
            taskRunner.stop()
            aeronErrorLogTask.cancel()
            system.terminate()
            throw new AeronTerminated(cause)
          }
        } else
          throw new AeronTerminated(cause)
      }
    })

    ctx.aeronDirectoryName(aeronDir)
    aeron = Aeron.connect(ctx)
  }

  // TODO Add FR Events
  private def startAeronErrorLog(): Unit = {
    areonErrorLog = new AeronErrorLog(new File(aeronDir, CncFileDescriptor.CNC_FILE), log)
    val lastTimestamp = new AtomicLong(0L)
    import system.dispatcher
    aeronErrorLogTask = system.scheduler.schedule(3.seconds, 5.seconds) {
      if (!isShutdown) {
        val newLastTimestamp = areonErrorLog.logErrors(log, lastTimestamp.get)
        lastTimestamp.set(newLastTimestamp + 1)
      }
    }
  }

  override protected def outboundTransportSink(outboundContext: OutboundContext, streamId: Int,
                                               bufferPool: EnvelopeBufferPool): Sink[EnvelopeBuffer, Future[Done]] = {

    // FIXME in previous impl giveUpAfter was Duration.Inf for the outboundControl ?
    val giveUpAfter =
      if (streamId == controlStreamId) settings.Advanced.GiveUpSystemMessageAfter
      else settings.Advanced.GiveUpMessageAfter
    Sink.fromGraph(new AeronSink(outboundChannel(outboundContext.remoteAddress), streamId, aeron, taskRunner,
      bufferPool, giveUpAfter, createFlightRecorderEventSink()))
  }

  def aeronSource(streamId: Int, pool: EnvelopeBufferPool): Source[EnvelopeBuffer, AeronSource.ResourceLifecycle] =
    Source.fromGraph(new AeronSource(inboundChannel, streamId, aeron, taskRunner, pool,
      createFlightRecorderEventSink(), aeronSourceSpinningStrategy))

  private def aeronSourceSpinningStrategy: Int =
    if (settings.Advanced.InboundLanes > 1 || // spinning was identified to be the cause of massive slowdowns with multiple lanes, see #21365
      settings.Advanced.IdleCpuLevel < 5) 0 // also don't spin for small IdleCpuLevels
    else 50 * settings.Advanced.IdleCpuLevel - 240

  override protected def runInboundStreams(): Unit = {
    runInboundControlStream()
    runInboundOrdinaryMessagesStream()

    if (largeMessageChannelEnabled) {
      runInboundLargeMessagesStream()
    }
  }

  private def runInboundControlStream(): Unit = {
    if (isShutdown) throw ShuttingDown
    val (resourceLife, ctrl, completed) =
      aeronSource(controlStreamId, envelopeBufferPool)
        .via(inboundFlow(settings, NoInboundCompressions))
        .toMat(inboundControlSink)({ case (a, (c, d)) ⇒ (a, c, d) })
        .run()(controlMaterializer)

    attachControlMessageObserver(ctrl)

    updateStreamMatValues(controlStreamId, resourceLife, completed)
    attachStreamRestart("Inbound control stream", completed, () ⇒ runInboundControlStream())
  }

  private def runInboundOrdinaryMessagesStream(): Unit = {
    if (isShutdown) throw ShuttingDown

    val (resourceLife, inboundCompressionAccesses, completed) =
      if (inboundLanes == 1) {
        aeronSource(ordinaryStreamId, envelopeBufferPool)
          .viaMat(inboundFlow(settings, _inboundCompressions))(Keep.both)
          .toMat(inboundSink(envelopeBufferPool))({ case ((a, b), c) ⇒ (a, b, c) })
          .run()(materializer)

      } else {
        val hubKillSwitch = KillSwitches.shared("hubKillSwitch")
        val source: Source[InboundEnvelope, (ResourceLifecycle, InboundCompressionAccess)] =
          aeronSource(ordinaryStreamId, envelopeBufferPool)
            .via(hubKillSwitch.flow)
            .viaMat(inboundFlow(settings, _inboundCompressions))(Keep.both)

        // Select lane based on destination to preserve message order,
        // Also include the uid of the sending system in the hash to spread
        // "hot" destinations, e.g. ActorSelection anchor.
        val partitioner: InboundEnvelope ⇒ Int = env ⇒ {
          env.recipient match {
            case OptionVal.Some(r) ⇒
              val a = r.path.uid
              val b = env.originUid
              val hashA = 23 + a
              val hash: Int = 23 * hashA + java.lang.Long.hashCode(b)
              math.abs(hash) % inboundLanes
            case OptionVal.None ⇒ 0
          }
        }

        val (resourceLife, compressionAccess, hub) =
          source
            .toMat(Sink.fromGraph(new FixedSizePartitionHub[InboundEnvelope](partitioner, inboundLanes,
              settings.Advanced.InboundHubBufferSize)))({ case ((a, b), c) ⇒ (a, b, c) })
            .run()(materializer)

        val lane = inboundSink(envelopeBufferPool)
        val completedValues: Vector[Future[Done]] =
          (0 until inboundLanes).map { _ ⇒
            hub.toMat(lane)(Keep.right).run()(materializer)
          }(collection.breakOut)

        import system.dispatcher
        val completed = Future.sequence(completedValues).map(_ ⇒ Done)

        // tear down the upstream hub part if downstream lane fails
        // lanes are not completed with success by themselves so we don't have to care about onSuccess
        completed.failed.foreach { reason ⇒ hubKillSwitch.abort(reason) }

        (resourceLife, compressionAccess, completed)
      }

    setInboundCompressionAccess(inboundCompressionAccesses)

    updateStreamMatValues(ordinaryStreamId, resourceLife, completed)
    attachStreamRestart("Inbound message stream", completed, () ⇒ runInboundOrdinaryMessagesStream())
  }

  private def runInboundLargeMessagesStream(): Unit = {
    if (isShutdown) throw ShuttingDown

    val (resourceLife, completed) = aeronSource(largeStreamId, largeEnvelopeBufferPool)
      .via(inboundLargeFlow(settings))
      .toMat(inboundSink(largeEnvelopeBufferPool))(Keep.both)
      .run()(materializer)

    updateStreamMatValues(largeStreamId, resourceLife, completed)
    attachStreamRestart("Inbound large message stream", completed, () ⇒ runInboundLargeMessagesStream())
  }

  private def updateStreamMatValues(streamId: Int, aeronSourceLifecycle: AeronSource.ResourceLifecycle, completed: Future[Done]): Unit = {
    implicit val ec = materializer.executionContext
    updateStreamMatValues(streamId, InboundStreamMatValues(
      Some(aeronSourceLifecycle),
      completed.recover { case _ ⇒ Done }))
  }

  override protected def shutdownTransport(): Future[Done] = {
    import system.dispatcher
    taskRunner.stop().map { _ ⇒
      topLevelFREvents.loFreq(Transport_Stopped, NoMetaData)
      if (aeronErrorLogTask != null) {
        aeronErrorLogTask.cancel()
        topLevelFREvents.loFreq(Transport_AeronErrorLogTaskStopped, NoMetaData)
      }
      if (aeron != null) aeron.close()
      if (areonErrorLog != null) areonErrorLog.close()
      if (mediaDriver.get.isDefined) stopMediaDriver()

      Done
    }
  }

}
