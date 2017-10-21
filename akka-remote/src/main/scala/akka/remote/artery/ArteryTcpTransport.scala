/**
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.remote.artery

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.KeyStore
import java.security.SecureRandom
import javax.net.ssl._

import scala.concurrent.Future

import akka.Done
import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.remote.RemoteActorRefProvider
import akka.remote.artery.compress._
import akka.stream.Client
import akka.stream.IgnoreComplete
import akka.stream.Server
import akka.stream.SinkShape
import akka.stream.TLSProtocol._
import akka.stream.TLSRole
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Framing
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.MergeHub
import akka.stream.scaladsl.Partition
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.TLS
import akka.stream.scaladsl.Tcp
import akka.stream.scaladsl.Tcp.ServerBinding
import akka.util.ByteString

/**
 * INTERNAL API
 */
private[remote] class ArteryTcpTransport(_system: ExtendedActorSystem, _provider: RemoteActorRefProvider)
  extends ArteryTransport(_system, _provider) {
  import ArteryTransport.InboundStreamMatValues
  import FlightRecorderEvents._

  val tlsEnabled = true // FIXME config

  private var serverBinding: Option[Future[ServerBinding]] = None

  override protected def startTransport(): Unit = {
    // FIXME new TCP events
    topLevelFREvents.loFreq(Transport_AeronStarted, NoMetaData)
  }

  override protected def outboundTransportSink(
    outboundContext: OutboundContext,
    streamId:        Int,
    bufferPool:      EnvelopeBufferPool): Sink[EnvelopeBuffer, Future[Done]] = {
    implicit val sys = system

    val tcp = Tcp().outgoingConnection(outboundContext.remoteAddress.host.get, outboundContext.remoteAddress.port.get)

    val connectionFlow: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] =
      if (tlsEnabled) {
        Flow[ByteString]
          .map(bytes ⇒ SendBytes(bytes))
          .viaMat(tls(role = Client).joinMat(tcp)(Keep.right))(Keep.right)
          .collect {
            case SessionBytes(_, bytes) ⇒ bytes
          }
      } else
        tcp

    Flow[EnvelopeBuffer]
      .map { env ⇒
        val bytes = ByteString(env.byteBuffer)
        bufferPool.release(env)
        bytes
      }
      .via(connectionFlow)
      .map(b ⇒ throw new IllegalStateException(s"Unexpected incoming bytes in outbound connection to [${outboundContext.remoteAddress}]"))
      .toMat(Sink.ignore)(Keep.right)
  }

  override protected def runInboundStreams(): Unit = {
    implicit val mat = materializer
    implicit val sys = system

    val controlHub = runInboundControlStream()
    val ordinaryMessagesHub = runInboundOrdinaryMessagesStream()
    // FIXME large messages

    val inboundStream: Sink[EnvelopeBuffer, NotUsed] =
      Sink.fromGraph(GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._
        val partition = b.add(Partition[EnvelopeBuffer](2, env ⇒ {
          env.byteBuffer.get(EnvelopeBuffer.StreamIdOffset).toInt match {
            case `ordinaryStreamId` ⇒ 1
            case `controlStreamId`  ⇒ 0
            case `largeStreamId`    ⇒ 2
          }
        }))
        partition.out(0) ~> controlHub
        partition.out(1) ~> ordinaryMessagesHub
        SinkShape(partition.in)
      })

    val inboundConnectionFlow = Flow[ByteString]
      .via(Framing.lengthField(fieldLength = 4, fieldOffset = EnvelopeBuffer.FrameLengthOffset,
        settings.Advanced.MaximumFrameSize, byteOrder = ByteOrder.LITTLE_ENDIAN))
      .map { frame ⇒
        val buffer = ByteBuffer.wrap(frame.toArray)
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        new EnvelopeBuffer(buffer)
      }
      .alsoTo(inboundStream)
      .filter(_ ⇒ false) // don't send back anything in this TCP socket
      .map(_ ⇒ ByteString.empty) // make it a Flow[ByteString] again

    serverBinding =
      Some(Tcp().bind(localAddress.address.host.get, localAddress.address.port.get)
        .to(Sink.foreach { connection ⇒
          if (tlsEnabled) {
            val rhs: Flow[SslTlsInbound, SslTlsOutbound, Any] =
              Flow[SslTlsInbound]
                .collect {
                  case SessionBytes(_, bytes) ⇒ bytes
                }
                .via(inboundConnectionFlow)
                .map(SendBytes.apply)

            connection.handleWith(tls(role = Server).reversed join rhs)
          } else
            connection.handleWith(inboundConnectionFlow)
        })
        .run())
  }

  private def runInboundControlStream(): Sink[EnvelopeBuffer, NotUsed] = {
    if (isShutdown) throw ArteryTransport.ShuttingDown

    val (hub, ctrl, completed) =
      MergeHub.source[EnvelopeBuffer]
        .via(inboundFlow(settings, NoInboundCompressions))
        .toMat(inboundControlSink)({ case (a, (c, d)) ⇒ (a, c, d) })
        .run()(controlMaterializer)
    attachControlMessageObserver(ctrl)
    implicit val ec = materializer.executionContext
    updateStreamMatValues(controlStreamId, completed)
    attachStreamRestart("Inbound control stream", completed, () ⇒ runInboundControlStream())

    hub
  }

  private def runInboundOrdinaryMessagesStream(): Sink[EnvelopeBuffer, NotUsed] = {
    if (isShutdown) throw ArteryTransport.ShuttingDown

    // FIXME inboundLanes > 1

    val (hub, inboundCompressionAccesses, completed) =
      MergeHub.source[EnvelopeBuffer]
        .viaMat(inboundFlow(settings, _inboundCompressions))(Keep.both)
        .toMat(inboundSink(envelopeBufferPool))({ case ((a, b), c) ⇒ (a, b, c) })
        .run()(materializer)

    setInboundCompressionAccess(inboundCompressionAccesses)

    updateStreamMatValues(controlStreamId, completed)
    attachStreamRestart("Inbound message stream", completed, () ⇒ runInboundOrdinaryMessagesStream())

    hub
  }

  private def updateStreamMatValues(streamId: Int, completed: Future[Done]): Unit = {
    implicit val ec = materializer.executionContext
    updateStreamMatValues(controlStreamId, InboundStreamMatValues(
      None,
      completed.recover { case _ ⇒ Done }))
  }

  override protected def shutdownTransport(): Future[Done] = {
    implicit val ec = materializer.executionContext
    serverBinding match {
      case Some(binding) ⇒
        for {
          b ← binding
          _ ← b.unbind()
        } yield {
          topLevelFREvents.loFreq(Transport_Stopped, NoMetaData)
          Done
        }
      case None ⇒
        Future.successful(Done)
    }
  }

  // FIXME proper SslConfig

  def initWithTrust(trustPath: String) = {
    val password = "changeme"

    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    keyStore.load(getClass.getResourceAsStream("/keystore"), password.toCharArray)

    val trustStore = KeyStore.getInstance(KeyStore.getDefaultType)
    trustStore.load(getClass.getResourceAsStream(trustPath), password.toCharArray)

    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(keyStore, password.toCharArray)

    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(trustStore)

    val context = SSLContext.getInstance("TLS")
    context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)
    context
  }

  def initSslContext(): SSLContext = initWithTrust("/truststore")

  lazy val sslContext = initSslContext()
  lazy val cipherSuites = NegotiateNewSession.withCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA")

  def tls(role: TLSRole) = TLS(sslContext, None, cipherSuites, role, IgnoreComplete)

}
