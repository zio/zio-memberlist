package zio.memberlist.transport

import zio._
import zio.console.{Console, _}
import zio.logging.Logging
import zio.memberlist.TransportError
import zio.nio.core.{InetAddress, InetSocketAddress, SocketAddress}
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment

object TransportSpec extends DefaultRunnableSpec {

  private val udpEnv =
    (TestEnvironment.live ++ Logging.ignore) >>> udp.live(128)

  def bindAndWaitForValue(
    addr: SocketAddress,
    startServer: Promise[Nothing, SocketAddress],
    handler: Channel => UIO[Unit] = _ => ZIO.unit
  ): ZIO[Has[ConnectionLessTransport], TransportError, Chunk[Byte]] =
    for {
      q <- Queue.bounded[Chunk[Byte]](10)
      h  = (out: Channel) =>
             {
               for {
                 _    <- handler(out)
                 data <- out.read
                 _    <- q.offer(data)
               } yield ()
             }.catchAll(ex => putStrLn("error in server: " + ex.getCause).orDie.provideLayer(Console.live))
      p <- bind(addr)(h).use { bind =>
             for {
               address <- bind.localAddress
               _       <- startServer.succeed(address)
               chunk   <- q.take
             } yield chunk
           }
    } yield p

  def spec: Spec[TestEnvironment, TestFailure[Exception], TestSuccess] =
    suite("transport")(
      suite("udp")(
        testM("can send and receive messages") {
          checkM(Gen.listOf(Gen.anyByte)) { bytes =>
            val payload = Chunk.fromIterable(bytes)
            for {
              localHost   <- InetAddress.localHost
              serverAddr  <- InetSocketAddress.inetAddress(localHost, 0)
              clientAddr  <- InetSocketAddress.inetAddress(localHost, 0)
              startServer <- Promise.make[Nothing, SocketAddress]
              chunk       <- bindAndWaitForValue(serverAddr, startServer).fork
              address     <- startServer.await

              _      <- bind(clientAddr)(_ => ZIO.unit).use(bind => bind.send(address, payload))
              result <- chunk.join
            } yield assert(result)(equalTo(payload))
          }
        }
      ).provideCustomLayer(udpEnv)
    )
}
