package zio.memberlist.transport

import zio._
import zio.clock.Clock
import zio.logging.{Logging, log}
import zio.memberlist.TransportError
import zio.memberlist.TransportError._
import zio.nio.channels.{Channel => _, _}
import zio.nio.core.{Buffer, SocketAddress}

object udp {

  /**
   * Creates udp transport with given maximum message size.
   * @param mtu - maximum message size
   * @return layer with Udp transport
   */
  def live(mtu: Int): ZLayer[Clock with Logging, Nothing, Has[ConnectionLessTransport]] =
    ZLayer.fromFunction { env =>
      new ConnectionLessTransport {
        def bind(addr: SocketAddress)(connectionHandler: Channel => UIO[Unit]): Managed[TransportError, Bind] =
          DatagramChannel
            .bind(Some(addr))
            .mapError(BindFailed(addr, _))
            .withEarlyRelease
            .onExit { _ =>
              log.info("shutting down server")
            }
            .mapM { case (close, server) =>
              Buffer
                .byte(mtu)
                .flatMap(buffer =>
                  server
                    .receive(buffer)
                    .mapError(ExceptionWrapper(_))
                    .zipLeft(buffer.flip)
                    .flatMap {
                      case Some(remoteAddr) =>
                        connectionHandler(
                          new Channel(
                            bytes => buffer.getChunk(bytes),
                            chunk =>
                              Buffer.byte(chunk).flatMap(server.send(_, remoteAddr)).mapError(ExceptionWrapper(_)).unit,
                            ZIO.succeed(true),
                            ZIO.unit
                          )
                        )
                      case None             =>
                        ZIO.unit //this is situation when channel is configure for non blocking.
                    }
                )
                .forever
                .fork
                .as {
                  val local = server.localAddress
                    .flatMap(opt => IO.effect(opt.get).orDie)
                    .mapError(ExceptionWrapper(_))
                  new Bind(
                    server.isOpen,
                    close.unit,
                    local,
                    (addr, data) => {
                      val size = data.size
                      Buffer
                        .byte(
                          Chunk((size >>> 24).toByte, (size >>> 16).toByte, (size >>> 8).toByte, size.toByte) ++ data
                        )
                        .flatMap(server.send(_, addr))
                        .mapError(ExceptionWrapper(_))
                        .unit
                    }
                  )
                }
            }
            .provide(env)

      }
    }
}
