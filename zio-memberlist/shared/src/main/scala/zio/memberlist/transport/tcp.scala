package zio.memberlist.transport

import zio._
import zio.clock.Clock
import zio.duration._
import zio.logging.{Logging, log}
import zio.memberlist.encoding.encoding._
import zio.memberlist.{NodeAddress, TransportError, uuid}
import zio.nio.channels._
import zio.stream._

import java.{util => ju}

object tcp {

  def make(
    maxConnections: Long,
    connectionTimeout: Duration,
    sendTimeout: Duration,
    retryInterval: Duration = 50.millis
  ): ZLayer[Clock with Logging, Nothing, Has[Transport]] = ZLayer.fromFunction { env =>
    def toConnection(
      channel: AsynchronousSocketChannel,
      id: ju.UUID,
      close0: UIO[Unit]
    ): UIO[Connection[Any, TransportError, Chunk[Byte]]] =
      for {
        writeLock <- Semaphore.make(1)
        readLock  <- Semaphore.make(1)
      } yield new Connection[Any, TransportError, Chunk[Byte]] {
        override def send(dataChunk: Chunk[Byte]): IO[TransportError, Unit] = {
          val size      = dataChunk.size
          val sizeChunk = Chunk.fromArray(intToByteArray(size))

          log.debug(s"$id: Sending $size bytes") *>
            writeLock.withPermit {
              channel
                .writeChunk(sizeChunk ++ dataChunk)
                .mapError(TransportError.ExceptionWrapper(_))
                .timeoutFail(TransportError.RequestTimeout(sendTimeout))(sendTimeout)
                .unit
            }
        }.provide(env)
        override val receive: Stream[Nothing, Chunk[Byte]]                  =
          ZStream.repeatEffect {
            readLock.withPermit {
              for {
                length <- channel
                            .readChunk(4)
                            .flatMap(c => byteArrayToInt(c.toArray))
                data   <- channel.readChunk(length * 8)
                _      <- log.debug(s"$id: Received $length bytes")
              } yield data
            }
          }.catchAll(_ => ZStream.empty)
            .provide(env)

        override val close: UIO[Unit] = close0
      }

    new Transport {
      override def connect(to: NodeAddress): Managed[TransportError, ChunkConnection] = {
        for {
          id         <- uuid.makeRandomUUID.toManaged_
          _          <- log.debug(s"$id: new outbound connection to $to").toManaged_
          connection <- AsynchronousSocketChannel()
                          .mapError(TransportError.ExceptionWrapper(_))
                          .withEarlyRelease
                          .mapM { case (close, channel) =>
                            to.socketAddress
                              .flatMap(channel.connect(_).mapError(TransportError.ExceptionWrapper(_))) *> toConnection(
                              channel,
                              id,
                              close.unit
                            )
                          }
                          .retry(Schedule.spaced(retryInterval))
                          .timeout(connectionTimeout)
                          .flatMap(
                            _.fold[Managed[TransportError, ChunkConnection]](
                              ZManaged.fail(TransportError.ConnectionTimeout(connectionTimeout))
                            )(ZManaged.succeed(_))
                          )
        } yield connection
      }.provide(env)

      override def bind(addr: NodeAddress): Stream[TransportError, Connection[Any, TransportError, Chunk[Byte]]] = {

        val bind = ZStream.managed {
          AsynchronousServerSocketChannel()
            .mapError(TransportError.ExceptionWrapper(_))
            .tapM { server =>
              addr.socketAddress.flatMap { sAddr =>
                server.bind(Some(sAddr)).mapError(TransportError.BindFailed(sAddr, _))
              }
            }
            .zip(ZManaged.fromEffect(Semaphore.make(maxConnections)))
        }

        val bindConnection = bind.flatMap { case (server, lock) =>
          ZStream.managed(ZManaged.scope).flatMap { allocate =>
            ZStream
              .repeatEffect(
                allocate(lock.withPermitManaged *> server.accept.mapError(TransportError.ExceptionWrapper(_)))
              )
              .mapM { case (close, channel) =>
                for {
                  id         <- uuid.makeRandomUUID
                  _          <- log.debug(s"$id: new inbound connection")
                  connection <- toConnection(channel, id, close(Exit.unit).unit)
                } yield connection
              }
          }
        }

        ZStream.fromEffect(log.info(s"Binding transport to $addr")) *> bindConnection
      }.provide(env)
    }
  }
}
