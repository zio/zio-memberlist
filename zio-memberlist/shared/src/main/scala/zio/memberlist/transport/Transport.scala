package zio.memberlist.transport

import zio._
import zio.memberlist.{NodeAddress, TransportError}
import zio.stream._

trait Transport {
  def bind(addr: NodeAddress): Stream[TransportError, ChunkConnection]
  def connect(to: NodeAddress): Managed[TransportError, ChunkConnection]

  def send(to: NodeAddress, data: Chunk[Byte]): IO[TransportError, Unit] =
    connect(to).use(_.send(data))
}

object Transport {

  def bind(addr: NodeAddress): ZStream[Has[Transport], TransportError, ChunkConnection] =
    ZStream.accessStream(_.get.bind(addr))

  def connect(to: NodeAddress): ZManaged[Has[Transport], TransportError, ChunkConnection] =
    ZManaged.accessManaged(_.get.connect(to))

  def send(to: NodeAddress, data: Chunk[Byte]): ZIO[Has[Transport], TransportError, Unit] =
    ZIO.accessM(_.get.send(to, data))

}
