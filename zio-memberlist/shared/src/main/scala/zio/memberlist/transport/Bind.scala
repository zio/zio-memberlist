package zio.memberlist.transport

import zio.memberlist.TransportError
import zio.nio.core.SocketAddress
import zio.{Chunk, IO}

final class Bind(
  val isOpen: IO[TransportError, Boolean],
  val close: IO[TransportError, Unit],
  val localAddress: IO[TransportError, SocketAddress],
  val send: (SocketAddress, Chunk[Byte]) => IO[TransportError, Unit]
)
