package zio.memberlist.transport

import zio.memberlist.TransportError
import zio.nio.core.SocketAddress
import zio.{Managed, UIO}

trait ConnectionLessTransport {
  def bind(localAddr: SocketAddress)(connectionHandler: Channel => UIO[Unit]): Managed[TransportError, Bind]
}

object ConnectionLessTransport {}
