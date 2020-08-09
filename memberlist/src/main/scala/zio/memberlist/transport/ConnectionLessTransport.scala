package zio.memberlist.transport

import zio.memberlist.TransportError
import zio.nio.core.SocketAddress
import zio.{ Managed, UIO }

object ConnectionLessTransport {

  trait Service {
    def bind(localAddr: SocketAddress)(connectionHandler: Channel => UIO[Unit]): Managed[TransportError, Bind]
    def connect(to: SocketAddress): Managed[TransportError, Channel]
  }
}
