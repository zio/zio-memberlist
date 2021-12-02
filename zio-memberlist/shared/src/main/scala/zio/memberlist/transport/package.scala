package zio.memberlist

import zio.nio.core.SocketAddress
import zio.{Chunk, Has, ZIO, ZManaged}

package object transport {

  type ChunkConnection = Connection[Any, TransportError, Chunk[Byte]]

  def bind[R <: Has[ConnectionLessTransport]](
    localAddr: SocketAddress
  )(
    connectionHandler: Channel => ZIO[R, Nothing, Unit]
  ): ZManaged[R, TransportError, Bind] =
    ZManaged
      .environment[R]
      .flatMap(env =>
        env
          .get[ConnectionLessTransport]
          .bind(localAddr)(conn => connectionHandler(conn).provide(env))
      )

}
