package zio.memberlist.protocols

import zio.logging._
import zio.memberlist.{Protocol, _}
import zio.stream.ZStream
import zio.{Chunk, ZIO}

object DeadLetter {

  def protocol: ZIO[Logging, Error, Protocol[Chunk[Byte]]] =
    Protocol[Chunk[Byte]].make(
      msg => log(LogLevel.Error)("message [" + msg + "] in dead letter") *> Message.noResponse,
      ZStream.empty
    )

}
