package zio.memberlist.protocols

import zio.{ Chunk, ZIO }
import zio.memberlist._
import zio.stream.ZStream
import zio.logging._
import zio.memberlist.Protocol

object DeadLetter {

  def protocol: ZIO[Logging, Error, Protocol[Chunk[Byte]]] =
    Protocol[Chunk[Byte]].make(
      { msg =>
        log(LogLevel.Error)("message [" + msg + "] in dead letter") *> Message.noResponse
      },
      ZStream.empty
    )

}
