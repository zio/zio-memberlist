package zio.memberlist.protocols

import zio.ZIO
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.{Protocol, _}
import zio.stream._

object User {

  def protocol[B: ByteCodec](
    userIn: zio.Queue[Message.BestEffortByName[B]],
    userOut: zio.Queue[Message.BestEffortByName[B]]
  ): ZIO[Any, Error, Protocol[messages.User[B]]] =
    Protocol[messages.User[B]].make(
      msg => userIn.offer(Message.BestEffortByName(msg.node, msg.message.msg)).as(Message.NoResponse),
      ZStream
        .fromQueue(userOut)
        .collect { case Message.BestEffortByName(node, msg) =>
          Message.BestEffortByName(node, messages.User(msg))
        }
    )

}
