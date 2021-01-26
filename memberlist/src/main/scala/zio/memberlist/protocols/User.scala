package zio.memberlist.protocols

import zio.ZIO
import zio.memberlist.Protocol
import zio.stream._
import zio.memberlist._
import zio.memberlist.encoding.ByteCodec

object User {

  def protocol[B: ByteCodec](
    userIn: zio.Queue[Message.BestEffort[B]],
    userOut: zio.Queue[Message.BestEffort[B]]
  ): ZIO[Any, Error, Protocol[messages.User[B]]] =
    Protocol[messages.User[B]].make(
      msg => userIn.offer(Message.BestEffort(msg.node, msg.message.msg)).as(Message.NoResponse),
      ZStream
        .fromQueue(userOut)
        .collect {
          case Message.BestEffort(node, msg) =>
            Message.BestEffort(node, messages.User(msg))
        }
    )

}
