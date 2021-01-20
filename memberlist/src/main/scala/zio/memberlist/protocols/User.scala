package zio.memberlist.protocols

import zio.ZIO
import zio.memberlist.Protocol
import zio.stream._
import zio.memberlist._
import zio.memberlist.encoding.ByteCodec

final case class User[A](msg: A) extends AnyVal

object User {

  implicit def codec[A](implicit ev: ByteCodec[A]): ByteCodec[User[A]] =
    ev.bimap(User.apply, _.msg)

  def protocol[B: ByteCodec](
    userIn: zio.Queue[Message.BestEffort[B]],
    userOut: zio.Queue[Message.BestEffort[B]]
  ): ZIO[Any, Error, Protocol[User[B]]] =
    Protocol[User[B]].make(
      msg => userIn.offer(Message.BestEffort(msg.node, msg.message.msg)).as(Message.NoResponse),
      ZStream
        .fromQueue(userOut)
        .collect {
          case Message.BestEffort(node, msg) =>
            Message.BestEffort(node, User(msg))
        }
    )

}
