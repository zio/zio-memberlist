package zio.memberlist.protocols

import upickle.default._
import zio.Chunk
import zio.memberlist.NodeAddress
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.protocols.messages.FailureDetection.{Ack, Alive, Dead, Ping, PingReq, Suspect}
import zio.memberlist.protocols.messages.Initial.{Accept, Join, Reject}
import zio.memberlist.state.NodeName

object messages {

  sealed trait MemberlistMessage

  object MemberlistMessage {
    implicit def codec[A: ByteCodec]: ByteCodec[MemberlistMessage] =
      ByteCodec.tagged[MemberlistMessage][
        Join,
        Accept,
        Reject,
        Ping,
        PingReq,
        Ack,
        Suspect,
        Alive,
        Dead,
        User[A],
      ]
  }

  sealed trait FailureDetection extends MemberlistMessage

  object FailureDetection {

    final case class Ping(seqNo: Long) extends FailureDetection

    final case class Ack(seqNo: Long) extends FailureDetection

    final case class Nack(seqNo: Long) extends FailureDetection

    final case class PingReq(seqNo: Long, target: NodeName) extends FailureDetection

    final case class Suspect(incarnation: Long, from: NodeName, node: NodeName) extends FailureDetection

    final case class Alive(incarnation: Long, nodeId: NodeName) extends FailureDetection

    final case class Dead(incarnation: Long, from: NodeName, nodeId: NodeName) extends FailureDetection

    implicit val ackCodec: ByteCodec[Ack] =
      ByteCodec.fromReadWriter(macroRW[Ack])

    implicit val nackCodec: ByteCodec[Nack] =
      ByteCodec.fromReadWriter(macroRW[Nack])

    implicit val pingCodec: ByteCodec[Ping] =
      ByteCodec.fromReadWriter(macroRW[Ping])

    implicit val pingReqCodec: ByteCodec[PingReq] =
      ByteCodec.fromReadWriter(macroRW[PingReq])

    implicit val suspectCodec: ByteCodec[Suspect] =
      ByteCodec.fromReadWriter(macroRW[Suspect])

    implicit val aliveCodec: ByteCodec[Alive] =
      ByteCodec.fromReadWriter(macroRW[Alive])

    implicit val deadCodec: ByteCodec[Dead] =
      ByteCodec.fromReadWriter(macroRW[Dead])

  }

  case class PushPull()

  sealed trait Initial extends MemberlistMessage

  object Initial {

    final case class Join(nodeAddress: NodeAddress, nodeName: NodeName) extends Initial

    case class Accept(nodeAddress: NodeAddress) extends Initial

    final case class Reject(msg: String) extends Initial

    implicit val joinCodec: ByteCodec[Join] =
      ByteCodec.fromReadWriter(macroRW[Join])

    implicit val acceptCodec: ByteCodec[Accept] =
      ByteCodec.fromReadWriter(macroRW[Accept])

    implicit val rejectCodec: ByteCodec[Reject] =
      ByteCodec.fromReadWriter(macroRW[Reject])
  }

  final case class User[A](msg: A) extends MemberlistMessage

  object User {
    implicit def codec[A](implicit ev: ByteCodec[A]): ByteCodec[User[A]] =
      ev.bimap(User.apply, _.msg)
  }

  final case class WithPiggyback(
    node: NodeName,
    message: Chunk[Byte],
    gossip: List[Chunk[Byte]]
  ) extends MemberlistMessage

  object WithPiggyback {
    implicit val codec: ByteCodec[WithPiggyback] =
      ByteCodec.fromReadWriter(macroRW[WithPiggyback])

    implicit val chunkRW: ReadWriter[Chunk[Byte]] =
      implicitly[ReadWriter[Array[Byte]]]
        .bimap[Chunk[Byte]](
          ch => ch.toArray,
          arr => Chunk.fromArray(arr)
        )
  }

}
