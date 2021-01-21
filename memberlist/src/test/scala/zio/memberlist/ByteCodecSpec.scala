package zio.memberlist

import zio.memberlist.protocols.FailureDetection._
import zio.memberlist.protocols.{ FailureDetection, Initial }
import zio.test._
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.UnionType._

object ByteCodecSpec extends DefaultRunnableSpec {

  val codec: ByteCodec[FailureDetection with Initial] = ByteCodec.tagged[UNil Or FailureDetection Or Initial][
    Ping,
    Ack,
    Nack,
    PingReq,
    Suspect,
    Alive,
    Dead,
    Initial.Join,
    Initial.Accept.type,
    Initial.Reject
  ]

  implicit val failureDetectionCodec = codec.lens[FailureDetection]
  implicit val joinCodec             = codec.lens[Initial]

  def spec =
    suite("ByteCodec")(
      //swim failure detection
      ByteCodecLaws[Ping](gens.ping),
      ByteCodecLaws[Ack](gens.ack),
      ByteCodecLaws[Nack](gens.nack),
      ByteCodecLaws[PingReq](gens.pingReq),
      //swim suspicion
      ByteCodecLaws[Suspect](gens.suspect),
      ByteCodecLaws[Alive](gens.alive),
      ByteCodecLaws[Dead](gens.dead),
      //swim initial
      ByteCodecLaws[Initial.Join](gens.swimJoin),
      ByteCodecLaws[Initial.Accept.type](gens.swimAccept),
      ByteCodecLaws[Initial.Reject](gens.swimReject),
      ByteCodecLaws[FailureDetection](gens.failureDetectionProtocol),
      ByteCodecLaws[Initial](gens.initialSwimlProtocol)
    )
}
