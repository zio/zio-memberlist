package zio.memberlist

import zio.memberlist.encoding.ByteCodec
import zio.memberlist.protocols.messages.FailureDetection._
import zio.memberlist.protocols.messages.{MemberlistMessage, _}
import zio.test._

object ByteCodecSpec extends DefaultRunnableSpec {

  implicit val codec: ByteCodec[MemberlistMessage] = ByteCodec.tagged[MemberlistMessage][
    Ping,
    Ack,
    Nack,
    PingReq,
    Suspect,
    Alive,
    Dead,
    Initial.Join,
    Initial.Accept,
    Initial.Reject
  ]

  def spec: ZSpec[Environment, Failure] =
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
      ByteCodecLaws[Initial.Accept](gens.swimAccept),
      ByteCodecLaws[Initial.Reject](gens.swimReject),
      ByteCodecLaws[MemberlistMessage](Gen.oneOf(gens.failureDetectionProtocol, gens.initialSwimlProtocol))
    )
}
