package zio.memberlist

import zio.memberlist.protocols.FailureDetection._
import zio.memberlist.protocols.{ FailureDetection, Initial }
import zio.test._

object ByteCodecSpec extends DefaultRunnableSpec {

  def spec =
    suite("ByteCodec")(
      //swim failure detection
      ByteCodecLaws[Ping.type](gens.ping),
      ByteCodecLaws[Ack.type](gens.ack),
      ByteCodecLaws[Nack.type](gens.nack),
      ByteCodecLaws[PingReq](gens.pingReq),
      ByteCodecLaws[FailureDetection](gens.failureDetectionProtocol),
      //swim suspicion
      ByteCodecLaws[Suspect](gens.suspect),
      ByteCodecLaws[Alive](gens.alive),
      ByteCodecLaws[Dead](gens.dead),
      //swim initial
      ByteCodecLaws[Initial.Join](gens.swimJoin),
      ByteCodecLaws[Initial.Accept.type](gens.swimAccept),
      ByteCodecLaws[Initial.Reject](gens.swimReject),
      ByteCodecLaws[Initial](gens.initialSwimlProtocol)
    )
}
