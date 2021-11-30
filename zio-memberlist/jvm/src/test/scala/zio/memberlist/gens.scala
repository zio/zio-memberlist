package zio.memberlist

import zio.memberlist.protocols.messages.FailureDetection._
import zio.memberlist.protocols.messages._
import zio.memberlist.state.NodeName
import zio.memberlist.uuid.makeRandomUUID
import zio.random.Random
import zio.test._

import java.util.UUID

object gens {

  val nodeAddress: Gen[Random with Sized, NodeAddress] =
    Gen.chunkOf(Gen.anyByte).zipWith(Gen.anyInt)(NodeAddress.apply)

  val nodeName: Gen[Random with Sized, NodeName] =
    Gen.alphaNumericString.map(NodeName.apply)

  val uuid: Gen[Any, UUID] =
    Gen.fromEffect(makeRandomUUID)

  val ping: Gen[Random, Ping] =
    for {
      seqNo <- Gen.anyLong
    } yield Ping(seqNo)

  val ack: Gen[Random, Ack] =
    for {
      seqNo <- Gen.anyLong
    } yield Ack(seqNo)

  val nack: Gen[Random, Nack] =
    for {
      seqNo <- Gen.anyLong
    } yield Nack(seqNo)

  val pingReq: Gen[Random with Sized, PingReq] =
    for {
      seqNo <- Gen.anyLong
      to    <- nodeName
    } yield PingReq(seqNo, to)

  val suspect: Gen[Random with Sized, Suspect] =
    for {
      incarnation <- Gen.anyLong
      from        <- nodeName
      to          <- nodeName
    } yield Suspect(incarnation, from, to)

  val alive: Gen[Random with Sized, Alive] =
    for {
      incarnation <- Gen.anyLong
      from        <- nodeName
    } yield Alive(incarnation, from)

  val dead: Gen[Random with Sized, Dead] =
    for {
      incarnation <- Gen.anyLong
      from        <- nodeName
      to          <- nodeName
    } yield Dead(incarnation, from, to)

  val failureDetectionProtocol: Gen[Random with Sized, FailureDetection] =
    Gen.oneOf(ping, ack, nack, pingReq, suspect, alive, dead)

  val swimJoin: Gen[Random with Sized, Initial.Join] =
    nodeAddress.map(Initial.Join(_))

  val swimAccept: Gen[Random with Sized, Initial.Accept.type] =
    Gen.const(Initial.Accept)

  val swimReject: Gen[Random with Sized, Initial.Reject] =
    Gen.alphaNumericString.map(Initial.Reject(_))

  val initialSwimlProtocol: Gen[Random with Sized, Initial] =
    Gen.oneOf(swimReject, swimAccept, swimJoin)
}
