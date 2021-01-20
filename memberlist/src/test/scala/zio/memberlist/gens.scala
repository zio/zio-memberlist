package zio.memberlist

import java.util.UUID

import zio.memberlist.protocols.FailureDetection._
import zio.memberlist.protocols._
import zio.memberlist.uuid.makeRandomUUID
import zio.random.Random
import zio.test._

object gens {

  val nodeAddress: Gen[Random with Sized, NodeAddress] =
    Gen.listOf(Gen.anyByte).map(_.toArray).zipWith(Gen.anyInt)(NodeAddress.apply)

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
      to    <- nodeAddress
    } yield PingReq(seqNo, to)

  val suspect: Gen[Random with Sized, Suspect] =
    for {
      incarnation <- Gen.anyLong
      from        <- nodeAddress
      to          <- nodeAddress
    } yield Suspect(incarnation, from, to)

  val alive: Gen[Random with Sized, Alive] =
    for {
      incarnation <- Gen.anyLong
      from        <- nodeAddress
    } yield Alive(incarnation, from)

  val dead: Gen[Random with Sized, Dead] =
    for {
      incarnation <- Gen.anyLong
      from        <- nodeAddress
      to          <- nodeAddress
    } yield Dead(incarnation, from, to)

  val failureDetectionProtocol: Gen[Random with Sized, FailureDetection] =
    Gen.oneOf(ping, ack, nack, pingReq, suspect, alive, dead)

  val swimJoin: Gen[Random with Sized, Initial.Join] =
    nodeAddress.map(Initial.Join)

  val swimAccept: Gen[Random with Sized, Initial.Accept.type] =
    Gen.const(Initial.Accept)

  val swimReject: Gen[Random with Sized, Initial.Reject] =
    Gen.alphaNumericString.map(Initial.Reject)

  val initialSwimlProtocol: Gen[Random with Sized, Initial] =
    Gen.oneOf(swimReject, swimAccept, swimJoin)
}
