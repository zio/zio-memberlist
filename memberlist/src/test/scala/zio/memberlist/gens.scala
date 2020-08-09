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

  val ping: Gen[Any, Ping.type] =
    Gen.const(Ping)

  val ack: Gen[Any, Ack.type] =
    Gen.const(Ack)

  val nack: Gen[Any, Nack.type] =
    Gen.const(Nack)

  val pingReq: Gen[Random with Sized, PingReq] =
    nodeAddress.map(PingReq)

  val suspect: Gen[Random with Sized, Suspect] =
    nodeAddress.zip(nodeAddress).map { case (from, to) => Suspect(from, to) }

  val alive: Gen[Random with Sized, Alive] =
    nodeAddress.map(Alive)

  val dead: Gen[Random with Sized, Dead] =
    nodeAddress.map(Dead)

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
