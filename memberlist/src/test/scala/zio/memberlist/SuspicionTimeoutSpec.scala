package zio.memberlist

import zio.clock.Clock
import zio.duration._
import zio.logging.Logging
import zio.memberlist.state._
import zio.memberlist.SwimError.{ SuspicionTimeoutAlreadyStarted, SuspicionTimeoutCancelled }
import zio.test.Assertion.{ equalTo, isLeft }
import zio.test.environment.TestClock
import zio.test.{ assert, suite, testM }
import zio.{ ZIO, ZLayer }

object SuspicionTimeoutSpec extends KeeperSpec {

  val logger = Logging.console((_, line) => line)

  def testLayer(
    protocolInterval: Duration,
    suspicionAlpha: Int,
    suspicionBeta: Int,
    suspicionRequiredConfirmations: Int
  ) =
    (ZLayer
      .requires[Clock] ++ logger ++ IncarnationSequence.live) >+> Nodes.live(NodeAddress(Array(0, 0, 0, 0), 1111)) >+> SuspicionTimeout
      .live(
        protocolInterval,
        suspicionAlpha,
        suspicionBeta,
        suspicionRequiredConfirmations
      )

  val spec = suite("Suspicion timeout")(
    testM("schedule timeout with 100 nodes cluster") {
      for {
        _           <- ZIO.foreach(1 to 100)(i => Nodes.addNode(NodeAddress(Array(1, 2, 3, 4), i)).commit)
        node        = NodeAddress(Array(1, 2, 3, 4), 1)
        _           <- Nodes.changeNodeState(node, NodeState.Suspect).commit
        timeout     <- SuspicionTimeout.registerTimeout(node).commit
        _           <- timeout.awaitStart
        _           <- timeout.awaitAction.fork
        _           <- TestClock.adjust(50000.milliseconds)
        elapsedTime <- timeout.elapsedTimeMs
        nodeStatus  <- Nodes.nodeState(node).commit
      } yield assert(elapsedTime)(equalTo(4000L)) && assert(nodeStatus)(equalTo(NodeState.Dead))
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3)),
    testM("timeout should be decreased when another confirmation arrives") {
      for {
        _       <- ZIO.foreach(1 to 100)(i => Nodes.addNode(NodeAddress(Array(1, 2, 3, 4), i)).commit)
        node    = NodeAddress(Array(1, 1, 1, 1), 1111)
        other   = NodeAddress(Array(2, 1, 1, 1), 1111)
        timeout <- SuspicionTimeout.registerTimeout(node).commit
        _       <- timeout.awaitStart

        fiber       <- timeout.awaitAction.fork
        _           <- TestClock.adjust(150.milliseconds)
        _           <- SuspicionTimeout.incomingSuspect(node, other).commit
        _           <- TestClock.adjust(50000.milliseconds)
        _           <- fiber.join
        elapsedTime <- timeout.elapsedTimeMs
      } yield assert(elapsedTime)(equalTo(3000L))
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3)) /*@@ flaky*/,
    testM("should be able to cancel") {
      for {
        _            <- ZIO.foreach(1 to 100)(i => Nodes.addNode(NodeAddress(Array(1, 2, 3, 4), i)).commit)
        node         = NodeAddress(Array(1, 1, 1, 1), 1111)
        timeout      <- SuspicionTimeout.registerTimeout(node).commit
        _            <- timeout.awaitStart
        timeoutFiber <- timeout.awaitAction.either.fork
        _            <- TestClock.adjust(150.milliseconds)
        _            <- SuspicionTimeout.cancelTimeout(node).commit
        _            <- TestClock.adjust(50000.milliseconds)
        res          <- timeoutFiber.join
      } yield assert(res)(isLeft(equalTo(SuspicionTimeoutCancelled(node))))
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3)),
    testM("should be rejected when already started") {
      for {
        _      <- ZIO.foreach(1 to 100)(i => Nodes.addNode(NodeAddress(Array(1, 2, 3, 4), i)).commit)
        node   = NodeAddress(Array(1, 1, 1, 1), 1111)
        _      <- SuspicionTimeout.registerTimeout(node).commit
        result <- SuspicionTimeout.registerTimeout(node).commit.either
      } yield assert(result)(isLeft(equalTo(SuspicionTimeoutAlreadyStarted(node))))
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3))
  )

}
