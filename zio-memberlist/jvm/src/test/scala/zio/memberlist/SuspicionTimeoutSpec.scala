package zio.memberlist

import zio.clock.Clock
import zio.duration._
import zio.logging.Logging
import zio.memberlist.SwimError.{SuspicionTimeoutAlreadyStarted, SuspicionTimeoutCancelled}
import zio.memberlist.state._
import zio.test.Assertion.{equalTo, isLeft}
import zio.test.environment.{TestClock, TestEnvironment}
import zio.test.{Spec, TestFailure, TestSuccess, assert, assertTrue, suite, testM}
import zio.{Chunk, ZIO, ZLayer}

object SuspicionTimeoutSpec extends KeeperSpec {

  val logger: ZLayer[zio.console.Console with Clock, Nothing, Logging] = Logging.console()

  def testLayer(
    protocolInterval: Duration,
    suspicionAlpha: Int,
    suspicionBeta: Int,
    suspicionRequiredConfirmations: Int
  ): ZLayer[
    Clock with zio.console.Console with Clock with Any,
    Nothing,
    Clock with Logging with IncarnationSequence with Nodes with SuspicionTimeout
  ] =
    (ZLayer
      .requires[Clock] ++ logger ++ IncarnationSequence.live) >+> Nodes.live(
      NodeName("local-node")
    ) >+> SuspicionTimeout
      .live(
        protocolInterval,
        suspicionAlpha,
        suspicionBeta,
        suspicionRequiredConfirmations
      )

  def node(i: Int): Node =
    Node(NodeName("node-" + i), NodeAddress(Chunk(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)

  val spec: Spec[TestEnvironment, TestFailure[Error], TestSuccess] = suite("Suspicion timeout")(
    testM("schedule timeout with 100 nodes cluster") {
      for {
        _           <- ZIO.foreach_(1 to 100)(i => Nodes.addNode(node(i)).commit)
        node         = NodeName("node-1")
        _           <- Nodes.changeNodeState(node, NodeState.Suspect).commit
        timeout     <- SuspicionTimeout.registerTimeout(node).commit
        _           <- timeout.awaitStart
        _           <- TestClock.adjust(150000.milliseconds)
        _           <- timeout.awaitAction
        elapsedTime <- timeout.elapsedTimeMs
        nodeStatus  <- Nodes.nodeState(node).commit
      } yield assertTrue(elapsedTime == 4000L) && assertTrue(nodeStatus == NodeState.Dead)
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3)),
    testM("timeout should be decreased when another confirmation arrives") {
      for {
        _       <- ZIO.foreach_(1 to 100)(i => Nodes.addNode(node(i)).commit)
        node     = NodeName("node-1")
        other    = NodeName("node-2")
        timeout <- SuspicionTimeout.registerTimeout(node).commit
        _       <- timeout.awaitStart

        fiber       <- timeout.awaitAction.fork
        _           <- TestClock.adjust(150.milliseconds)
        _           <- SuspicionTimeout.incomingSuspect(node, other).commit
        _           <- TestClock.adjust(50000.milliseconds)
        _           <- fiber.join
        elapsedTime <- timeout.elapsedTimeMs
      } yield assertTrue(elapsedTime == 3000L)
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3)) /*@@ flaky*/,
    testM("should be able to cancel") {
      for {
        _            <- ZIO.foreach_(1 to 100)(i => Nodes.addNode(node(i)).commit)
        node          = NodeName("node-1")
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
        _      <- ZIO.foreach_(1 to 100)(i => Nodes.addNode(node(i)).commit)
        node    = NodeName("test-node")
        _      <- SuspicionTimeout.registerTimeout(node).commit
        result <- SuspicionTimeout.registerTimeout(node).commit.either
      } yield assert(result)(isLeft(equalTo(SuspicionTimeoutAlreadyStarted(node))))
    }.provideCustomLayer(testLayer(1.second, 1, 2, 3))
  )

}
