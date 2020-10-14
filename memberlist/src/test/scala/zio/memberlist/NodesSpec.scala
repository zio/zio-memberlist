package zio.memberlist

import zio._
import zio.logging._
import zio.test._
import zio.memberlist.Nodes._
import zio.test.Assertion._
import zio.clock._

object NodesSpec extends KeeperSpec {

  val logger = Logging.console((_, line) => line)

  val spec = suite("nodes")(
    testM("add node") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)
      for {
        next0 <- nextNode().commit
        _     <- addNode(testNodeAddress).commit
        next1 <- nextNode().commit
        _     <- changeNodeState(testNodeAddress, NodeState.Healthy).commit
        next2 <- nextNode().commit
      } yield assert(next0)(isNone) && assert(next1)(isNone) && assert(next2)(
        isSome(equalTo((testNodeAddress, NodeState.Healthy)))
      )
    },
    testM("add node twice") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)
      for {
        _    <- addNode(testNodeAddress).commit
        _    <- changeNodeState(testNodeAddress, NodeState.Healthy).commit
        _    <- addNode(testNodeAddress).commit
        next <- nextNode().commit
      } yield assert(next)(isSome(equalTo((testNodeAddress, NodeState.Healthy))))
    },
    testM("exclude node") {
      val testNodeAddress1 = NodeAddress(Array(1, 2, 3, 4), 1111)
      val testNodeAddress2 = NodeAddress(Array(1, 2, 3, 4), 1112)
      for {
        _    <- addNode(testNodeAddress1).commit
        _    <- changeNodeState(testNodeAddress1, NodeState.Healthy).commit
        _    <- addNode(testNodeAddress2).commit
        _    <- changeNodeState(testNodeAddress2, NodeState.Healthy).commit
        next <- ZIO.foreach(1 to 10)(_ => nextNode(Some(testNodeAddress2)).commit)
      } yield assert(next.flatten.toSet)(equalTo(Set((testNodeAddress1, NodeState.Healthy: NodeState))))
    },
    testM("should propagate events") {
      val testNodeAddress1 = NodeAddress(Array(1, 2, 3, 4), 1111)
      val testNodeAddress2 = NodeAddress(Array(1, 2, 3, 4), 1112)
      for {
        _       <- addNode(testNodeAddress1).commit
        _       <- changeNodeState(testNodeAddress1, NodeState.Healthy).commit
        _       <- changeNodeState(testNodeAddress1, NodeState.Suspicion).commit
        _       <- changeNodeState(testNodeAddress1, NodeState.Dead).commit
        events1 <- Nodes.events.take(2).runCollect
        _       <- addNode(testNodeAddress2).commit
        _       <- changeNodeState(testNodeAddress2, NodeState.Healthy).commit
        _       <- changeNodeState(testNodeAddress2, NodeState.Suspicion).commit
        _       <- changeNodeState(testNodeAddress2, NodeState.Dead).commit
        events2 <- Nodes.events.take(2).runCollect
      } yield assert(events1)(
        hasSameElements(
          List(
            MembershipEvent.Join(testNodeAddress1),
            MembershipEvent.Leave(testNodeAddress1)
          )
        )
      ) && assert(events2)(
        hasSameElements(
          List(
            MembershipEvent.Join(testNodeAddress2),
            MembershipEvent.Leave(testNodeAddress2)
          )
        )
      )
    }
  ).provideCustomLayer((ZLayer.requires[Clock] ++ logger) >>> Nodes.live)

}
