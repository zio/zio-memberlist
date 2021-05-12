package zio.memberlist

import zio._
import zio.clock._
import zio.logging._
import zio.memberlist.state.Nodes._
import zio.memberlist.state.{ NodeState, _ }
import zio.stm.ZSTM
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._

object NodesSpec extends KeeperSpec {

  val logger = Logging.console((_, line) => line)

  val spec = suite("nodes")(
    testM("add node") {
      val aliveNode = Node(NodeName("alive-node"), NodeAddress(Array(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)
      val suspectNode =
        Node(NodeName("suspect-node"), NodeAddress(Array(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Suspect)
      val deadNode = Node(NodeName("dead-node"), NodeAddress(Array(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Dead)
      val leftNode = Node(NodeName("left-node"), NodeAddress(Array(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Left)
      for {
        next0     <- nextNode().commit
        _         <- ZSTM.foreach_(Chunk(aliveNode, suspectNode, deadNode, leftNode))(addNode).commit
        fiveCalls <- ZStream.repeatEffect(nextNode().commit).take(5).runCollect
      } yield assert(next0)(isNone) &&
        assert(fiveCalls)(
          equalTo(
            Chunk(
              Some((aliveNode.name, aliveNode)),
              Some((suspectNode.name, suspectNode)),
              Some((aliveNode.name, aliveNode)),
              Some((suspectNode.name, suspectNode)),
              Some((aliveNode.name, aliveNode))
            )
          )
        )
    },
    testM("add node twice") {
      val testNode = Node(NodeName("test-node"), NodeAddress(Array(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)
      for {
        _    <- addNode(testNode).commit
        _    <- addNode(testNode).commit
        next <- nextNode().commit
      } yield assert(next)(isSome(equalTo((testNode.name, testNode))))
    },
    testM("exclude node") {
      val testNode1 = Node(NodeName("test-node-1"), NodeAddress(Array(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)
      val testNode2 = Node(NodeName("test-node-2"), NodeAddress(Array(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)
      for {
        _    <- addNode(testNode1).commit
        _    <- addNode(testNode2).commit
        next <- ZIO.foreach(1 to 10)(_ => nextNode(Some(testNode2.name)).commit)
      } yield assert(next.flatten.toSet)(equalTo(Set((testNode1.name, testNode1))))
    },
    testM("should propagate events") {
      val testNode1 = Node(NodeName("test-node-1"), NodeAddress(Array(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)
      val testNode2 = Node(NodeName("test-node-2"), NodeAddress(Array(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)
      for {
        _       <- addNode(testNode1).commit
        _       <- changeNodeState(testNode1.name, NodeState.Suspect).commit
        _       <- changeNodeState(testNode1.name, NodeState.Dead).commit
        events1 <- Nodes.events.take(2).runCollect
        _       <- addNode(testNode2).commit
        _       <- changeNodeState(testNode2.name, NodeState.Suspect).commit
        _       <- changeNodeState(testNode2.name, NodeState.Dead).commit
        events2 <- Nodes.events.take(2).runCollect
      } yield assert(events1)(
        hasSameElements(
          List(
            MembershipEvent.Join(testNode1.name),
            MembershipEvent.Leave(testNode1.name)
          )
        )
      ) && assert(events2)(
        hasSameElements(
          List(
            MembershipEvent.Join(testNode2.name),
            MembershipEvent.Leave(testNode2.name)
          )
        )
      )
    }
  ).provideCustomLayer(
    (ZLayer.requires[Clock] ++ logger) >>> Nodes
      .live(NodeName("local-node"))
  )

}
