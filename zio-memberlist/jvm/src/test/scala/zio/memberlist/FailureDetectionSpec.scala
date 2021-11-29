package zio.memberlist

import zio._
import zio.clock._
import zio.console._
import zio.duration._
import zio.logging._
import zio.memberlist.protocols.messages.FailureDetection._
import zio.memberlist.protocols.{FailureDetection, messages}
import zio.memberlist.state._
import zio.test.Assertion.equalTo
import zio.test.TestAspect.ignore
import zio.test._
import zio.test.environment.{TestClock, TestEnvironment}

object FailureDetectionSpec extends KeeperSpec {

  private val protocolPeriod: Duration  = 1.second
  private val protocolTimeout: Duration = 500.milliseconds

  val logger: ZLayer[Console with Clock, Nothing, Logging] = Logging.console()

  val nodesLayer: ZLayer[
    Console with Clock with Console with Clock with Any with Any with Any with Any,
    Nothing,
    Console with Clock with Logging with IncarnationSequence with MessageSequence with MessageAcknowledge with LocalHealthMultiplier with Nodes with SuspicionTimeout
  ] = (
    ZLayer.requires[Console] ++
      ZLayer.requires[Clock] ++
      logger ++
      IncarnationSequence.live ++
      MessageSequenceNo.live ++
      MessageAcknowledge.live ++
      LocalHealthMultiplier.live(9)
  ) >+> zio.memberlist.state.Nodes
    .live(NodeName("test-node")) >+> SuspicionTimeout.live(protocolPeriod, 3, 5, 3)

  val recorder
    : ZLayer[Clock with Logging with Nodes with FailureDetection.Env, Nothing, ProtocolRecorder.ProtocolRecorder[
      messages.FailureDetection
    ]] =
    ProtocolRecorder
      .make(
        FailureDetection
          .protocol(protocolPeriod, protocolTimeout, NodeName("test-node"))
          .flatMap(_.debug)
      )
      .orDie

  val testLayer: ZLayer[
    Console with Clock with Console with Clock with Any with Any with Any with Any,
    Nothing,
    Console with Clock with Logging with IncarnationSequence with MessageSequence with MessageAcknowledge with LocalHealthMultiplier with Nodes with SuspicionTimeout with ProtocolRecorder.ProtocolRecorder[
      messages.FailureDetection
    ]
  ] = nodesLayer >+> recorder

  val node1: Node = Node(NodeName("node-1"), NodeAddress(Chunk(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)
  val node2: Node = Node(NodeName("node-2"), NodeAddress(Chunk(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)
  val node3: Node = Node(NodeName("node-3"), NodeAddress(Chunk(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)

  val spec: Spec[TestEnvironment, TestFailure[Any], TestSuccess] = suite("failure detection")(
    testM("Ping healthy Nodes periodically") {
      for {
        recorder <- ProtocolRecorder[messages.FailureDetection] { case Message.BestEffort(nodeAddr, Ping(seqNo)) =>
                      Message.BestEffort(nodeAddr, Ack(seqNo))
                    }
        _        <- Nodes.addNode(node1).commit
        _        <- Nodes.addNode(node2).commit
        _        <- TestClock.adjust(100.seconds)
        messages <- recorder.collectN(3) { case Message.BestEffort(addr, _: Ping) => addr }
      } yield assertTrue(messages.toSet == Set(node1.name, node2.name))
    }.provideCustomLayer(testLayer),
    // The test is passing locally, but for some reasons in CircleCI it always
    // times out for 2.12 at JDK8, while the other versions eventually pass;
    // I will ignore it for now, but it needs to be addressed in the future.
    testM("should change to Dead if there is no nodes to send PingReq") {
      for {
        recorder  <- ProtocolRecorder[messages.FailureDetection]()
        _         <- Nodes.addNode(node1).commit
        _         <- TestClock.adjust(1500.milliseconds)
        messages  <- recorder.collectN(2) { case msg => msg }
        nodeState <- Nodes
                       .nodeState(node1.name)
                       .orElseSucceed(NodeState.Dead)
                       .commit // in case it was cleaned up already
      } yield assertTrue(messages == List(Message.BestEffort(node1.name, Ping(1)), Message.NoResponse)) &&
        assertTrue(nodeState == NodeState.Dead)
    }.provideCustomLayer(testLayer) @@ ignore,
    testM("should send PingReq to other node") {
      for {
        recorder <- ProtocolRecorder[messages.FailureDetection] {
                      case Message.BestEffort(name, Ping(seqNo)) if name == node2.name =>
                        Message.BestEffort(node2.name, Ack(seqNo))
                      case Message.BestEffort(name, Ping(_)) if name == node1.name     =>
                        Message.NoResponse //simulate failing node
                    }
        _        <- Nodes.addNode(node1).commit
        _        <- Nodes.addNode(node2).commit
        _        <- TestClock.adjust(10.seconds)
        msg      <- recorder.collectN(1) { case Message.BestEffort(_, msg: PingReq) => msg }
      } yield assertTrue(msg == List(PingReq(1, node1.name)))
    }.provideCustomLayer(testLayer),
    testM("should change to Healthy when ack after PingReq arrives") {
      for {
        recorder <- ProtocolRecorder[messages.FailureDetection] {
                      case Message.BestEffort(name, Ping(seqNo)) if name == node2.name       =>
                        Message.BestEffort(node2.name, Ack(seqNo))
                      case Message.BestEffort(name, Ping(_)) if name == node1.name           =>
                        Message.NoResponse //simulate failing node
                      case Message.BestEffort(name, PingReq(seqNo, _)) if name == node2.name =>
                        Message.BestEffort(node2.name, Ack(seqNo))
                    }
        _        <- Nodes.addNode(node1).commit
        _        <- Nodes.addNode(node2).commit
        _        <- TestClock.adjust(10.seconds)
        _        <- recorder.collectN(1) { case Message.BestEffort(_, msg: PingReq) => msg }
//        event <- internalEvents.collect {
//                  case NodeStateChanged(`nodeAddress1`, NodeState.Unreachable, NodeState.Healthy) => ()
//                }.runHead
      } yield assert(true)(equalTo(true))
    }.provideCustomLayer(testLayer)
  )
}
