package zio.memberlist

import zio._
import zio.clock._
import zio.logging._
import zio.memberlist.state._
import zio.test.Assertion._
import zio.test._

import java.util.UUID

object BroadcastSpec extends KeeperSpec {

  val logger: ZLayer[zio.console.Console with Clock, Nothing, Logging]                  = Logging.console()
  val testLayer: ZLayer[Clock with zio.console.Console with Clock, Nothing, Has[Nodes]] =
    (ZLayer.requires[Clock] ++ logger) >>> Nodes.live(NodeName(UUID.randomUUID().toString))

  val node1: Node = Node(NodeName("node-1"), NodeAddress(Chunk(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)
  val node2: Node = Node(NodeName("node-2"), NodeAddress(Chunk(2, 2, 2, 2), 1111), Chunk.empty, NodeState.Alive)

  def generateMessage(size: Int): Chunk[Byte] =
    Chunk.fromArray(Array.fill[Byte](size)(1))

  val spec: ZSpec[Environment, Failure] = suite("broadcast")(
    testM("add and retrieve from broadcast") {
      for {
        broadcast <- Broadcast.make(500, 2)
        _         <- Nodes.addNode(node1).commit
        _         <- Nodes.addNode(node2).commit
        _         <- broadcast.add(Message.Broadcast(generateMessage(100)))
        _         <- broadcast.add(Message.Broadcast(generateMessage(50)))
        _         <- broadcast.add(Message.Broadcast(generateMessage(200)))
        result    <- broadcast.broadcast(200)
      } yield assert(result)(hasSameElements(List(generateMessage(50), generateMessage(200))))
    },
    testM("resent message") {
      for {
        broadcast <- Broadcast.make(500, 2)
        _         <- Nodes.addNode(node1).commit
        _         <- Nodes.addNode(node2).commit
        _         <- broadcast.add(Message.Broadcast(generateMessage(100)))
        result    <- ZIO.reduceAll(
                       ZIO.succeedNow(List.empty[Chunk[Byte]]),
                       (1 to 3).map(_ => broadcast.broadcast(100))
                     )(_ ++ _)
      } yield assertTrue(result.size == 2)
    }
  ).provideCustomLayer(testLayer)
}
