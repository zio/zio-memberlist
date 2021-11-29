package zio.memberlist

import zio._
import zio.clock.Clock
import zio.logging._
import zio.memberlist.PingPong._
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.protocols.messages.WithPiggyback
import zio.memberlist.state._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
import zio.test.environment.TestEnvironment
object MessagesSpec extends KeeperSpec {

  val logger: ZLayer[zio.console.Console with Clock, Nothing, Logging] = Logging.console()

  val messages: ZManaged[Has[Nodes] with Logging, TransportError, (TestTransport, MessageSink)] = for {
    local     <- NodeAddress.local(1111).toManaged_
    transport <- TestTransport.make
    broadcast <- Broadcast.make(64000, 2).toManaged_
    messages  <- MessageSink.make(NodeName("test-node"), local, broadcast, transport)
  } yield (transport, messages)

  val testNode: Node = Node(NodeName("node-1"), NodeAddress(Chunk(1, 1, 1, 1), 1111), Chunk.empty, NodeState.Alive)

  val spec: Spec[TestEnvironment, TestFailure[Error], TestSuccess] = suite("messages")(
    testM("receiveMessage") {

      val protocol = Protocol[PingPong].make(
        {
          case Message.BestEffort(sender, Ping(i)) =>
            ZIO.succeed(Message.BestEffort(sender, Pong(i)))
          case _                                   =>
            Message.noResponse
        },
        ZStream.empty
      )

      messages.use { case (testTransport, messages) =>
        for {
          dl       <- protocol
          _        <- Nodes.addNode(testNode).commit
          _        <- messages.process(dl.binary)
          ping1    <- ByteCodec[PingPong].toChunk(PingPong.Ping(123))
          ping2    <- ByteCodec[PingPong].toChunk(PingPong.Ping(321))
          _        <- testTransport.incommingMessage(WithPiggyback(testNode.name, ping1, List.empty))
          _        <- testTransport.incommingMessage(WithPiggyback(testNode.name, ping2, List.empty))
          messages <- testTransport.outgoingMessages.mapM { case WithPiggyback(_, chunk, _) =>
                        ByteCodec.decode[PingPong](chunk)
                      }.take(2).runCollect
        } yield assert(messages)(hasSameElements(List(PingPong.Pong(123), PingPong.Pong(321))))
      }
    },
    testM("should not exceed size of message") {

      val protocol = Protocol[PingPong].make(
        {
          case Message.BestEffort(sender, Ping(i)) =>
            ZIO.succeed(
              Message.Batch(
                Message.BestEffort(sender, Pong(i)),
                Message.Broadcast(Pong(i)),
                List.fill(2000)(Message.Broadcast(Ping(1))): _*
              )
            )
          case _                                   =>
            Message.noResponse
        },
        ZStream.empty
      )

      messages.use { case (testTransport, messages) =>
        for {
          dl    <- protocol
          _     <- Nodes.addNode(testNode).commit
          _     <- messages.process(dl.binary)
          ping1 <- ByteCodec[PingPong].toChunk(PingPong.Ping(123))
          _     <- testTransport.incommingMessage(WithPiggyback(testNode.name, ping1, List.empty))
          m     <- testTransport.outgoingMessages.runHead
          bytes <- m.fold[IO[SerializationError.SerializationTypeError, Chunk[Byte]]](ZIO.succeedNow(Chunk.empty))(
                     ByteCodec[WithPiggyback].toChunk(_)
                   )
        } yield assert(m.map(_.gossip.size))(isSome(equalTo(1487))) && assert(bytes.size)(equalTo(62591))
      }
    }
  ).provideCustomLayer(
    logger ++ IncarnationSequence.live ++ ((ZLayer.requires[Clock] ++ logger) >>> Nodes
      .live(NodeName("local-node")))
  )
}
