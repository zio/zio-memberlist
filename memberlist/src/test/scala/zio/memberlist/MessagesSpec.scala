package zio.memberlist

import zio._
import zio.clock.Clock
import zio.logging._
import zio.memberlist.MessageSink.WithPiggyback
import zio.memberlist.PingPong._
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.state._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test._
object MessagesSpec extends KeeperSpec {

  val logger = Logging.console((_, line) => line)

  val messages = for {
    local     <- NodeAddress.local(1111).toManaged_
    transport <- TestTransport.make
    broadcast <- Broadcast.make(64000, 2).toManaged_
    messages  <- MessageSink.make(local, broadcast, transport)
  } yield (transport, messages)

  val spec = suite("messages")(
    testM("receiveMessage") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)

      val protocol = Protocol[PingPong].make(
        {
          case Message.BestEffort(sender, Ping(i)) =>
            ZIO.succeed(Message.BestEffort(sender, Pong(i)))
          case _ =>
            Message.noResponse
        },
        ZStream.empty
      )

      messages.use {
        case (testTransport, messages) =>
          for {
            dl    <- protocol
            _     <- messages.process(dl.binary)
            ping1 <- ByteCodec[PingPong].toChunk(PingPong.Ping(123))
            ping2 <- ByteCodec[PingPong].toChunk(PingPong.Ping(321))
            _     <- testTransport.incommingMessage(WithPiggyback(testNodeAddress, ping1, List.empty))
            _     <- testTransport.incommingMessage(WithPiggyback(testNodeAddress, ping2, List.empty))
            messages <- testTransport.outgoingMessages.mapM {
                         case WithPiggyback(_, chunk, _) =>
                           ByteCodec.decode[PingPong](chunk)
                       }.take(2).runCollect
          } yield assert(messages)(hasSameElements(List(PingPong.Pong(123), PingPong.Pong(321))))
      }
    },
    testM("should not exceed size of message") {
      val testNodeAddress = NodeAddress(Array(1, 2, 3, 4), 1111)

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
          case _ =>
            Message.noResponse
        },
        ZStream.empty
      )

      messages.use {
        case (testTransport, messages) =>
          for {
            dl    <- protocol
            _     <- messages.process(dl.binary)
            ping1 <- ByteCodec[PingPong].toChunk(PingPong.Ping(123))
            _     <- testTransport.incommingMessage(WithPiggyback(testNodeAddress, ping1, List.empty))
            m     <- testTransport.outgoingMessages.runHead
            bytes <- m.fold[IO[SerializationError.SerializationTypeError, Chunk[Byte]]](ZIO.succeedNow(Chunk.empty))(
                      ByteCodec[WithPiggyback].toChunk(_)
                    )
          } yield assert(m.map(_.gossip.size))(isSome(equalTo(1487))) && assert(bytes.size)(equalTo(62538))
      }
    }
  ).provideCustomLayer(
    logger ++ IncarnationSequence.live ++ ((ZLayer.requires[Clock] ++ logger) >>> Nodes
      .live(NodeAddress(Array(0, 0, 0, 0), 1111)))
  )
}
