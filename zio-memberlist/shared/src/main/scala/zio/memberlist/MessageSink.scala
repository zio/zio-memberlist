package zio.memberlist

import zio.clock.Clock
import zio.logging.{Logging, log}
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.protocols.messages.WithPiggyback
import zio.memberlist.state.{NodeName, Nodes}
import zio.memberlist.transport.ConnectionLessTransport
import zio.nio.core.SocketAddress
import zio.stream.{Take, ZStream}
import zio.{Cause, Chunk, Exit, Fiber, Has, IO, Queue, ZIO, ZManaged}

class MessageSink(
  val local: NodeName,
  messages: Queue[Take[Error, Message[Chunk[Byte]]]],
  broadcast: Broadcast,
  sendBestEffort: (SocketAddress, Chunk[Byte]) => IO[TransportError, Unit]
) {

  /**
   * Sends message to target.
   */
  def send(msg: Message[Chunk[Byte]]): ZIO[Clock with Logging with Has[Nodes], Error, Unit] =
    msg match {
      case Message.NoResponse                            => ZIO.unit
      case Message.BestEffort(nodeAddress, message)      =>
        for {
          broadcast    <- broadcast.broadcast(message.size)
          withPiggyback = WithPiggyback(local, message, broadcast)
          chunk        <- ByteCodec[WithPiggyback].toChunk(withPiggyback)
          nodeAddress  <- Nodes.nodeAddress(nodeAddress).commit.flatMap(_.socketAddress)
          _            <- sendBestEffort(nodeAddress, chunk)
        } yield ()
      case msg: Message.Batch[Chunk[Byte]]               =>
        val (broadcast, rest) =
          (msg.first :: msg.second :: msg.rest.toList).partition(_.isInstanceOf[Message.Broadcast[_]])
        ZIO.foreach_(broadcast)(send) *>
          ZIO.foreach_(rest)(send)
      case msg @ Message.Broadcast(_)                    =>
        broadcast.add(msg)
      case Message.WithTimeout(message, action, timeout) =>
        send(message) *> action.delay(timeout).flatMap(send).unit
    }

  def process(
    protocol: Protocol[Chunk[Byte]]
  ): ZIO[Clock with Logging with Has[Nodes], Nothing, Fiber.Runtime[Nothing, Unit]] = {
    def processTake(take: Take[Error, Message[Chunk[Byte]]]) =
      take.foldM(
        ZIO.unit,
        log.error("error: ", _),
        ZIO.foreach_(_)(send(_).catchAll(e => log.throwable("error during send", e)))
      )
    ZStream
      .fromQueue(messages)
      .collectM { case Take(Exit.Success(msgs)) =>
        ZIO.foreach(msgs) {
          case msg: Message.BestEffort[Chunk[Byte]] =>
            Take.fromEffect(protocol.onMessage(msg))
          case _                                    =>
            ZIO.dieMessage("Something went horribly wrong.")
        }
      }
      .mapMPar(10) { msgs =>
        ZIO.foreach_(msgs)(processTake)
      }
      .runDrain
      .fork *>
      MessageSink
        .recoverErrors(protocol.produceMessages)
        .mapMPar(10)(processTake)
        .runDrain
        .fork
  }
}

object MessageSink {

  private def recoverErrors[R, E, A](stream: ZStream[R, E, A]): ZStream[R with Logging, Nothing, Take[E, A]] =
    stream.either.mapM(
      _.fold(
        e => log.error("error during sending", Cause.fail(e)).as(Take.halt(Cause.fail(e))),
        v => ZIO.succeedNow(Take.single(v))
      )
    )

  def make(
    local: NodeName,
    nodeAddress: NodeAddress,
    broadcast: Broadcast,
    udpTransport: ConnectionLessTransport
  ): ZManaged[Logging, TransportError, MessageSink] =
    for {
      messageQueue <- Queue
                        .bounded[Take[Error, Message[Chunk[Byte]]]](1000)
                        .toManaged(_.shutdown)
      bind         <- ConnectionHandler.bind(nodeAddress, udpTransport, messageQueue)
    } yield new MessageSink(local, messageQueue, broadcast, bind.send)

}
