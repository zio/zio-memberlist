package zio.memberlist

import upickle.default._
import zio.{ Cause, Chunk, Exit, Fiber, IO, Queue, ZIO, ZManaged }
import zio.clock.Clock
import zio.logging.{ log, Logging }
import zio.memberlist.MessageSink.WithPiggyback
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.transport.ConnectionLessTransport
import zio.nio.core.SocketAddress
import zio.stream.{ Take, ZStream }

class MessageSink(
  val local: NodeAddress,
  messages: Queue[Take[Error, Message[Chunk[Byte]]]],
  broadcast: Broadcast,
  sendBestEffort: (SocketAddress, Chunk[Byte]) => IO[TransportError, Unit]
) {

  /**
   * Sends message to target.
   */
  def send(msg: Message[Chunk[Byte]]): ZIO[Clock with Logging, Error, Unit] =
    msg match {
      case Message.NoResponse => ZIO.unit
      case Message.BestEffort(nodeAddress, message) =>
        for {
          broadcast     <- broadcast.broadcast(message.size)
          withPiggyback = WithPiggyback(local, message, broadcast)
          chunk         <- ByteCodec[WithPiggyback].toChunk(withPiggyback)
          nodeAddress   <- nodeAddress.socketAddress
          _             <- sendBestEffort(nodeAddress, chunk)
        } yield ()
      case msg: Message.Batch[Chunk[Byte]] =>
        val (broadcast, rest) =
          (msg.first :: msg.second :: msg.rest.toList).partition(_.isInstanceOf[Message.Broadcast[_]])
        ZIO.foreach_(broadcast)(send) *>
          ZIO.foreach_(rest)(send)
      case msg @ Message.Broadcast(_) =>
        broadcast.add(msg)
      case Message.WithTimeout(message, action, timeout) =>
        send(message) *> action.delay(timeout).flatMap(send).unit
    }

  def process(protocol: Protocol[Chunk[Byte]]): ZIO[Clock with Logging, Nothing, Fiber.Runtime[Nothing, Unit]] = {
    def processTake(take: Take[Error, Message[Chunk[Byte]]]) =
      take.foldM(
        ZIO.unit,
        log.error("error: ", _),
        ZIO.foreach_(_)(send(_).catchAll(e => log.error("error during send: " + e.getCause)))
      )
    ZStream
      .fromQueue(messages)
      .collectM {
        case Take(Exit.Success(msgs)) =>
          ZIO.foreach(msgs) {
            case msg: Message.BestEffort[Chunk[Byte]] =>
              Take.fromEffect(protocol.onMessage(msg))
            case _ =>
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

  final case class WithPiggyback(
    node: NodeAddress,
    message: Chunk[Byte],
    gossip: List[Chunk[Byte]]
  )

  private def recoverErrors[R, E, A](stream: ZStream[R, E, A]): ZStream[R with Logging, Nothing, Take[E, A]] =
    stream.either.mapM(
      _.fold(
        e => log.error("error during sending", Cause.fail(e)).as(Take.halt(Cause.fail(e))),
        v => ZIO.succeedNow(Take.single(v))
      )
    )

  implicit val codec: ByteCodec[WithPiggyback] =
    ByteCodec.fromReadWriter(macroRW[WithPiggyback])

  implicit val chunkRW: ReadWriter[Chunk[Byte]] =
    implicitly[ReadWriter[Array[Byte]]]
      .bimap[Chunk[Byte]](
        ch => ch.toArray,
        arr => Chunk.fromArray(arr)
      )

  def make(
    local: NodeAddress,
    broadcast: Broadcast,
    udpTransport: ConnectionLessTransport.Service
  ): ZManaged[Logging, TransportError, MessageSink] =
    for {
      messageQueue <- Queue
                       .bounded[Take[Error, Message[Chunk[Byte]]]](1000)
                       .toManaged(_.shutdown)
      bind <- ConnectionHandler.bind(local, udpTransport, messageQueue)
    } yield new MessageSink(local, messageQueue, broadcast, bind.send)

}
