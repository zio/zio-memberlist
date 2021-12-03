package zio.memberlist

import zio.logging._
import zio.memberlist.encoding.ByteCodec
import zio.stream.{ZStream, _}
import zio.{Chunk, IO, ZIO}

import scala.reflect.{ClassTag, classTag}

/**
 * Protocol represents message flow.
 * @tparam M - type of messages handles by this protocol
 */
trait Protocol[M] {
  self =>

  /**
   * Converts this protocol to Chunk[Byte] protocol. This helps with composing multi protocols together.
   *
   * @param codec - TaggedCodec that handles serialization to Chunk[Byte]
   * @return - Protocol that operates on Chunk[Byte]
   */
  final def binary(implicit codec: ByteCodec[M]): Protocol[Chunk[Byte]] =
    new Protocol[Chunk[Byte]] {

      override val onMessage: Message.BestEffortByName[Chunk[Byte]] => IO[Error, Message[Chunk[Byte]]] =
        msg =>
          ByteCodec
            .decode[M](msg.message)
            .flatMap(decoded => self.onMessage(msg.copy(message = decoded)))
            .flatMap(_.transformM(ByteCodec.encode[M]))

      override val produceMessages: Stream[Error, Message[Chunk[Byte]]] =
        self.produceMessages.mapM(_.transformM(ByteCodec.encode[M]))
    }

  /**
   * Adds logging to each received and sent message.
   */
  val debug: ZIO[Logging, Error, Protocol[M]] =
    ZIO.access[Logging] { env =>
      new Protocol[M] {
        override def onMessage: Message.BestEffortByName[M] => IO[Error, Message[M]] =
          msg =>
            env.get.log(LogLevel.Trace)("Receive [" + msg + "]") *>
              self
                .onMessage(msg)
                .tap(msg => env.get.log(LogLevel.Trace)("Replied with [" + msg + "]"))

        override val produceMessages: Stream[Error, Message[M]] =
          self.produceMessages.tap { msg =>
            env.get.log(LogLevel.Trace)("Sending [" + msg + "]")
          }
      }
    }

  /**
   * Handler for incomming messages.
   */
  def onMessage: Message.BestEffortByName[M] => IO[Error, Message[M]]

  /**
   * Stream of outgoing messages.
   */
  val produceMessages: zio.stream.Stream[Error, Message[M]]

}

object Protocol {

  final class ProtocolComposePartiallyApplied[A] {
    def apply[A1 <: A: ClassTag, A2 <: A: ClassTag, A3 <: A: ClassTag](
      a1: Protocol[A1],
      a2: Protocol[A2],
      a3: Protocol[A3]
    ) = new Protocol[A] {

      override val onMessage: Message.BestEffortByName[A] => IO[Error, Message[A]] = {
        case msg: Message.BestEffortByName[A1 @unchecked] if classTag[A1].runtimeClass.isInstance(msg.message) =>
          a1.onMessage(msg)
        case msg: Message.BestEffortByName[A2 @unchecked] if classTag[A2].runtimeClass.isInstance(msg.message) =>
          a2.onMessage(msg)
        case msg: Message.BestEffortByName[A3 @unchecked] if classTag[A3].runtimeClass.isInstance(msg.message) =>
          a3.onMessage(msg)
        case _                                                                                                 => Message.noResponse

      }

      override val produceMessages: Stream[Error, Message[A]] = {
        val allStreams: List[Stream[Error, Message[A]]] =
          a1.asInstanceOf[Protocol[A]].produceMessages :: a2
            .asInstanceOf[Protocol[A]]
            .produceMessages :: a3.asInstanceOf[Protocol[A]].produceMessages :: Nil

        ZStream.mergeAllUnbounded()(allStreams: _*)
      }

    }
  }

  def compose[A] = new ProtocolComposePartiallyApplied[A]

  class ProtocolBuilder[M] {

    def make[R, R1](
      in: Message.BestEffortByName[M] => ZIO[R, Error, Message[M]],
      out: zio.stream.ZStream[R1, Error, Message[M]]
    ): ZIO[R with R1, Error, Protocol[M]] =
      ZIO.access[R with R1](env =>
        new Protocol[M] {

          override val onMessage: Message.BestEffortByName[M] => IO[Error, Message[M]] =
            msg => in(msg).provide(env)

          override val produceMessages: Stream[Error, Message[M]] =
            out.provide(env)
        }
      )
  }

  def apply[M]: ProtocolBuilder[M] =
    new ProtocolBuilder[M]

}
