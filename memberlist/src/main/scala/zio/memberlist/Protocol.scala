package zio.memberlist

import zio.logging.{ Logging, _ }
import zio.memberlist.encoding.ByteCodec
import zio.stream.{ ZStream, _ }
import zio.{ Chunk, IO, ZIO }

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
  final def binary[A <: M](implicit codec: ByteCodec[A]): Protocol[Chunk[Byte]] =
    new Protocol[Chunk[Byte]] {
      val lens = codec.lens[M]

      override val onMessage: Message.BestEffort[Chunk[Byte]] => IO[Error, Message[Chunk[Byte]]] =
        msg =>
          codec
            .fromChunk(msg.message)
            .flatMap(decoded => self.onMessage(msg.copy(message = decoded)))
            .flatMap(_.transformM(lens.toChunk))

      override val produceMessages: Stream[Error, Message[Chunk[Byte]]] =
        self.produceMessages.mapM(_.transformM(lens.toChunk))
    }

  /**
   * Adds logging to each received and sent message.
   */
  val debug: ZIO[Logging, Error, Protocol[M]] =
    ZIO.access[Logging] { env =>
      new Protocol[M] {
        override def onMessage: Message.BestEffort[M] => IO[Error, Message[M]] =
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
  def onMessage: Message.BestEffort[M] => IO[Error, Message[M]]

  /**
   * Stream of outgoing messages.
   */
  val produceMessages: zio.stream.Stream[Error, Message[M]]

}

object Protocol {

  def compose[A](first: Protocol[A], second: Protocol[A], rest: Protocol[A]*): Protocol[A] =
    new Protocol[A] {

      override val onMessage: Message.BestEffort[A] => IO[Error, Message[A]] =
        msg => (second :: rest.toList).foldLeft(first.onMessage(msg))((acc, a) => acc.orElse(a.onMessage(msg)))

      override val produceMessages: Stream[Error, Message[A]] =
        ZStream.mergeAllUnbounded()(
          (first.produceMessages :: second.produceMessages :: rest.map(_.produceMessages).toList): _*
        )

    }

  class ProtocolBuilder[M] {

    def make[R, R1](
      in: Message.BestEffort[M] => ZIO[R, Error, Message[M]],
      out: zio.stream.ZStream[R1, Error, Message[M]]
    ): ZIO[R with R1, Error, Protocol[M]] =
      ZIO.access[R with R1](env =>
        new Protocol[M] {

          override val onMessage: Message.BestEffort[M] => IO[Error, Message[M]] =
            msg => in(msg).provide(env)

          override val produceMessages: Stream[Error, Message[M]] =
            out.provide(env)
        }
      )
  }

  def apply[M]: ProtocolBuilder[M] =
    new ProtocolBuilder[M]

}
