package zio.memberlist

import zio.duration.Duration
import zio.memberlist.Message._
import zio.memberlist.state.NodeName
import zio.stm.ZSTM
import zio.{Has, IO, UIO, ZIO}

sealed trait Message[+A] {
  self =>

  final def transformM[B](fn: A => IO[Error, B]): IO[Error, Message[B]] =
    self match {
      case msg: Message.BestEffort[A] =>
        fn(msg.message).map(b => msg.copy(message = b))
      case msg: Message.Broadcast[A]  =>
        fn(msg.message).map(b => msg.copy(message = b))
      case msg: Message.Batch[A]      =>
        for {
          m1   <- msg.first.transformM(fn)
          m2   <- msg.second.transformM(fn)
          rest <- ZIO.foreach(msg.rest.toSeq)(_.transformM(fn))
        } yield Message.Batch(m1, m2, rest: _*)
      case msg: WithTimeout[A]        =>
        msg.message
          .transformM(fn)
          .map(b => msg.copy(message = b, action = msg.action.flatMap(_.transformM(fn))))
      case NoResponse                 =>
        Message.noResponse
    }
}

object Message {

  final case class BestEffort[A](node: NodeName, message: A) extends Message[A]

  final case class Batch[A](first: Message[A], second: Message[A], rest: Message[A]*) extends Message[A]

  final case class Broadcast[A](message: A) extends Message[A]

  final case class WithTimeout[A](message: Message[A], action: IO[Error, Message[A]], timeout: Duration)
      extends Message[A]

  case object NoResponse extends Message[Nothing]

  def withTimeout[R, A](
    message: Message[A],
    action: ZIO[R, Error, Message[A]],
    timeout: Duration
  ): ZSTM[R, Nothing, WithTimeout[A]] =
    for {
      env <- ZSTM.environment[R]
    } yield WithTimeout(message, action.provide(env), timeout)

  def withScaledTimeout[R, A](
    message: Message[A],
    action: ZIO[R, Error, Message[A]],
    timeout: Duration
  ): ZSTM[Has[LocalHealthMultiplier] with R, Nothing, WithTimeout[A]] =
    for {
      env    <- ZSTM.environment[R]
      scaled <- LocalHealthMultiplier.scaleTimeout(timeout)
    } yield WithTimeout(message, action.provide(env), scaled)

  def withTimeoutM[R, R1, A](
    message: ZIO[R1, Error, Message[A]],
    action: ZIO[R, Error, Message[A]],
    timeout: Duration
  ): ZIO[R1 with R, Error, WithTimeout[A]] =
    for {
      env <- ZIO.environment[R]
      msg <- message
    } yield WithTimeout(msg, action.provide(env), timeout)

  val noResponse: UIO[NoResponse.type] = ZIO.succeedNow(NoResponse)

}
