package zio.memberlist

import zio.stm.{TSet, URSTM, USTM, ZSTM}
import zio.{Has, ULayer}

trait MessageAcknowledge {
  def ack[A](msg: A): USTM[Unit]
  def register[A](msg: A): USTM[Unit]
  def isCompleted[A](msg: A): USTM[Boolean]
}

object MessageAcknowledge {

  def ack[A](msg: A): URSTM[Has[MessageAcknowledge], Unit] =
    ZSTM.accessM[Has[MessageAcknowledge]](_.get.ack(msg))

  def register[A](msg: A): URSTM[Has[MessageAcknowledge], Unit] =
    ZSTM.accessM[Has[MessageAcknowledge]](_.get.register(msg))

  def isCompleted[A](msg: A): URSTM[Has[MessageAcknowledge], Boolean] =
    ZSTM.accessM[Has[MessageAcknowledge]](_.get.isCompleted(msg))

  val live: ULayer[Has[MessageAcknowledge]] =
    TSet
      .empty[Any]
      .commit
      .map(pendingAcks =>
        new MessageAcknowledge {

          override def ack[A](msg: A): USTM[Unit] =
            pendingAcks.delete(msg)

          override def register[A](msg: A): USTM[Unit] =
            pendingAcks.put(msg)

          override def isCompleted[A](msg: A): USTM[Boolean] =
            pendingAcks.contains(msg).map(!_)
        }
      )
      .toLayer

}
