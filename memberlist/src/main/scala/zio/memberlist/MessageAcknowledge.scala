package zio.memberlist

import zio.stm.{ TSet, URSTM, USTM, ZSTM }
import zio.{ ULayer, ZLayer }

object MessageAcknowledge {

  trait Service {
    def ack[A](msg: A): USTM[Unit]
    def register[A](msg: A): USTM[Unit]
    def isCompleted[A](msg: A): USTM[Boolean]
  }

  def ack[A](msg: A): URSTM[MessageAcknowledge, Unit] =
    ZSTM.accessM[MessageAcknowledge](_.get.ack(msg))

  def register[A](msg: A): URSTM[MessageAcknowledge, Unit] =
    ZSTM.accessM[MessageAcknowledge](_.get.register(msg))

  def isCompleted[A](msg: A): URSTM[MessageAcknowledge, Boolean] =
    ZSTM.accessM[MessageAcknowledge](_.get.isCompleted(msg))

  val live: ULayer[MessageAcknowledge] =
    ZLayer.fromEffect(
      TSet
        .empty[Any]
        .commit
        .map(pendingAcks =>
          new Service {

            override def ack[A](msg: A): USTM[Unit] =
              pendingAcks.delete(msg)

            override def register[A](msg: A): USTM[Unit] =
              pendingAcks.put(msg)

            override def isCompleted[A](msg: A): USTM[Boolean] =
              pendingAcks.contains(msg).map(!_)
          }
        )
    )

}
