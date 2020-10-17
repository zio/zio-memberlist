package zio.memberlist

import zio.stm.{ TSet, URSTM, USTM, ZSTM }
import zio.{ ULayer, ZLayer }

object MessageAcknowledge {

  trait Service {
    def ack[A](msg: A, conversationId: Long): USTM[Unit]
    def register[A](msg: A, conversationId: Long): USTM[Unit]
    def isCompleted[A](msg: A, conversationId: Long): USTM[Boolean]
  }

  def ack[A](msg: A, conversationId: Long): URSTM[MessageAcknowledge, Unit] =
    ZSTM.accessM[MessageAcknowledge](_.get.ack(msg, conversationId))

  def register[A](msg: A, conversationId: Long): URSTM[MessageAcknowledge, Unit] =
    ZSTM.accessM[MessageAcknowledge](_.get.register(msg, conversationId))

  def isCompleted[A](msg: A, conversationId: Long): URSTM[MessageAcknowledge, Boolean] =
    ZSTM.accessM[MessageAcknowledge](_.get.isCompleted(msg, conversationId))

  val live: ULayer[MessageAcknowledge] =
    ZLayer.fromEffect(
      TSet
        .empty[(Long, Any)]
        .commit
        .map(pendingAcks =>
          new Service {

            override def ack[A](msg: A, conversationId: Long): USTM[Unit] =
              pendingAcks.delete((conversationId, msg))

            override def register[A](msg: A, conversationId: Long): USTM[Unit] =
              pendingAcks.put((conversationId, msg))

            override def isCompleted[A](msg: A, conversationId: Long): USTM[Boolean] =
              pendingAcks.contains((conversationId, msg)).map(!_)
          }
        )
    )

}
