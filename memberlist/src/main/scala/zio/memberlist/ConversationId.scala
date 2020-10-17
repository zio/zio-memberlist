package zio.memberlist

import zio.stm.{ TRef, URSTM, USTM, ZSTM }
import zio.{ ULayer, ZLayer }

object ConversationId {

  trait Service {
    val next: USTM[Long]
  }

  val next: URSTM[ConversationId, Long] =
    ZSTM.accessM[ConversationId](_.get.next)

  def live: ULayer[ConversationId] =
    ZLayer.fromEffect(
      TRef
        .makeCommit[Long](0)
        .map(ref =>
          new ConversationId.Service {

            override val next: USTM[Long] =
              ref.updateAndGet(_ + 1)
          }
        )
    )
}
