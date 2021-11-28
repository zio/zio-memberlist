package zio.memberlist

import zio.stm.{TRef, URSTM, USTM, ZSTM}
import zio.{ULayer, ZLayer}

object MessageSequenceNo {

  trait Service {
    val next: USTM[Long]
  }

  val next: URSTM[MessageSequence, Long] =
    ZSTM.accessM[MessageSequence](_.get.next)

  def live: ULayer[MessageSequence] =
    ZLayer.fromEffect(
      TRef
        .makeCommit[Long](0)
        .map(ref =>
          new MessageSequenceNo.Service {

            override val next: USTM[Long] =
              ref.updateAndGet(_ + 1)
          }
        )
    )
}
