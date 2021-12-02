package zio.memberlist

import zio.stm.{TRef, URSTM, USTM, ZSTM}
import zio.{Has, ULayer}

trait MessageSequenceNo  {
  val next: USTM[Long]
}
object MessageSequenceNo {

  val next: URSTM[Has[MessageSequenceNo], Long] =
    ZSTM.accessM[Has[MessageSequenceNo]](_.get.next)

  def live: ULayer[Has[MessageSequenceNo]] =
    TRef
      .makeCommit[Long](0)
      .map(ref =>
        new MessageSequenceNo {

          override val next: USTM[Long] =
            ref.updateAndGet(_ + 1)
        }
      )
      .toLayer
}
