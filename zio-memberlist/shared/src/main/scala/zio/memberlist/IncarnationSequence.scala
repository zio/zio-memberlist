package zio.memberlist

import zio.stm.{TRef, URSTM, USTM, ZSTM}
import zio.{Has, ULayer}

trait IncarnationSequence {
  val current: USTM[Long]
  val next: USTM[Long]
  def nextAfter(i: Long): USTM[Long]
}

object IncarnationSequence {

  val current: URSTM[Has[IncarnationSequence], Long] =
    ZSTM.accessM[Has[IncarnationSequence]](_.get.current)

  val next: URSTM[Has[IncarnationSequence], Long] =
    ZSTM.accessM[Has[IncarnationSequence]](_.get.next)

  def nextAfter(i: Long): URSTM[Has[IncarnationSequence], Long] =
    ZSTM.accessM[Has[IncarnationSequence]](_.get.nextAfter(i))

  def live: ULayer[Has[IncarnationSequence]] =
    TRef
      .makeCommit[Long](0)
      .map(ref =>
        new IncarnationSequence {

          override val current: zio.stm.USTM[Long] =
            ref.get

          override val next: USTM[Long] =
            ref.updateAndGet(_ + 1)

          override def nextAfter(i: Long): USTM[Long] =
            ref.updateAndGet(_ => i + 1)
        }
      )
      .toLayer
}
