package zio.memberlist

import zio.stm.{ TRef, URSTM, USTM, ZSTM }
import zio.{ ULayer, ZLayer }

object IncarnationSequence {

  trait Service {
    val current: USTM[Long]
    val next: USTM[Long]
    def nextAfter(i: Long): USTM[Long]
  }

  val current: URSTM[IncarnationSequence, Long] =
    ZSTM.accessM[IncarnationSequence](_.get.current)

  val next: URSTM[IncarnationSequence, Long] =
    ZSTM.accessM[IncarnationSequence](_.get.next)
  
  def nextAfter(i: Long): URSTM[IncarnationSequence, Long] =
    ZSTM.accessM[IncarnationSequence](_.get.nextAfter(i))

  def live: ULayer[IncarnationSequence] =
    ZLayer.fromEffect(
      TRef
        .makeCommit[Long](0)
        .map(ref =>
          new IncarnationSequence.Service {

            override val current: zio.stm.USTM[Long] = 
              ref.get

            override val next: USTM[Long] =
              ref.updateAndGet(_ + 1)

            override def nextAfter(i: Long): USTM[Long] = 
              ref.updateAndGet(_ => i + 1)
          }
        )
    )
}
