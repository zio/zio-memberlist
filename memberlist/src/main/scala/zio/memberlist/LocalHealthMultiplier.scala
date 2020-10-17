package zio.memberlist

import zio.duration._
import zio.stm.{ TRef, URSTM, USTM, ZSTM }
import zio._

/**
 * LHM is a saturating counter, with a max value S and min value zero, meaning it will not
 * increase above S or decrease below zero.
 *
 * The following events cause the specified changes to the LHM counter:
 * - Successful probe (ping or ping-req with ack): -1
 * - Failed probe +1
 * - Refuting a suspect message about self: +1
 * - Probe with missed nack: +1
 */
object LocalHealthMultiplier {

  trait Service {
    def increase: USTM[Unit]
    def decrease: USTM[Unit]
    def scaleTimeout(timeout: Duration): USTM[Duration]
  }

  def increase: URSTM[LocalHealthMultiplier, Unit] =
    ZSTM.accessM[LocalHealthMultiplier](_.get.increase)

  def decrease: URSTM[LocalHealthMultiplier, Unit] =
    ZSTM.accessM[LocalHealthMultiplier](_.get.decrease)

  def scaleTimeout(timeout: Duration): URSTM[LocalHealthMultiplier, Duration] =
    ZSTM.accessM[LocalHealthMultiplier](_.get.scaleTimeout(timeout))

  def live(max: Int): ULayer[LocalHealthMultiplier] =
    ZLayer.fromEffect(
      TRef
        .makeCommit(0)
        .map(ref =>
          new Service {

            override def increase: USTM[Unit] =
              ref.update(current => math.min(current + 1, max))

            override def decrease: USTM[Unit] =
              ref.update(current => math.max(current - 1, 0))

            override def scaleTimeout(timeout: Duration): USTM[Duration] =
              ref.get.map(score => timeout * (score.toDouble + 1.0))
          }
        )
    )

}
