package zio.memberlist

import zio._
import zio.duration._
import zio.stm.{TRef, URSTM, USTM, ZSTM}

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
trait LocalHealthMultiplier {
  def increase: USTM[Unit]
  def decrease: USTM[Unit]
  def scaleTimeout(timeout: Duration): USTM[Duration]
}

object LocalHealthMultiplier {

  def increase: URSTM[Has[LocalHealthMultiplier], Unit] =
    ZSTM.accessM[Has[LocalHealthMultiplier]](_.get.increase)

  def decrease: URSTM[Has[LocalHealthMultiplier], Unit] =
    ZSTM.accessM[Has[LocalHealthMultiplier]](_.get.decrease)

  def scaleTimeout(timeout: Duration): URSTM[Has[LocalHealthMultiplier], Duration] =
    ZSTM.accessM[Has[LocalHealthMultiplier]](_.get.scaleTimeout(timeout))

  private def live0(max: Int): UIO[LocalHealthMultiplier] =
    TRef
      .makeCommit(0)
      .map(ref =>
        new LocalHealthMultiplier {

          override def increase: USTM[Unit] =
            ref.update(current => math.min(current + 1, max))

          override def decrease: USTM[Unit] =
            ref.update(current => math.max(current - 1, 0))

          override def scaleTimeout(timeout: Duration): USTM[Duration] =
            ref.get.map(score => timeout * (score.toDouble + 1.0))
        }
      )

  val liveWithConfig: ZLayer[Has[MemberlistConfig], Nothing, Has[LocalHealthMultiplier]] =
    zio.config.getConfig[MemberlistConfig].flatMap(config => live0(config.localHealthMaxMultiplier)).toLayer

  def live(max: Int): ULayer[Has[LocalHealthMultiplier]] =
    live0(max).toLayer

}
