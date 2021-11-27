package zio.memberlist

import zio.Chunk
import zio.duration._
import zio.test.{DefaultRunnableSpec, TestAspect}
import zio.test.TestAspectAtLeastR
import zio.test.environment.Live

abstract class KeeperSpec extends DefaultRunnableSpec {
  override val aspects: List[TestAspectAtLeastR[Live]] = List(TestAspect.timeout(180.seconds))

  def address(n: Int): NodeAddress =
    NodeAddress(Chunk.empty, n)
}
