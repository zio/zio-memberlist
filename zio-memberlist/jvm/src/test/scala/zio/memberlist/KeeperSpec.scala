package zio.memberlist

import zio.Chunk
import zio.duration._
import zio.test.environment.Live
import zio.test.{DefaultRunnableSpec, TestAspect, TestAspectAtLeastR}

abstract class KeeperSpec extends DefaultRunnableSpec {
  override val aspects: List[TestAspectAtLeastR[Live]] = List(TestAspect.timeout(180.seconds))

  def address(n: Int): NodeAddress =
    NodeAddress(Chunk.empty, n)
}
