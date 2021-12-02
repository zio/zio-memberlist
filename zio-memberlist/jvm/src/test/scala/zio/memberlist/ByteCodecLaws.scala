package zio.memberlist

import zio.memberlist.encoding.ByteCodec
import zio.test.Assertion._
import zio.test._

import scala.reflect.ClassTag

object ByteCodecLaws {

  final class ByteCodecLawsPartiallyApplied[A] {

    def apply[R](gen: Gen[R, A])(implicit
      codec: ByteCodec[A],
      tag: ClassTag[A]
    ): Spec[TestConfig with R, TestFailure[Nothing], TestSuccess] =
      suite(s"ByteCodecLaws[${tag.runtimeClass.getName}]")(
        testM("codec round trip") {
          checkM(gen) { a =>
            assertM(codec.toChunk(a).flatMap[Any, Any, A](codec.fromChunk).run)(
              succeeds(equalTo(a))
            )
          }
        }
      )
  }

  def apply[A]: ByteCodecLawsPartiallyApplied[A] =
    new ByteCodecLawsPartiallyApplied[A]()
}
