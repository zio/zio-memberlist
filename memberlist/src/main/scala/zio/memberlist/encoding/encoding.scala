package zio.memberlist.encoding

import zio.memberlist.SerializationError.DeserializationTypeError
import zio.{ IO, ZIO }

object encoding {

  def byteArrayToInt(b: Array[Byte]): IO[DeserializationTypeError, Int] =
    ZIO.effect {
      BigInt.apply(b).intValue
    }.mapError(DeserializationTypeError(_))

  def intToByteArray(a: Int): Array[Byte] =
    Array(((a >> 24) & 0xFF).toByte, ((a >> 16) & 0xFF).toByte, ((a >> 8) & 0xFF).toByte, (a & 0xFF).toByte)

}
