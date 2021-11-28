package zio.memberlist

import zio.memberlist.encoding.ByteCodec
import zio.memberlist.protocols.messages.User
import zio.test.Assertion.equalTo
import zio.test._

object UserProtocolSpec extends KeeperSpec {

  val spec: Spec[Any, TestFailure[SerializationError with Product], TestSuccess] = suite("User Protocol Serialization")(
    testM("Ping read and write") {
      val ping0: User[PingPong] = User(PingPong.Ping(1))
      val pong0: User[PingPong] = User(PingPong.Pong(1))
      for {
        pingChunk <- ByteCodec.encode[User[PingPong]](ping0)
        ping      <- ByteCodec.decode[User[PingPong]](pingChunk)
        pongChunk <- ByteCodec.encode[User[PingPong]](pong0)
        pong      <- ByteCodec.decode[User[PingPong]](pongChunk)
      } yield assert(ping)(equalTo(ping0)) && assert(pong)(equalTo(pong0))
    }
  )

}
