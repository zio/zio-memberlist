package zio.memberlist

import zio.ZIO
import zio.memberlist.PingPong._
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.state.NodeName
import zio.stream.ZStream
import zio.test.Assertion.equalTo
import zio.test.{Spec, TestFailure, TestSuccess, assert}


object ProtocolSpec extends KeeperSpec {

  val protocolDefinition: ZIO[Any with Any, Error, Protocol[PingPong]] = Protocol[PingPong].make(
    {
      case Message.BestEffort(sender, Ping(i)) =>
        ZIO.succeed(Message.BestEffort(sender, Pong(i)))
      case _                                   => Message.noResponse
    },
    ZStream.empty
  )

  val testNode: NodeName = NodeName("test-node")

  val spec: Spec[Any with Any, TestFailure[Error], TestSuccess] = suite("protocol spec")(
    testM("request response") {
      for {
        protocol <- protocolDefinition
        response <- protocol.onMessage(Message.BestEffort(testNode, Ping(123)))
      } yield assert(response)(equalTo(Message.BestEffort(testNode, Pong(123))))
    },
    testM("binary request response") {
      for {
        protocol       <- protocolDefinition.map(_.binary)
        binaryMessage  <- ByteCodec.encode[PingPong](Ping(123))
        responseBinary <- protocol.onMessage(Message.BestEffort(testNode, binaryMessage))
        response       <- responseBinary match {
                            case Message.BestEffort(addr, chunk) =>
                              ByteCodec.decode[PingPong](chunk).map(pp => Message.BestEffort(addr, pp))
                            case _                               => ZIO.succeed(Message.NoResponse)
                          }
      } yield assert(response)(equalTo(Message.BestEffort[PingPong](testNode, Pong(123))))
    }
  )

}
