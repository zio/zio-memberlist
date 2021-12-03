package zio.memberlist

import upickle.default.macroRW
import zio.clock.Clock
import zio.console.Console
import zio.duration._
import zio.logging.{Logging, log}
import zio.memberlist.discovery.Discovery
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.state.NodeName
import zio.nio.core.{InetAddress, InetSocketAddress}
import zio.{ExitCode, Has, URIO, ZEnv, ZIO, ZLayer}

class LocalNode(port: Int) extends zio.App {

  sealed trait ChaosMonkey

  object ChaosMonkey {
    final case object SimulateCpuSpike extends ChaosMonkey

    implicit val cpuSpikeCodec: ByteCodec[SimulateCpuSpike.type] =
      ByteCodec.fromReadWriter(macroRW[SimulateCpuSpike.type])

    implicit val codec: ByteCodec[ChaosMonkey] =
      ByteCodec.tagged[ChaosMonkey][
        SimulateCpuSpike.type
      ]
  }

  import ChaosMonkey._

  val discovery: ZLayer[Any, Nothing, Has[Discovery]] = for {
    localAddress <- InetAddress.localHost.orDie.toLayer
    first        <- InetSocketAddress.inetAddress(localAddress.get, 5557).toLayer
    second       <- InetSocketAddress.inetAddress(localAddress.get, 5558).toLayer
    discovery    <- Discovery.staticList(Set(first.get, second.get))
  } yield discovery

  val dependencies: ZLayer[Console with Clock with zio.system.System, Error, Logging with Has[
    MemberlistConfig
  ] with Has[Discovery] with Clock with Has[Memberlist[ChaosMonkey]]] = {
    val config  = ZLayer.succeed(
      MemberlistConfig(
        name = NodeName("local_node_" + port),
        port = port,
        protocolInterval = 1.second,
        protocolTimeout = 500.milliseconds,
        messageSizeLimit = 64000,
        broadcastResent = 10,
        localHealthMaxMultiplier = 8,
        suspicionAlpha = 9,
        suspicionBeta = 9,
        suspicionRequiredConfirmations = 3
      )
    )
    val logging = Logging.console()
    val seeds   = (logging ++ config) >+> discovery
    (seeds ++ Clock.live) >+> Memberlist.live[ChaosMonkey]
  }

  val program: URIO[Has[Memberlist[ChaosMonkey]] with Logging with zio.console.Console, ExitCode] =
    receive[ChaosMonkey].foreach { case (sender, message) =>
      log.info(s"receive message: $message from: $sender") *>
        ZIO.whenCase(message) { case SimulateCpuSpike =>
          log.info("simulating cpu spike")
        }
    }.fork *> events[ChaosMonkey].foreach(event => log.info("cluster event: " + event)).exitCode

  def run(args: List[String]): URIO[ZEnv with zio.console.Console, ExitCode] =
    program
      .provideCustomLayer(dependencies)
      .exitCode

}

object Node1 extends LocalNode(5557)
object Node2 extends LocalNode(5558)
