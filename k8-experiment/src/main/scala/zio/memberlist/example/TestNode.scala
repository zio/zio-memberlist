package zio.memberlist.example

import upickle.default.macroRW
import zio.clock.Clock
import zio.config.getConfig
import zio.duration._
import zio.logging.{Logging, log}
import zio.memberlist._
import zio.memberlist.discovery.Discovery
import zio.memberlist.encoding.ByteCodec
import zio.nio.core.InetAddress
import zio.{ExitCode, Has, URIO, ZEnv, ZIO, ZLayer}

object TestNode extends zio.App {

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

  val discovery: ZLayer[Has[MemberlistConfig] with Logging, Nothing, Has[Discovery.Service]] =
    ZLayer.fromManaged(
      for {
        appConfig  <- getConfig[MemberlistConfig].toManaged_
        serviceDns <- InetAddress
                        .byName("zio.memberlist-node.zio.memberlist-experiment.svc.cluster.local")
                        .orDie
                        .toManaged_
        discovery  <- Discovery.k8Dns(serviceDns, 10.seconds, appConfig.port).build.map(_.get)
      } yield discovery
    )

  val dependencies: ZLayer[zio.console.Console with Clock with zio.system.System with Any, Error, Logging with Has[
    MemberlistConfig
  ] with Has[Discovery.Service] with Clock with Memberlist[ChaosMonkey]] = {
    val config  = MemberlistConfig.fromEnv.orDie
    val logging = Logging.console()
    val seeds   = (logging ++ config) >+> discovery
    (seeds ++ Clock.live) >+> Memberlist.live[ChaosMonkey]
  }

  val program: URIO[Memberlist[ChaosMonkey] with Logging with zio.console.Console, ExitCode] =
    receive[ChaosMonkey].foreach { case (sender, message) =>
      log.info(s"receive message: $message from: $sender") *>
        ZIO.whenCase(message) { case SimulateCpuSpike =>
          log.info("simulating cpu spike")
        }
    }.exitCode

  def run(args: List[String]): URIO[ZEnv with zio.console.Console, ExitCode] =
    program
      .provideCustomLayer(dependencies)
      .exitCode

}
