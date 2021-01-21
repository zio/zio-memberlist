package zio.memberlist.example

import upickle.default.macroRW
import zio.clock.Clock
import zio.config.config
import zio.console.putStrLn
import zio.logging.{ log, Logging }
import zio.memberlist.discovery.Discovery
import zio.memberlist.encoding.ByteCodec
import zio.memberlist._
import zio.nio.core.InetAddress
import zio.{ ExitCode, ZIO, ZLayer }
import zio.duration._
import zio.memberlist.UnionType._

object TestNode extends zio.App {

  sealed trait ChaosMonkey

  object ChaosMonkey {
    final case object SimulateCpuSpike extends ChaosMonkey

    implicit val cpuSpikeCodec: ByteCodec[SimulateCpuSpike.type] =
      ByteCodec.fromReadWriter(macroRW[SimulateCpuSpike.type])

    implicit val codec: ByteCodec[ChaosMonkey] =
      ByteCodec.tagged[UNil Or ChaosMonkey][
        SimulateCpuSpike.type
      ]
  }

  import ChaosMonkey._

  val discovery =
    ZLayer.fromManaged(
      for {
        appConfig <- config[MemberlistConfig].toManaged_
        serviceDns <- InetAddress
                       .byName("zio.memberlist-node.zio.memberlist-experiment.svc.cluster.local")
                       .orDie
                       .toManaged_
        discovery <- Discovery.k8Dns(serviceDns, 10.seconds, appConfig.port).build.map(_.get)
      } yield discovery
    )

  val dependencies = {
    val config  = MemberlistConfig.fromEnv.orDie
    val logging = Logging.console((_, msg) => msg)
    val seeds   = (logging ++ config) >+> discovery
    (seeds ++ Clock.live) >+> Memberlist.live[ChaosMonkey]
  }

  val program =
    receive[ChaosMonkey].foreach {
      case (sender, message) =>
        log.info(s"receive message: $message from: $sender") *>
          ZIO.whenCase(message) {
            case SimulateCpuSpike => log.info("simulating cpu spike")
          }
    }.as(ExitCode.failure)

  def run(args: List[String]) =
    program
      .provideCustomLayer(dependencies)
      .catchAll(ex => putStrLn("error: " + ex).as(ExitCode.success))

}
