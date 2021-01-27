package zio.memberlist

import zio.Layer
import zio.config.ConfigDescriptor._
import zio.config.{ ConfigDescriptor, ReadError, ZConfig }
import zio.duration.{ Duration, _ }
import zio.memberlist.state.NodeName

case class MemberlistConfig(
  name: NodeName,
  port: Int,
  protocolInterval: Duration,
  protocolTimeout: Duration,
  messageSizeLimit: Int,
  broadcastResent: Int,
  localHealthMaxMultiplier: Int,
  suspicionAlpha: Int,
  suspicionBeta: Int,
  suspicionRequiredConfirmations: Int
)

object MemberlistConfig {

  val description: ConfigDescriptor[MemberlistConfig] =
    (string("NAME").xmap[NodeName](NodeName(_), _.name) |@|
      int("PORT").default(5557) |@|
      zioDuration("PROTOCOL_INTERVAL").default(1.second) |@|
      zioDuration("PROTOCOL_TIMEOUT").default(500.milliseconds) |@|
      int("MESSAGE_SIZE_LIMIT").default(64000) |@|
      int("BROADCAST_RESENT").default(10) |@|
      int("LOCAL_HEALTH_MAX_MULTIPLIER").default(8) |@|
      int("SUSPICION_ALPHA_MULTIPLIER").default(9) |@|
      int("SUSPICION_BETA_MULTIPLIER").default(9) |@|
      int("SUSPICION_CONFIRMATIONS").default(3))(MemberlistConfig.apply, MemberlistConfig.unapply)

  val fromEnv: Layer[ReadError[String], ZConfig[MemberlistConfig]] = ZConfig.fromSystemEnv(description)
}
