package zio.memberlist

import zio.memberlist.state.NodeName

sealed trait MembershipEvent

object MembershipEvent {
  final case class Join(id: NodeName)  extends MembershipEvent
  final case class Leave(id: NodeName) extends MembershipEvent
}
