package zio.memberlist

import upickle.default.{ macroRW, _ }
import zio.memberlist.GossipState.StateDiff

final case class GossipState(members: Vector[NodeAddress]) extends AnyVal {

  def addMember(member: NodeAddress): GossipState =
    copy(members = this.members :+ member)

  def diff(other: GossipState): StateDiff =
    StateDiff(
      this.members.diff(other.members),
      other.members.diff(this.members)
    )

  def merge(other: GossipState): GossipState =
    copy(members = this.members ++ other.members)

  def removeMember(member: NodeAddress): GossipState =
    copy(members = this.members.filterNot(_ == member))

  override def toString: String = s"GossipState[${members.mkString(",")}] "
}

object GossipState {
  val Empty = GossipState(Vector.empty[NodeAddress])

  implicit val gossipStateRw: ReadWriter[GossipState] = macroRW[GossipState]
  final case class StateDiff(local: Vector[NodeAddress], remote: Vector[NodeAddress])
}
