package zio.memberlist.state

sealed trait NodeState

object NodeState {
  case object Init    extends NodeState
  case object Alive   extends NodeState
  case object Suspect extends NodeState
  case object Dead    extends NodeState
  case object Left    extends NodeState
}
