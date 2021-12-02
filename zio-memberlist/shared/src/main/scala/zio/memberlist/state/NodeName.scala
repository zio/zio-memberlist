package zio.memberlist.state

import upickle.default._

final case class NodeName(name: String)

object NodeName {
  implicit val nodeNameRw: ReadWriter[NodeName] = macroRW[NodeName]
}
