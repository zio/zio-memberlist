package zio.memberlist.state

import java.net.InetAddress
import zio.Chunk

final case class Node(
  name: String,
  addr: InetAddress,
  port: Int,
  meta: Chunk[Byte], // Metadata from the delegate for this node.
  state: NodeState,
  pMin: Int,
  pMax: Int,
  pCur: Int,
  dMin: Int,
  dMax: Int,
  dCur: Int
)
