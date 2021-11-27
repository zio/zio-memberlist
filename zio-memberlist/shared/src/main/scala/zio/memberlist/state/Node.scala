package zio.memberlist.state

import zio.Chunk
import zio.memberlist.NodeAddress

final case class Node(
  name: NodeName,
//  addr: InetAddress,
//  port: Int,
  addr: NodeAddress,
  meta: Chunk[Byte], // Metadata from the delegate for this node.
  state: NodeState
//  pMin: Int,
//  pMax: Int,
//  pCur: Int,
//  dMin: Int,
//  dMax: Int,
//  dCur: Int
)
