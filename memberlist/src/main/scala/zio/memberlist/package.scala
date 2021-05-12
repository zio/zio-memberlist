package zio

import zio.memberlist.state.NodeName
import zio.stream.ZStream

package object memberlist {
  type MessageSequence       = Has[MessageSequenceNo.Service]
  type IncarnationSequence   = Has[IncarnationSequence.Service]
  type Nodes                 = Has[state.Nodes.Service]
  type MessageAcknowledge    = Has[MessageAcknowledge.Service]
  type SuspicionTimeout      = Has[SuspicionTimeout.Service]
  type Memberlist[A]         = Has[Memberlist.Service[A]]
  type LocalHealthMultiplier = Has[LocalHealthMultiplier.Service]

  def broadcast[A: Tag](data: A): ZIO[Memberlist[A], Error, Unit] =
    ZIO.accessM(_.get.broadcast(data))

  def events[A: Tag]: ZStream[Memberlist[A], Error, MembershipEvent] =
    ZStream.accessStream(_.get.events)

  def localMember[A: Tag]: ZIO[Memberlist[A], Nothing, NodeName] =
    ZIO.access(_.get.localMember)

  def nodes[A: Tag]: ZIO[Memberlist[A], Nothing, Set[NodeName]] =
    ZIO.accessM(_.get.nodes)

  def receive[A: Tag]: ZStream[Memberlist[A], Error, (NodeName, A)] =
    ZStream.accessStream(_.get.receive)

  def send[A: Tag](data: A, receipt: NodeName): ZIO[Memberlist[A], Error, Unit] =
    ZIO.accessM(_.get.send(data, receipt))

}
