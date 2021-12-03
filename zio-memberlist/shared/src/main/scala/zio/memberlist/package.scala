package zio

import zio.memberlist.state.NodeName
import zio.stream.ZStream

package object memberlist {

  def broadcast[A: Tag](data: A): ZIO[Has[Memberlist[A]], Error, Unit] =
    ZIO.accessM(_.get.broadcast(data))

  def events[A: Tag]: ZStream[Has[Memberlist[A]], Error, MembershipEvent] =
    ZStream.accessStream(_.get.events)

  def localMember[A: Tag]: ZIO[Has[Memberlist[A]], Nothing, NodeName] =
    ZIO.access(_.get.localMember)

  def nodes[A: Tag]: ZIO[Has[Memberlist[A]], Nothing, Set[NodeName]] =
    ZIO.accessM(_.get.nodes)

  def receive[A: Tag]: ZStream[Has[Memberlist[A]], Error, (NodeName, A)] =
    ZStream.accessStream(_.get.receive)

  def send[A: Tag](data: A, receipt: NodeName): ZIO[Has[Memberlist[A]], Error, Unit] =
    ZIO.accessM(_.get.send(data, receipt))

}
