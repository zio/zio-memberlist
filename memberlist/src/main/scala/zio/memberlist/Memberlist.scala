package zio.memberlist

import izumi.reflect.Tag
import zio.clock.Clock
import zio.config._
import zio.logging._
import zio.memberlist.discovery.Discovery
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.state._
import zio.memberlist.protocols.{ DeadLetter, FailureDetection, Initial, User }
import zio.stream.{ Stream, ZStream }
import zio.{ IO, Queue, UIO, ZLayer, ZManaged }
import zio.memberlist.protocols.FailureDetection.PingReq
import zio.memberlist.protocols.FailureDetection.Suspect
import zio.memberlist.protocols.FailureDetection.Alive
import zio.memberlist.protocols.FailureDetection.Dead
import zio.memberlist.protocols.FailureDetection.Ping
import zio.memberlist.protocols.FailureDetection.Ack
import zio.memberlist.UnionType._

object Memberlist {

  trait Service[A] {
    def broadcast(data: A): IO[zio.memberlist.Error, Unit]
    def events: Stream[Nothing, MembershipEvent]
    def localMember: NodeAddress
    def nodes: UIO[Set[NodeAddress]]
    def receive: Stream[Nothing, (NodeAddress, A)]
    def send(data: A, receipt: NodeAddress): UIO[Unit]
  }

  type Env = ZConfig[MemberlistConfig] with Discovery with Logging with Clock

  final private[this] val QueueSize = 1000

  def live[B: ByteCodec: Tag]: ZLayer[Env, Error, Swim[B]] = {

    implicit val codec: ByteCodec[FailureDetection with Initial with User[B]] =
      ByteCodec.tagged[UNil Or FailureDetection Or Initial Or User[B]][
        Ping,
        PingReq,
        Ack,
        Suspect,
        Alive,
        Dead
      ]

    val internalLayer =
      (ZLayer.requires[Env] ++
        MessageSequenceNo.live ++
        IncarnationSequence.live ++
        LocalHealthMultiplier.liveWithConfig ++
        Nodes.liveWithConfig ++
        MessageAcknowledge.live) >+> SuspicionTimeout.liveWithConfig

    val managed =
      for {
        env              <- ZManaged.environment[MessageSequence with Nodes]
        localConfig      <- config[MemberlistConfig].toManaged_
        _                <- log.info("starting SWIM on port: " + localConfig.port).toManaged_
        udpTransport     <- transport.udp.live(localConfig.messageSizeLimit).build.map(_.get)
        userIn           <- Queue.bounded[Message.BestEffort[B]](QueueSize).toManaged(_.shutdown)
        userOut          <- Queue.bounded[Message.BestEffort[B]](QueueSize).toManaged(_.shutdown)
        localNodeAddress = env.get[Nodes.Service].localNode
        initial          <- Initial.protocol(localNodeAddress).flatMap(_.debug).toManaged_
        failureDetection <- FailureDetection
                             .protocol(localConfig.protocolInterval, localConfig.protocolTimeout, localNodeAddress)
                             .flatMap(_.debug)
                             .map(_.binary)
                             .toManaged_

        user         <- User.protocol[B](userIn, userOut).map(_.binary).toManaged_
        deadLetter   <- DeadLetter.protocol.toManaged_
        allProtocols = Protocol.compose(initial.binary, failureDetection, user, deadLetter)
        broadcast0   <- Broadcast.make(localConfig.messageSizeLimit, localConfig.broadcastResent).toManaged_
        messages0    <- ConnectionHandler.make(localNodeAddress, broadcast0, udpTransport)
        _            <- messages0.process(allProtocols).toManaged_
      } yield new Memberlist.Service[B] {

        override def broadcast(data: B): IO[SerializationError, Unit] =
          for {
            bytes <- ByteCodec.encode[User[B]](User(data))
            _     <- broadcast0.add(Message.Broadcast(bytes))
          } yield ()

        override val receive: Stream[Nothing, (NodeAddress, B)] =
          ZStream.fromQueue(userIn).collect {
            case Message.BestEffort(n, m) => (n, m)
          }

        override def send(data: B, receipt: NodeAddress): UIO[Unit] =
          userOut.offer(Message.BestEffort(receipt, data)).unit

        override def events: Stream[Nothing, MembershipEvent] =
          env.get[Nodes.Service].events

        override def localMember: NodeAddress = localNodeAddress

        override def nodes: UIO[Set[NodeAddress]] =
          env.get[Nodes.Service].healthyNodes.map(_.map(_._1).toSet).commit
      }

    internalLayer >>> ZLayer.fromManaged(managed)
  }
}
