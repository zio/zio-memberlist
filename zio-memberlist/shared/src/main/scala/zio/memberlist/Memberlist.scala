package zio.memberlist

import izumi.reflect.Tag
import zio.clock.Clock
import zio.config._
import zio.logging._
import zio.memberlist.discovery.Discovery
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.protocols.messages.MemberlistMessage
import zio.memberlist.protocols.{FailureDetection, Initial, User, messages}
import zio.memberlist.state._
import zio.stream.{Stream, ZStream}
import zio.{Chunk, Has, IO, Queue, UIO, ZLayer, ZManaged}

trait Memberlist[A] {
  def broadcast(data: A): IO[zio.memberlist.Error, Unit]
  def events: Stream[Nothing, MembershipEvent]
  def localMember: NodeName
  def nodes: UIO[Set[NodeName]]
  def receive: Stream[Nothing, (NodeName, A)]
  def send(data: A, receipt: NodeName): UIO[Unit]
}
object Memberlist   {

  type Env = Has[MemberlistConfig] with Has[Discovery] with Logging with Clock

  final private[this] val QueueSize = 1000

  def live[B: ByteCodec: Tag]: ZLayer[Env, Error, Has[Memberlist[B]]] = {
    val internalLayer =
      (ZLayer.requires[Env] ++
        MessageSequenceNo.live ++
        IncarnationSequence.live ++
        LocalHealthMultiplier.liveWithConfig ++
        Nodes.liveWithConfig ++
        MessageAcknowledge.live) >+> SuspicionTimeout.liveWithConfig

    val managed =
      for {
        env              <- ZManaged.environment[Has[MessageSequenceNo] with Has[Nodes]]
        localConfig      <- getConfig[MemberlistConfig].toManaged_
        _                <- log.info("starting SWIM on port: " + localConfig.port).toManaged_
        udpTransport     <- transport.udp.live(localConfig.messageSizeLimit).build.map(_.get)
        userIn           <- Queue.bounded[Message.BestEffortByName[B]](QueueSize).toManaged(_.shutdown)
        userOut          <- Queue.bounded[Message.BestEffortByName[B]](QueueSize).toManaged(_.shutdown)
        localNodeName     = localConfig.name
        localAddress     <- NodeAddress.local(localConfig.port).toManaged_
        initial          <- Initial.protocol(localAddress, localNodeName).toManaged_
        failureDetection <- FailureDetection
                              .protocol(localConfig.protocolInterval, localConfig.protocolTimeout, localNodeName)
                              .toManaged_

        user        <- User.protocol[B](userIn, userOut).toManaged_
        //        deadLetter   <- DeadLetter.protocol.toManaged_
        allProtocols = Protocol.compose[MemberlistMessage](initial, failureDetection, user).binary
        broadcast0  <- Broadcast.make(localConfig.messageSizeLimit, localConfig.broadcastResent).toManaged_

        messages0 <- MessageSink.make(localNodeName, localAddress, broadcast0, udpTransport)
        _         <- messages0.process(allProtocols).toManaged_
        localNode  = Node(localConfig.name, localAddress, Chunk.empty, NodeState.Alive)
        _         <- env.get[Nodes].addNode(localNode).commit.toManaged_
      } yield new Memberlist[B] {

        override def broadcast(data: B): IO[SerializationError, Unit] =
          for {
            bytes <- ByteCodec.encode[messages.User[B]](messages.User(data))
            _     <- broadcast0.add(Message.Broadcast(bytes))
          } yield ()

        override val receive: Stream[Nothing, (NodeName, B)] =
          ZStream.fromQueue(userIn).collect { case Message.BestEffortByName(n, m) =>
            (n, m)
          }

        override def send(data: B, receipt: NodeName): UIO[Unit] =
          userOut.offer(Message.BestEffortByName(receipt, data)).unit

        override def events: Stream[Nothing, MembershipEvent] =
          env.get[Nodes].events

        override def localMember: NodeName = localNodeName

        override def nodes: UIO[Set[NodeName]] =
          env.get[Nodes].healthyNodes.map(_.map(_._1).toSet).commit
      }

    internalLayer >>> ZLayer.fromManaged(managed)
  }
}
