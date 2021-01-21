package zio.memberlist.state

import zio.ZLayer
import zio.clock.Clock
import zio.logging.Logging
import zio.memberlist.ClusterError.UnknownNode
import zio.memberlist.NodeAddress
import zio.memberlist.MembershipEvent
import zio.memberlist.MembershipEvent.{ Join, Leave }
import zio.memberlist.Error
import zio.stm._
import zio.stream.{ Stream, ZStream }
import zio.{ Has, ZIO }
import zio.memberlist.MemberlistConfig
import zio.config._

object Nodes {

  /**
   * Nodes maintains state of the cluster.
   */
  trait Service {
    def addNode(node: NodeAddress): USTM[Unit]

    /**
     * Changes node state and issue membership event.
     * @param id - member id
     * @param newState - new state
     */
    def changeNodeState(id: NodeAddress, newState: NodeState): STM[Error, Unit]

    /**
     * close connection and remove Node from cluster.
     * @param id node id
     */
    def disconnect(id: NodeAddress): STM[Error, Unit]

    /**
     *  Stream of Membership Events
     */
    def events: Stream[Nothing, MembershipEvent]

    val localNode: NodeAddress

    /**
     * Returns next node.
     */
    def next(exclude: Option[NodeAddress]): USTM[Option[(NodeAddress, NodeState)]]

    /**
     * Node state for given NodeId.
     */
    def nodeState(id: NodeAddress): STM[Error, NodeState]

    val numberOfNodes: USTM[Int]

    /**
     * Lists members that are in healthy state.
     */
    def healthyNodes: USTM[List[(NodeAddress, NodeState)]]

    /**
     * Returns string with cluster state.
     */
    val prettyPrint: USTM[String]
  }

  def addNode(node: NodeAddress): ZSTM[zio.memberlist.Nodes, Nothing, Unit] =
    ZSTM.accessM[zio.memberlist.Nodes](_.get.addNode(node))

  val localNode: URSTM[zio.memberlist.Nodes, NodeAddress] =
    ZSTM.access[zio.memberlist.Nodes](_.get.localNode)

  def nextNode(exclude: Option[NodeAddress] = None): URSTM[zio.memberlist.Nodes, Option[(NodeAddress, NodeState)]] =
    ZSTM.accessM[zio.memberlist.Nodes](_.get.next(exclude))

  def nodeState(id: NodeAddress): ZSTM[zio.memberlist.Nodes, Error, NodeState] =
    ZSTM.accessM[zio.memberlist.Nodes](_.get.nodeState(id))

  def changeNodeState(id: NodeAddress, newState: NodeState): ZSTM[zio.memberlist.Nodes, Error, Unit] =
    ZSTM.accessM[zio.memberlist.Nodes](_.get.changeNodeState(id, newState))

  def disconnect(id: NodeAddress): ZSTM[zio.memberlist.Nodes, Error, Unit] =
    ZSTM.accessM[zio.memberlist.Nodes](_.get.disconnect(id))

  val prettyPrint: URSTM[zio.memberlist.Nodes, String] =
    ZSTM.accessM[zio.memberlist.Nodes](_.get.prettyPrint)

  def events: ZStream[zio.memberlist.Nodes, Nothing, MembershipEvent] =
    ZStream.accessStream[zio.memberlist.Nodes](_.get.events)

  final case class NodeStateChanged(node: NodeAddress, oldState: NodeState, newState: NodeState)

  def live0(localNode0: NodeAddress): ZIO[Logging with Clock, Nothing, zio.memberlist.state.Nodes.Service] =
    for {
      nodeStates       <- TMap.empty[NodeAddress, NodeState].commit
      eventsQueue      <- TQueue.bounded[MembershipEvent](100).commit
      roundRobinOffset <- TRef.make(0).commit
    } yield new Nodes.Service {

      def addNode(node: NodeAddress): USTM[Unit] =
        nodeStates
          .put(node, NodeState.Init)
          .whenM(nodeStates.contains(node).map(!_))
          .unit

      def changeNodeState(id: NodeAddress, newState: NodeState): STM[Error, Unit] =
        nodeState(id).flatMap { prev =>
          ZSTM.when(prev != newState) {
            nodeStates
              .put(id, newState)
              .tap { _ =>
                ZSTM
                  .whenCase(newState) {
                    case NodeState.Alive if prev == NodeState.Init => eventsQueue.offer(Join(id))
                    case NodeState.Dead | NodeState.Left           => eventsQueue.offer(Leave(id))
                  }
                  .unit
              }
          }
        }

      def disconnect(id: NodeAddress): STM[Error, Unit] =
        nodeStates.delete(id)

      def events: Stream[Nothing, MembershipEvent] =
        ZStream.fromTQueue(eventsQueue)

      val localNode: NodeAddress =
        localNode0

      def next(
        exclude: Option[NodeAddress]
      ): USTM[Option[(NodeAddress, NodeState)]] /*(exclude: List[NodeId] = Nil)*/ =
        for {
          list <- nodeStates.toList
                   .map(
                     _.filter(entry =>
                       (entry._2 == NodeState.Alive || entry._2 == NodeState.Suspect) && !exclude
                         .contains(entry._1)
                     )
                   )

          nextIndex <- roundRobinOffset.updateAndGet(old => if (old < list.size - 1) old + 1 else 0)
          _         <- nodeStates.removeIf((_, v) => v == NodeState.Dead).when(nextIndex == 0)
        } yield list.drop(nextIndex).headOption

      def nodeState(id: NodeAddress): STM[Error, NodeState] =
        nodeStates.get(id).get.orElseFail(UnknownNode(id))

      val numberOfNodes: USTM[Int] =
        nodeStates.keys.map(_.size)

      def healthyNodes: USTM[List[(NodeAddress, NodeState)]] =
        nodeStates.toList.map(_.filter(_._2 == NodeState.Alive))

      val prettyPrint: USTM[String] =
        nodeStates.toList.map(nodes =>
          "[ size: " + nodes.size +
            " nodes: [" +
            nodes.map {
              case (address, nodeState) =>
                "address: " + address + " state: " + nodeState
            }.mkString("|") +
            "]]"
        )
    }

  def live(localNode0: NodeAddress): ZLayer[Logging with Clock, Nothing, zio.memberlist.Nodes] =
    ZLayer.fromEffect(
      live0(localNode0)
    )

  val liveWithConfig: ZLayer[Logging with Clock with Has[MemberlistConfig], Nothing, zio.memberlist.Nodes] =
    ZLayer.fromEffect(
      for {

        localConfig <- config[MemberlistConfig]
        localNode   <- NodeAddress.local(localConfig.port)
        nodes       <- live0(localNode)
      } yield nodes
    )

}
