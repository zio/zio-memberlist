package zio.memberlist.state

import zio.clock.Clock
import zio.config._
import zio.logging.Logging
import zio.memberlist.ClusterError.UnknownNode
import zio.memberlist.MembershipEvent.{Join, Leave}
import zio.memberlist.{Error, MemberlistConfig, MembershipEvent, NodeAddress}
import zio.stm._
import zio.stream.{Stream, ZStream}
import zio.{Has, ZIO, ZLayer}

/**
 * Nodes maintains state of the cluster.
 */
trait Nodes {
  def addNode(node: Node): USTM[Unit]

  /**
   * Changes node state and issue membership event.
   * @param id - node name
   * @param newState - new state
   */
  def changeNodeState(id: NodeName, newState: NodeState): STM[Error, Unit]

  /**
   * close connection and remove Node from cluster.
   * @param id node name
   */
  def disconnect(id: NodeName): STM[Error, Unit]

  /**
   *  Stream of Membership Events
   */
  def events: Stream[Nothing, MembershipEvent]

  val localNodeName: NodeName

  /**
   * Returns next node.
   */
  def next(exclude: Option[NodeName]): USTM[Option[(NodeName, Node)]]

  /**
   * Node state for given name.
   */
  def nodeState(id: NodeName): STM[Error, NodeState]

  /**
   * Node Address for given name.
   */
  def nodeAddress(id: NodeName): STM[Error, NodeAddress]

  val numberOfNodes: USTM[Int]

  /**
   * Lists members that are in healthy state.
   */
  def healthyNodes: USTM[List[(NodeName, Node)]]

  /**
   * Returns string with cluster state.
   */
  val prettyPrint: USTM[String]
}

object Nodes {

  def addNode(node: Node): ZSTM[Has[Nodes], Nothing, Unit] =
    ZSTM.accessM[Has[Nodes]](_.get.addNode(node))

  val localNodeName: URSTM[Has[Nodes], NodeName] =
    ZSTM.access[Has[Nodes]](_.get.localNodeName)

  def nextNode(exclude: Option[NodeName] = None): URSTM[Has[Nodes], Option[(NodeName, Node)]] =
    ZSTM.accessM[Has[Nodes]](_.get.next(exclude))

  def nodeState(id: NodeName): ZSTM[Has[Nodes], Error, NodeState] =
    ZSTM.accessM[Has[Nodes]](_.get.nodeState(id))

  def nodeAddress(id: NodeName): ZSTM[Has[Nodes], Error, NodeAddress] =
    ZSTM.accessM[Has[Nodes]](_.get.nodeAddress(id))

  def changeNodeState(id: NodeName, newState: NodeState): ZSTM[Has[Nodes], Error, Unit] =
    ZSTM.accessM[Has[Nodes]](_.get.changeNodeState(id, newState))

  def disconnect(id: NodeName): ZSTM[Has[Nodes], Error, Unit] =
    ZSTM.accessM[Has[Nodes]](_.get.disconnect(id))

  val prettyPrint: URSTM[Has[Nodes], String] =
    ZSTM.accessM[Has[Nodes]](_.get.prettyPrint)

  def events: ZStream[Has[Nodes], Nothing, MembershipEvent] =
    ZStream.accessStream[Has[Nodes]](_.get.events)

  final case class NodeStateChanged(node: Node, oldState: NodeState, newState: NodeState)

  def live0(localNode0: NodeName): ZIO[Logging with Clock, Nothing, Nodes] =
    for {
      nodeStates       <- TMap.empty[NodeName, Node].commit
      eventsQueue      <- TQueue.bounded[MembershipEvent](1000).commit
      roundRobinOffset <- TRef.make(0).commit
    } yield new Nodes {

      def addNode(node: Node): USTM[Unit] =
        nodeStates
          .put(node.name, node)
          .whenM(nodeStates.contains(node.name).map(!_))
          .unit *> eventsQueue.offer(Join(node.name))

      def changeNodeState(id: NodeName, newState: NodeState): STM[Error, Unit] =
        nodeStates.get(id).get.orElseFail(UnknownNode(id)).flatMap { prev =>
          ZSTM.when(prev.state != newState) {
            nodeStates
              .put(id, prev.copy(state = newState))
              .tap { _ =>
                ZSTM
                  .whenCase(newState) { case NodeState.Dead | NodeState.Left =>
                    eventsQueue.offer(Leave(id))
                  }
                  .unit
              }
          }
        }

      def disconnect(id: NodeName): STM[Error, Unit] =
        nodeStates.delete(id)

      def events: Stream[Nothing, MembershipEvent] =
        ZStream.fromTQueue(eventsQueue)

      val localNodeName: NodeName =
        localNode0

      def next(
        exclude: Option[NodeName]
      ): USTM[Option[(NodeName, Node)]] /*(exclude: List[NodeId] = Nil)*/ =
        for {
          list <-
            nodeStates.toList
              .map(
                _.filter(entry =>
                  (entry._2.state == NodeState.Alive || entry._2.state == NodeState.Suspect) && entry._1 != localNodeName && !exclude
                    .contains(entry._1)
                )
              )

          nextIndex <- roundRobinOffset.updateAndGet(old => if (old < list.size - 1) old + 1 else 0)
          _         <- nodeStates
                         .removeIf((_, v) => v.state == NodeState.Dead || v.state == NodeState.Left)
                         .when(nextIndex == 0)
        } yield list.drop(nextIndex).headOption

      def nodeState(id: NodeName): STM[Error, NodeState] =
        nodeStates.get(id).get.map(_.state).orElseFail(UnknownNode(id))

      def nodeAddress(id: NodeName): STM[Error, NodeAddress] =
        nodeStates.get(id).get.map(_.addr).orElseFail(UnknownNode(id))

      val numberOfNodes: USTM[Int] =
        nodeStates.keys.map(_.size)

      def healthyNodes: USTM[List[(NodeName, Node)]] =
        nodeStates.toList.map(_.filter(entry => entry._2.state == NodeState.Alive && entry._1 != localNodeName))

      val prettyPrint: USTM[String] =
        nodeStates.toList.map(nodes =>
          "[ size: " + nodes.size +
            " nodes: [" +
            nodes.map { case (address, nodeState) =>
              "address: " + address + " state: " + nodeState
            }.mkString("|") +
            "]]"
        )
    }

  def live(localNode0: NodeName): ZLayer[Logging with Clock, Nothing, Has[Nodes]] =
    live0(localNode0).toLayer

  val liveWithConfig: ZLayer[Logging with Clock with Has[MemberlistConfig], Nothing, Has[Nodes]] =
    (for {
      localConfig <- getConfig[MemberlistConfig]
      nodes       <- live0(localConfig.name)
    } yield nodes).toLayer
}
