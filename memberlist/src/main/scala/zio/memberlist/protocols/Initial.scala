package zio.memberlist.protocols

import zio.ZIO
import zio.logging._
import zio.memberlist.state.Nodes._
import zio.memberlist.state.NodeState
import zio.memberlist.discovery._
import zio.memberlist.protocols.messages.Initial._
import zio.memberlist.{ NodeAddress, Protocol, _ }
import zio.stm.ZSTM
import zio.stream.ZStream

object Initial {

  type Env = MessageSequence with Nodes with Logging with Discovery

  def protocol(local: NodeAddress): ZIO[Env, Error, Protocol[messages.Initial]] =
    Protocol[messages.Initial].make(
      {
        case Message.BestEffort(_, Join(addr)) if addr == local =>
          Message.noResponse
        case Message.BestEffort(_, join @ Join(addr)) =>
          ZSTM.atomically(
            nodeState(addr)
              .as(Message.NoResponse)
              .orElse(
                addNode(addr) *>
                  changeNodeState(addr, NodeState.Alive)
              )
              .as(Message.Batch[messages.Initial](Message.BestEffort(addr, Accept), Message.Broadcast(join)))
          )

        case Message.BestEffort(sender, Accept) =>
          ZSTM.atomically(
            addNode(sender) *>
              changeNodeState(sender, NodeState.Alive).as(Message.NoResponse)
          )
        case Message.BestEffort(sender, Reject(msg)) =>
          log.error("Rejected from cluster: " + msg) *>
            disconnect(sender).as(Message.NoResponse).commit
      },
      ZStream
        .fromIterableM(discoverNodes.tap(otherNodes => log.info("Discovered other nodes: " + otherNodes)))
        .mapM { node =>
          NodeAddress
            .fromSocketAddress(node)
            .map(nodeAddress => Message.BestEffort(nodeAddress, Join(local)))
        }
    )

}
