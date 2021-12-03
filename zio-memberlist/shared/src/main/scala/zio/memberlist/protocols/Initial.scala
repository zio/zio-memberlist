package zio.memberlist.protocols

import zio.logging._
import zio.memberlist.discovery._
import zio.memberlist.protocols.messages.Initial._
import zio.memberlist.state.{Node, NodeName, NodeState, Nodes}
import zio.memberlist.{NodeAddress, Protocol, _}
import zio.stm.ZSTM
import zio.stream.ZStream
import zio.{Chunk, Has, ZIO}

object Initial {

  type Env = Has[MessageSequenceNo] with Has[Nodes] with Logging with Has[Discovery]

  def protocol(localAddr: NodeAddress, localName: NodeName): ZIO[Env, Error, Protocol[messages.Initial]] =
    Protocol[messages.Initial].make(
      {
        case Message.BestEffortByName(_, Join(addr, _)) if addr == localAddr =>
          Message.noResponse
        case Message.BestEffortByName(_, join @ Join(addr, name))            =>
          ZSTM.atomically(
            Nodes
              .nodeState(name)
              .as(Message.NoResponse)
              .orElse(
                Nodes.addNode(Node(name, addr, Chunk.empty, NodeState.Alive))
              )
              .as(
                Message
                  .Batch[messages.Initial](Message.BestEffortByName(name, Accept(localAddr)), Message.Broadcast(join))
              )
          )

        case Message.BestEffortByName(sender, Accept(addr)) =>
          ZSTM.atomically(
            Nodes.addNode(Node(sender, addr, Chunk.empty, NodeState.Alive)) *>
              Nodes.changeNodeState(sender, NodeState.Alive).as(Message.NoResponse)
          )
        case Message.BestEffortByName(sender, Reject(msg))  =>
          log.error("Rejected from cluster: " + msg) *>
            Nodes.disconnect(sender).as(Message.NoResponse).commit
      },
      ZStream
        .fromIterableM(Discovery.discoverNodes.tap(otherNodes => log.info("Discovered other nodes: " + otherNodes)))
        .mapM { node =>
          NodeAddress
            .fromSocketAddress(node)
            .map(nodeAddress => Message.BestEffortByAddress(nodeAddress, Join(localAddr, localName)))
        }
    )

}
