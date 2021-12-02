package zio.memberlist.protocols

import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.memberlist._
import zio.memberlist.protocols.messages.FailureDetection._
import zio.memberlist.state.Nodes._
import zio.memberlist.state.{NodeName, NodeState, Nodes}
import zio.stm.{STM, TMap, ZSTM}
import zio.stream.ZStream
import zio.{Has, Schedule, ZIO}

object FailureDetection {

  type Env = Has[LocalHealthMultiplier]
    with Has[MessageSequenceNo]
    with Has[IncarnationSequence]
    with Has[Nodes]
    with Logging
    with Has[MessageAcknowledge]
    with Clock
    with Has[SuspicionTimeout]

  def protocol(
    protocolPeriod: Duration,
    protocolTimeout: Duration,
    localNode: NodeName
  ): ZIO[Env, Error, Protocol[messages.FailureDetection]] =
    TMap
      .empty[Long, (NodeName, Long)]
      .commit
      .flatMap { pingReqs =>
        Protocol[messages.FailureDetection].make(
          {
            case Message.BestEffort(sender, msg @ Ack(seqNo)) =>
              log.debug(s"received ack[$seqNo] from $sender") *>
                ZSTM.atomically(
                  MessageAcknowledge.ack(msg) *>
                    pingReqs.get(seqNo).map {
                      case Some((originalNode, originalSeqNo)) =>
                        Message.BestEffort(originalNode, Ack(originalSeqNo))
                      case None                                =>
                        Message.NoResponse
                    } <* LocalHealthMultiplier.decrease
                )

            case Message.BestEffort(sender, Ping(seqNo)) =>
              ZIO.succeedNow(Message.BestEffort(sender, Ack(seqNo)))

            case Message.BestEffort(sender, PingReq(originalSeqNo, to)) =>
              MessageSequenceNo.next
                .map(newSeqNo => Ping(newSeqNo))
                .flatMap(ping =>
                  pingReqs.put(ping.seqNo, (sender, originalSeqNo)) *>
                    Message.withTimeout(
                      message = Message.BestEffort(to, ping),
                      action = pingReqs
                        .get(ping.seqNo)
                        .flatMap {
                          case Some((sender, originalSeqNo)) =>
                            pingReqs
                              .delete(ping.seqNo)
                              .as(Message.BestEffort(sender, Nack(originalSeqNo)))
                          case _                             =>
                            STM.succeedNow(Message.NoResponse)
                        }
                        .commit,
                      timeout = protocolTimeout
                    )
                )
                .commit
            case Message.BestEffort(_, msg @ Nack(_))                   =>
              MessageAcknowledge.ack(msg).commit *>
                Message.noResponse

            case Message.BestEffort(sender, Suspect(accusedIncarnation, _, `localNode`)) =>
              ZSTM.atomically(
                IncarnationSequence.nextAfter(accusedIncarnation).map { incarnation =>
                  val alive = Alive(incarnation, localNode)
                  Message.Batch(Message.BestEffort(sender, alive), Message.Broadcast(alive))
                } <* LocalHealthMultiplier.increase
              )

            case Message.BestEffort(from, Suspect(incarnation, _, node)) =>
              IncarnationSequence.current
                .zip(
                  nodeState(node).orElseSucceed(NodeState.Dead)
                )
                .flatMap {
                  case (localIncarnation, _) if localIncarnation > incarnation =>
                    STM.unit
                  case (_, NodeState.Dead)                                     =>
                    STM.unit
                  case (_, NodeState.Suspect)                                  =>
                    SuspicionTimeout.incomingSuspect(node, from)
                  case (_, _)                                                  =>
                    changeNodeState(node, NodeState.Suspect).ignore
                }
                .commit *> Message.noResponse

            case Message.BestEffort(sender, msg @ Dead(_, _, nodeAddress)) if sender == nodeAddress =>
              changeNodeState(nodeAddress, NodeState.Left).ignore
                .as(Message.Broadcast(msg))
                .commit

            case Message.BestEffort(_, msg @ Dead(_, _, nodeAddress)) =>
              nodeState(nodeAddress)
                .orElseSucceed(NodeState.Dead)
                .flatMap {
                  case NodeState.Dead =>
                    STM.succeed(Message.NoResponse)
                  case _              =>
                    changeNodeState(nodeAddress, NodeState.Dead).ignore
                      .as(Message.Broadcast(msg))
                }
                .commit

            case Message.BestEffort(_, msg @ Alive(_, nodeAddress)) =>
              ZSTM.atomically(
                SuspicionTimeout.cancelTimeout(nodeAddress) *>
                  changeNodeState(nodeAddress, NodeState.Alive).ignore
                    .as(Message.Broadcast(msg))
              )
          },
          ZStream
            .repeatEffectWith(
              nextNode().zip(MessageSequenceNo.next).commit,
              Schedule.forever.addDelayM(_ => LocalHealthMultiplier.scaleTimeout(protocolPeriod).commit)
            )
            .collectM { case (Some((probedNodeName, probeNode)), seqNo) =>
              ZSTM.atomically(
                MessageAcknowledge.register(Ack(seqNo)) *>
                  IncarnationSequence.current.flatMap(incarnation =>
                    Message.withScaledTimeout(
                      if (probeNode.state != NodeState.Alive)
                        Message.Batch(
                          Message.BestEffort(probedNodeName, Ping(seqNo)),
                          //this is part of buddy system
                          Message.BestEffort(probedNodeName, Suspect(incarnation, localNode, probedNodeName))
                        )
                      else
                        Message.BestEffort(probedNodeName, Ping(seqNo)),
                      pingTimeoutAction(
                        seqNo,
                        probedNodeName,
                        localNode,
                        protocolTimeout
                      ),
                      protocolTimeout
                    )
                  )
              )
            }
        )
      }

  private def pingTimeoutAction(
    seqNo: Long,
    probedNode: NodeName,
    localNode: NodeName,
    protocolTimeout: Duration
  ): ZIO[
    Has[LocalHealthMultiplier] with Has[Nodes] with Logging with Has[MessageAcknowledge] with Has[
      SuspicionTimeout
    ] with Has[IncarnationSequence],
    Error,
    Message[messages.FailureDetection]
  ] =
    ZIO.ifM(MessageAcknowledge.isCompleted(Ack(seqNo)).commit)(
      Message.noResponse,
      log.warn(s"node: $probedNode missed ack with id ${seqNo}") *>
        ZSTM.atomically(
          LocalHealthMultiplier.increase *>
            nextNode(Some(probedNode)).flatMap {
              case Some((next, _)) =>
                MessageAcknowledge.register(Nack(seqNo)) *>
                  Message.withScaledTimeout(
                    Message.BestEffort(next, PingReq(seqNo, probedNode)),
                    pingReqTimeoutAction(
                      seqNo = seqNo,
                      probedNode = probedNode,
                      localNode = localNode
                    ),
                    protocolTimeout
                  )

              case None =>
                // we don't know any other node to ask
                changeNodeState(probedNode, NodeState.Dead).as(Message.NoResponse)
            }
        )
    )

  private def pingReqTimeoutAction(
    seqNo: Long,
    probedNode: NodeName,
    localNode: NodeName
  ): ZIO[
    Has[Nodes] with Has[LocalHealthMultiplier] with Has[MessageAcknowledge] with Has[SuspicionTimeout] with Has[
      IncarnationSequence
    ],
    Error,
    Message[messages.FailureDetection]
  ] =
    ZIO.ifM(MessageAcknowledge.isCompleted(Ack(seqNo)).commit)(
      Message.noResponse,
      ZSTM.atomically(
        MessageAcknowledge.ack(Ack(seqNo)) *>
          (LocalHealthMultiplier.increase *> MessageAcknowledge.ack(Nack(seqNo)))
            .whenM(MessageAcknowledge.isCompleted(Nack(seqNo)).map(!_)) *>
          changeNodeState(probedNode, NodeState.Suspect) *>
          IncarnationSequence.current.flatMap(currentIncarnation =>
            Message.withTimeout[Has[SuspicionTimeout], messages.FailureDetection](
              Message.Broadcast(Suspect(currentIncarnation, localNode, probedNode)),
              SuspicionTimeout
                .registerTimeout(probedNode)
                .commit
                .flatMap(_.awaitAction)
                .orElse(Message.noResponse),
              Duration.Zero
            )
          )
      )
    )
}
