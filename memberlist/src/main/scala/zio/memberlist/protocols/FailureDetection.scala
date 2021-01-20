package zio.memberlist.protocols

import upickle.default._
import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.memberlist.state.Nodes._
import zio.memberlist.state.NodeState
import zio.memberlist._
import zio.memberlist.encoding.ByteCodec
import zio.stm.{ STM, TMap, ZSTM }
import zio.stream.ZStream
import zio.{ Schedule, ZIO }

sealed trait FailureDetection

object FailureDetection {

  final case class Ping(seqNo: Long) extends FailureDetection

  final case class Ack(seqNo: Long) extends FailureDetection

  final case class Nack(seqNo: Long) extends FailureDetection

  final case class PingReq(seqNo: Long, target: NodeAddress) extends FailureDetection

  final case class Suspect(incarnation: Long, from: NodeAddress, nodeId: NodeAddress) extends FailureDetection

  final case class Alive(incarnation: Long, nodeId: NodeAddress) extends FailureDetection

  final case class Dead(incarnation: Long, from: NodeAddress, nodeId: NodeAddress) extends FailureDetection

  implicit val ackCodec: ByteCodec[Ack] =
    ByteCodec.fromReadWriter(macroRW[Ack])

  implicit val nackCodec: ByteCodec[Nack] =
    ByteCodec.fromReadWriter(macroRW[Nack])

  implicit val pingCodec: ByteCodec[Ping] =
    ByteCodec.fromReadWriter(macroRW[Ping])

  implicit val pingReqCodec: ByteCodec[PingReq] =
    ByteCodec.fromReadWriter(macroRW[PingReq])

  implicit val suspectCodec: ByteCodec[Suspect] =
    ByteCodec.fromReadWriter(macroRW[Suspect])

  implicit val aliveCodec: ByteCodec[Alive] =
    ByteCodec.fromReadWriter(macroRW[Alive])

  implicit val deadCodec: ByteCodec[Dead] =
    ByteCodec.fromReadWriter(macroRW[Dead])

  implicit val byteCodec: ByteCodec[FailureDetection] =
    ByteCodec.tagged[FailureDetection][
      Ack,
      Ping,
      PingReq,
      Nack,
      Suspect,
      Alive,
      Dead
    ]

  type Env = LocalHealthMultiplier
    with MessageSequence
    with IncarnationSequence
    with Nodes
    with Logging
    with MessageAcknowledge
    with Clock
    with SuspicionTimeout

  def protocol(
    protocolPeriod: Duration,
    protocolTimeout: Duration,
    localNode: NodeAddress
  ): ZIO[Env, Error, Protocol[FailureDetection]] =
    TMap
      .empty[Long, (NodeAddress, Long)]
      .commit
      .flatMap { pingReqs =>
        Protocol[FailureDetection].make(
          {
            case Message.BestEffort(sender, msg @ Ack(seqNo)) =>
              log.debug(s"received ack[$seqNo] from $sender") *>
                ZSTM.atomically(
                  MessageAcknowledge.ack(msg) *>
                    pingReqs.get(seqNo).map {
                      case Some((originalNode, originalSeqNo)) =>
                        Message.BestEffort(originalNode, Ack(originalSeqNo))
                      case None =>
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
                          case _ =>
                            STM.succeedNow(Message.NoResponse)
                        }
                        .commit,
                      timeout = protocolTimeout
                    )
                )
                .commit
            case Message.BestEffort(_, msg @ Nack(_)) =>
              MessageAcknowledge.ack(msg).commit *>
                Message.noResponse

            case Message.BestEffort(sender, Suspect(accusedIncarnation, _, `localNode`)) =>
              ZSTM.atomically(
                IncarnationSequence.nextAfter(accusedIncarnation).map { incarnation =>
                  val alive = Alive(incarnation, localNode)
                  Message.Batch(Message.BestEffort(sender, alive), Message.Broadcast(alive))
                } <* LocalHealthMultiplier.increase
              )

            case Message.BestEffort(from, Suspect(_, _, node)) =>
              nodeState(node)
                .orElseSucceed(NodeState.Dead)
                .flatMap {
                  case NodeState.Suspect =>
                    SuspicionTimeout.incomingSuspect(node, from)
                  case NodeState.Dead =>
                    STM.unit
                  case _ =>
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
                  case _ =>
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
            .collectM {
              case (Some((probedNode, state)), seqNo) =>
                ZSTM.atomically(
                  MessageAcknowledge.register(Ack(seqNo)) *>
                    IncarnationSequence.current.flatMap(incarnation =>
                      Message.withScaledTimeout(
                        if (state != NodeState.Alive)
                          Message.Batch(
                            Message.BestEffort(probedNode, Ping(seqNo)),
                            //this is part of buddy system
                            Message.BestEffort(probedNode, Suspect(incarnation, localNode, probedNode))
                          )
                        else
                          Message.BestEffort(probedNode, Ping(seqNo)),
                        pingTimeoutAction(
                          seqNo,
                          probedNode,
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
    probedNode: NodeAddress,
    localNode: NodeAddress,
    protocolTimeout: Duration
  ): ZIO[
    LocalHealthMultiplier with Nodes with Logging with MessageAcknowledge with SuspicionTimeout with IncarnationSequence,
    Error,
    Message[FailureDetection]
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
    probedNode: NodeAddress,
    localNode: NodeAddress
  ): ZIO[
    Nodes with LocalHealthMultiplier with MessageAcknowledge with SuspicionTimeout with IncarnationSequence,
    Error,
    Message[FailureDetection]
  ] =
    ZIO.ifM(MessageAcknowledge.isCompleted(Ack(seqNo)).commit)(
      Message.noResponse,
      ZSTM.atomically(
        MessageAcknowledge.ack(Ack(seqNo)) *>
          (LocalHealthMultiplier.increase *> MessageAcknowledge.ack(Nack(seqNo)))
            .whenM(MessageAcknowledge.isCompleted(Nack(seqNo)).map(!_)) *>
          changeNodeState(probedNode, NodeState.Suspect) *>
          IncarnationSequence.current.flatMap(currentIncarnation =>
            Message.withTimeout[SuspicionTimeout, FailureDetection](
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
