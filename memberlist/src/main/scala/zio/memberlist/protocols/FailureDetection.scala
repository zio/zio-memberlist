package zio.memberlist.protocols

import upickle.default._
import zio.clock.Clock
import zio.duration._
import zio.logging._
import zio.memberlist.Nodes._
import zio.memberlist._
import zio.memberlist.encoding.ByteCodec
import zio.stm.{ STM, TMap, ZSTM }
import zio.stream.ZStream
import zio.{ Schedule, ZIO }

sealed trait FailureDetection

object FailureDetection {

  case object Ping                                                 extends FailureDetection
  case object Ack                                                  extends FailureDetection
  case object Nack                                                 extends FailureDetection
  final case class PingReq(target: NodeAddress)                    extends FailureDetection
  final case class Suspect(from: NodeAddress, nodeId: NodeAddress) extends FailureDetection
  final case class Alive(nodeId: NodeAddress)                      extends FailureDetection
  final case class Dead(nodeId: NodeAddress)                       extends FailureDetection

  implicit val ackCodec: ByteCodec[Ack.type] =
    ByteCodec.fromReadWriter(macroRW[Ack.type])

  implicit val nackCodec: ByteCodec[Nack.type] =
    ByteCodec.fromReadWriter(macroRW[Nack.type])

  implicit val pingCodec: ByteCodec[Ping.type] =
    ByteCodec.fromReadWriter(macroRW[Ping.type])

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
      Ack.type,
      Ping.type,
      PingReq,
      Nack.type,
      Suspect,
      Alive,
      Dead
    ]

  private class ProtocolFactory(
    pingReqs: TMap[Long, (NodeAddress, Long)],
    protocolPeriod: Duration,
    protocolTimeout: Duration,
    localNode: NodeAddress
  ) {

    val make = Protocol[FailureDetection].make(
      {
        case Message.Direct(sender, conversationId, Ack) =>
          log.debug(s"received ack[$conversationId] from $sender") *>
            ZSTM.atomically(
              MessageAcknowledge.ack(Ack, conversationId) *>
                pingReqs.get(conversationId).map {
                  case Some((originalNode, originalConversation)) =>
                    Message.Direct(originalNode, originalConversation, Ack)
                  case None =>
                    Message.NoResponse
                } <* LocalHealthMultiplier.decrease
            )

        case Message.Direct(sender, conversationId, Ping) =>
          ZIO.succeedNow(Message.Direct(sender, conversationId, Ack))

        case Message.Direct(sender, originalAck, PingReq(to)) =>
          Message
            .direct(to, Ping)
            .flatMap(ping =>
              pingReqs.put(ping.conversationId, (sender, originalAck)) *>
                Message.withTimeout(
                  message = ping,
                  action = pingReqs
                    .get(ping.conversationId)
                    .flatMap {
                      case Some((sender, originalAck)) =>
                        pingReqs
                          .delete(ping.conversationId)
                          .as(Message.Direct(sender, originalAck, Nack))
                      case _ =>
                        STM.succeedNow(Message.NoResponse)
                    }
                    .commit,
                  timeout = protocolTimeout
                )
            )
            .commit
        case Message.Direct(_, conversationId, Nack) =>
          MessageAcknowledge.ack(Nack, conversationId).commit *>
            Message.noResponse

        case Message.Direct(sender, _, Suspect(_, `localNode`)) =>
          ZSTM.atomically(
            Message
              .direct(sender, Alive(localNode))
              .map(
                Message.Batch(
                  _,
                  Message.Broadcast(Alive(localNode))
                )
              ) <* LocalHealthMultiplier.increase
          )

        case Message.Direct(from, _, Suspect(_, node)) =>
          nodeState(node)
            .orElseSucceed(NodeState.Dead)
            .flatMap {
              case NodeState.Suspicion =>
                SuspicionTimeout.incomingSuspect(node, from)
              case NodeState.Dead =>
                STM.unit
              case _ =>
                changeNodeState(node, NodeState.Suspicion).ignore
            }
            .commit *> Message.noResponse

        case Message.Direct(sender, _, msg @ Dead(nodeAddress)) if sender == nodeAddress =>
          changeNodeState(nodeAddress, NodeState.Left).ignore
            .as(Message.Broadcast(msg))
            .commit

        case Message.Direct(_, _, msg @ Dead(nodeAddress)) =>
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

        case Message.Direct(_, _, msg @ Alive(nodeAddress)) =>
          ZSTM.atomically(
            SuspicionTimeout.cancelTimeout(nodeAddress) *>
              changeNodeState(nodeAddress, NodeState.Healthy).ignore
                .as(Message.Broadcast(msg))
          )
      },
      ZStream
        .repeatEffectWith(
          nextNode().zip(ConversationId.next).commit,
          Schedule.forever.addDelayM(_ => LocalHealthMultiplier.scaleTimeout(protocolPeriod).commit)
        )
        .collectM {
          case (Some((probedNode, state)), conversationId) =>
            ZSTM.atomically(
              MessageAcknowledge.register(Ack, conversationId) *>
                Message.withScaledTimeout(
                  if (state != NodeState.Healthy)
                    Message.Batch(
                      Message.Direct(probedNode, conversationId, Ping),
                      //this is part of buddy system
                      Message.Direct(probedNode, conversationId, Suspect(localNode, probedNode))
                    )
                  else
                    Message.Direct(probedNode, conversationId, Ping),
                  pingTimeoutAction(
                    conversationId,
                    probedNode
                  ),
                  protocolTimeout
                )
            )
        }
    )

    private def pingTimeoutAction(
      conversationId: Long,
      probedNode: NodeAddress
    ): ZIO[
      LocalHealthMultiplier with Nodes with Logging with MessageAcknowledge with SuspicionTimeout,
      Error,
      Message[
        FailureDetection
      ]
    ] =
      ZIO.ifM(MessageAcknowledge.isCompleted(Ack, conversationId).commit)(
        Message.noResponse,
        log.warn(s"node: $probedNode missed ack with id ${conversationId}") *>
          ZSTM.atomically(
            LocalHealthMultiplier.increase *>
              nextNode(Some(probedNode)).flatMap {
                case Some((next, _)) =>
                  MessageAcknowledge.register(Nack, conversationId) *>
                    Message.withScaledTimeout(
                      Message.Direct(next, conversationId, PingReq(probedNode)),
                      pingReqTimeoutAction(
                        conversationId,
                        probedNode
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
      conversationId: Long,
      probedNode: NodeAddress
    ): ZIO[Nodes with LocalHealthMultiplier with MessageAcknowledge with SuspicionTimeout, Error, Message[
      FailureDetection
    ]] =
      ZIO.ifM(MessageAcknowledge.isCompleted(Ack, conversationId).commit)(
        Message.noResponse,
        ZSTM.atomically(
          MessageAcknowledge.ack(Ack, conversationId) *>
            (LocalHealthMultiplier.increase *> MessageAcknowledge.ack(Nack, conversationId))
              .whenM(MessageAcknowledge.isCompleted(Nack, conversationId).map(!_)) *>
            changeNodeState(probedNode, NodeState.Suspicion) *>
            Message.withTimeout[SuspicionTimeout, FailureDetection](
              Message.Broadcast(Suspect(localNode, probedNode)),
              SuspicionTimeout
                .registerTimeout(probedNode)
                .commit
                .flatMap(_.awaitAction)
                .orElse(Message.noResponse),
              Duration.Zero
            )
        )
      )
  }

  type Env = LocalHealthMultiplier
    with ConversationId
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
      .flatMap {
        case pendingAcks =>
          new ProtocolFactory(pendingAcks, protocolPeriod, protocolTimeout, localNode).make
      }

}
