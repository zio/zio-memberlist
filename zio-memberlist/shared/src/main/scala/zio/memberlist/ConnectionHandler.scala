package zio.memberlist

import zio._
import zio.logging.{Logging, log}
import zio.memberlist.encoding.ByteCodec
import zio.memberlist.protocols.messages.WithPiggyback
import zio.memberlist.transport.{Bind, Channel, ConnectionLessTransport}
import zio.stream.Take

/**
 * Logic for connection handler for udp transport
 */
object ConnectionHandler {

  private def read(connection: Channel, messages: Queue[Take[Error, Message[Chunk[Byte]]]]): IO[Error, Unit] =
    Take
      .fromEffect(
        connection.read >>= ByteCodec[WithPiggyback].fromChunk
      )
      .flatMap {
        _.foldM(
          messages.offer(Take.end),
          cause => messages.offer(Take.halt(cause)),
          values =>
            ZIO.foreach_(values) { withPiggyback =>
              val take =
                Take.single(Message.BestEffortByName(withPiggyback.node, withPiggyback.message))

              messages.offer(take) *>
                ZIO.foreach_(withPiggyback.gossip) { chunk =>
                  messages.offer(Take.single(Message.BestEffortByName(withPiggyback.node, chunk)))
                }
            }
        )
      }
      .unit

  def bind(
    local: NodeAddress,
    transport: ConnectionLessTransport,
    messages: Queue[Take[Error, Message[Chunk[Byte]]]]
  ): ZManaged[Logging, TransportError, Bind] =
    for {
      localAddress <- local.socketAddress.toManaged_
      _            <- log.info("bind to " + localAddress).toManaged_
      logger       <- ZManaged.environment[Logging]
      bind         <- transport
                        .bind(localAddress) { conn =>
                          read(conn, messages)
                            .catchAll(ex => log.error("fail to read", Cause.fail(ex)).unit.provide(logger))
                        }
    } yield bind

}
