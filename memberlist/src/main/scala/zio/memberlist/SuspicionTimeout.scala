package zio.memberlist

import java.util.concurrent.TimeUnit

import zio.clock.{ currentTime, Clock }
import zio.duration._
import zio.logging.{ Logger, Logging }
import zio.memberlist.Nodes._
import zio.memberlist.SwimError.{ SuspicionTimeoutAlreadyStarted, SuspicionTimeoutCancelled }
import zio.memberlist.protocols.FailureDetection
import zio.memberlist.protocols.FailureDetection.Dead
import zio.stm.{ TQueue, _ }
import zio.{ IO, UIO, URIO, ZIO, ZLayer }

object SuspicionTimeout {

  trait Service {
    def registerTimeout[A](node: NodeAddress): STM[SuspicionTimeoutAlreadyStarted, Timeout]
    def cancelTimeout(node: NodeAddress): USTM[Unit]
    def incomingSuspect(node: NodeAddress, from: NodeAddress): USTM[Unit]
  }

  def registerTimeout[A](node: NodeAddress): ZSTM[SuspicionTimeout, SuspicionTimeoutAlreadyStarted, Timeout] =
    ZSTM.accessM[SuspicionTimeout](_.get.registerTimeout(node))

  def incomingSuspect(node: NodeAddress, from: NodeAddress): URSTM[SuspicionTimeout, Unit] =
    ZSTM.accessM[SuspicionTimeout](_.get.incomingSuspect(node, from))

  def cancelTimeout(node: NodeAddress): URSTM[SuspicionTimeout, Unit] =
    ZSTM.accessM[SuspicionTimeout](_.get.cancelTimeout(node))

  private case class SuspicionTimeoutEntry(
    queue: TQueue[TimeoutCmd]
  )

  final class Timeout(
    val node: NodeAddress,
    min: Duration,
    max: Duration,
    start: TRef[Option[Long]],
    end: TRef[Option[Long]],
    confirmations: TSet[NodeAddress],
    commands: TQueue[TimeoutCmd],
    promise: TPromise[Error, Message[FailureDetection]],
    suspicionRequiredConfirmations: Int,
    store: TMap[NodeAddress, SuspicionTimeoutEntry],
    env: Clock with Nodes with Logging
  ) {

    val nodes  = env.get[Nodes.Service]
    val clock  = env.get[Clock.Service]
    val logger = env.get[Logger[String]]

    private val action: STM[Error, Message[Dead]] =
      ZSTM
        .ifM(nodeState(node).map(_ == NodeState.Suspicion).orElseSucceed(false))(
          changeNodeState(node, NodeState.Dead)
            .as(Message.Broadcast(Dead(node))),
          ZSTM.succeed(Message.NoResponse)
        )
        .provide(env)

    val awaitAction: IO[Error, Message[FailureDetection]] =
      promise.await.commit

    val awaitStart: UIO[Unit] =
      start.get.flatMap(v => ZSTM.check(v.isDefined)).commit

    val elapsedTimeMs: UIO[Long] = start.get.zip(end.get).commit.flatMap {
      case (Some(startTime), Some(endTime)) => ZIO.succeedNow(endTime - startTime)
      case (Some(startTime), None)          => clock.currentTime(TimeUnit.MILLISECONDS).map(_ - startTime)
      case (_, _)                           => ZIO.succeedNow(0) //not started yet
    }

    def recalculate: ZIO[Any, Error, Unit] =
      for {
        startTime <- start.get.commit.get
                      .orElse(clock.currentTime(TimeUnit.MILLISECONDS).tap(time => start.set(Some(time)).commit))
        confirmationsSize <- confirmations.size.commit
        timeout <- calculateTimeout(
                    start = startTime,
                    max = max,
                    min = min,
                    k = suspicionRequiredConfirmations,
                    c = confirmationsSize
                  ).provide(env)
        _ <- logger.info(s"schedule suspicious for $node with timeout: ${timeout.toMillis} ms")
        _ <- (clock.sleep(timeout) *> clock
              .currentTime(TimeUnit.MILLISECONDS)
              .flatMap(currentTime =>
                ZSTM.atomically(
                  action.flatMap(msg => promise.succeed(msg)) <* end.set(Some(currentTime)) <* store.delete(node)
                )
              )).raceFirst(
              commands.take.commit.flatMap {
                case TimeoutCmd.Cancel =>
                  logger.info(s"suspicious timeout for $node has been cancelled") *>
                    promise.fail(SuspicionTimeoutCancelled(node)).commit
                case TimeoutCmd.NewConfirmation(node) =>
                  confirmations.put(node).commit *>
                    recalculate
              }
            )
      } yield ()
  }

  sealed trait TimeoutCmd
  object TimeoutCmd {
    case class NewConfirmation(node: NodeAddress) extends TimeoutCmd
    case object Cancel                            extends TimeoutCmd
  }

  def live(
    protocolInterval: Duration,
    suspicionAlpha: Int,
    suspicionBeta: Int,
    suspicionRequiredConfirmations: Int
  ): ZLayer[Clock with Nodes with Logging, Nothing, SuspicionTimeout] = ZLayer.fromEffect(
    TMap
      .empty[NodeAddress, SuspicionTimeoutEntry]
      .commit
      .zip(
        TQueue
          .bounded[Timeout](100)
          .commit
          .tap(starter =>
            starter.take.commit
              .flatMap(timeout =>
                zio.logging.log.info(s"starting timeout for ${timeout.node}}") *>
                  timeout.recalculate
              )
              .forever
              .fork
          )
      )
      .zip(ZIO.environment[Clock with Nodes with Logging])
      .map {
        case ((store, startTimeout), env) =>
          val nodes = env.get[Nodes.Service]

          new Service {

            override def cancelTimeout(node: NodeAddress): USTM[Unit] =
              store
                .get(node)
                .flatMap[Any, Nothing, Unit] {
                  case Some(entry) =>
                    entry.queue.offer(TimeoutCmd.Cancel)
                  case None => STM.unit
                }
                .unit

            override def registerTimeout[A](node: NodeAddress): STM[SuspicionTimeoutAlreadyStarted, Timeout] =
              for {
                _             <- STM.fail(SuspicionTimeoutAlreadyStarted(node)).whenM(store.contains(node))
                queue         <- TQueue.bounded[TimeoutCmd](1)
                numberOfNodes <- nodes.numberOfNodes
                nodeScale     = math.max(1.0, math.log10(math.max(1.0, numberOfNodes.toDouble)))
                min           = protocolInterval * suspicionAlpha.toDouble * nodeScale
                max           = min * suspicionBeta.toDouble
                entry         = SuspicionTimeoutEntry(queue)
                _             <- store.put(node, entry)
                startTimeTRef <- TRef.make[Option[Long]](None)
                endTimeTRef   <- TRef.make[Option[Long]](None)
                confirmations <- TSet.make[NodeAddress]()
                onComplete    <- TPromise.make[Error, Message[FailureDetection]]
                timeout = new Timeout(
                  node = node,
                  min = min,
                  max = max,
                  start = startTimeTRef,
                  end = endTimeTRef,
                  confirmations = confirmations,
                  commands = queue,
                  promise = onComplete,
                  suspicionRequiredConfirmations = suspicionRequiredConfirmations,
                  store = store,
                  env = env
                )
                _ <- startTimeout.offer(timeout)
              } yield timeout

            override def incomingSuspect(node: NodeAddress, from: NodeAddress): USTM[Unit] =
              store
                .get(node)
                .flatMap {
                  case Some(entry) =>
                    entry.queue.offer(TimeoutCmd.NewConfirmation(from))
                  case _ => STM.unit
                }

          }
      }
  )

  private def calculateTimeout(
    start: Long,
    max: Duration,
    min: Duration,
    k: Int,
    c: Int
  ): URIO[Clock, Duration] =
    currentTime(TimeUnit.MILLISECONDS).map { currentTime =>
      val elapsed = currentTime - start
      val frac    = math.log(c.toDouble + 1.0) / math.log(k.toDouble + 1.0)
      val raw     = max.toMillis - frac * (max.toMillis - min.toMillis)
      val timeout = math.floor(raw).toLong
      if (timeout < min.toMillis) {
        Duration(min.toMillis - elapsed, TimeUnit.MILLISECONDS)
      } else {
        Duration(timeout - elapsed, TimeUnit.MILLISECONDS)
      }
    }

}
