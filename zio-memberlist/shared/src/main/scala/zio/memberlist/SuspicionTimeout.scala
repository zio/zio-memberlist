package zio.memberlist

import zio.clock.{Clock, currentTime}
import zio.config._
import zio.duration._
import zio.logging.{Logger, Logging}
import zio.memberlist.SuspicionTimeout.Timeout
import zio.memberlist.SwimError.{SuspicionTimeoutAlreadyStarted, SuspicionTimeoutCancelled}
import zio.memberlist.protocols.messages.FailureDetection
import zio.memberlist.protocols.messages.FailureDetection.Dead
import zio.memberlist.state.Nodes._
import zio.memberlist.state.{NodeState, _}
import zio.stm.{TQueue, _}
import zio.{Has, IO, UIO, URIO, ZIO, ZLayer}

import java.util.concurrent.TimeUnit
trait SuspicionTimeout  {
  def registerTimeout[A](node: NodeName): STM[SuspicionTimeoutAlreadyStarted, Timeout]
  def cancelTimeout(node: NodeName): USTM[Unit]
  def incomingSuspect(node: NodeName, from: NodeName): USTM[Unit]
}
object SuspicionTimeout {

  def registerTimeout[A](node: NodeName): ZSTM[Has[SuspicionTimeout], SuspicionTimeoutAlreadyStarted, Timeout] =
    ZSTM.accessM[Has[SuspicionTimeout]](_.get.registerTimeout(node))

  def incomingSuspect(node: NodeName, from: NodeName): URSTM[Has[SuspicionTimeout], Unit] =
    ZSTM.accessM[Has[SuspicionTimeout]](_.get.incomingSuspect(node, from))

  def cancelTimeout(node: NodeName): URSTM[Has[SuspicionTimeout], Unit] =
    ZSTM.accessM[Has[SuspicionTimeout]](_.get.cancelTimeout(node))

  final case class SuspicionTimeoutEntry(
    queue: TQueue[TimeoutCmd]
  )

  final class Timeout(
    val node: NodeName,
    min: Duration,
    max: Duration,
    start: TRef[Option[Long]],
    end: TRef[Option[Long]],
    confirmations: TSet[NodeName],
    commands: TQueue[TimeoutCmd],
    promise: TPromise[Error, Message[FailureDetection]],
    suspicionRequiredConfirmations: Int,
    store: TMap[NodeName, SuspicionTimeoutEntry],
    env: Clock with Has[Nodes] with Logging with Has[IncarnationSequence]
  ) {

    val nodes: Nodes                   = env.get[Nodes]
    val clock: Clock.Service           = env.get[Clock.Service]
    val logger: Logger[String]         = env.get[Logger[String]]
    val currentIncarnation: USTM[Long] = env.get[IncarnationSequence].current

    private val action: STM[Error, Message[Dead]] =
      ZSTM
        .ifM(nodeState(node).map(_ == NodeState.Suspect).orElseSucceed(false))(
          changeNodeState(node, NodeState.Dead) *>
            currentIncarnation.map(incarnation => Message.Broadcast(Dead(incarnation, nodes.localNodeName, node))),
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
        startTime         <- start.get.commit.get
                               .orElse(clock.currentTime(TimeUnit.MILLISECONDS).tap(time => start.set(Some(time)).commit))
        confirmationsSize <- confirmations.size.commit
        timeout           <- calculateTimeout(
                               start = startTime,
                               max = max,
                               min = min,
                               k = suspicionRequiredConfirmations,
                               c = confirmationsSize
                             ).provide(env)
        _                 <- logger.info(s"schedule suspicious for $node with timeout: ${timeout.toMillis} ms")
        _                 <- (clock.sleep(timeout) *> clock
                               .currentTime(TimeUnit.MILLISECONDS)
                               .flatMap(currentTime =>
                                 ZSTM.atomically(
                                   action.flatMap(msg => promise.succeed(msg)) <* end.set(Some(currentTime)) <* store.delete(node)
                                 )
                               )).raceFirst(
                               commands.take.commit.flatMap {
                                 case TimeoutCmd.Cancel                =>
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
    case class NewConfirmation(node: NodeName) extends TimeoutCmd
    case object Cancel                         extends TimeoutCmd
  }

  private def live0(
    protocolInterval: Duration,
    suspicionAlpha: Int,
    suspicionBeta: Int,
    suspicionRequiredConfirmations: Int
  ): ZIO[Clock with Has[Nodes] with Logging with Has[IncarnationSequence], Nothing, SuspicionTimeout] =
    TMap
      .empty[NodeName, SuspicionTimeoutEntry]
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
      .zip(ZIO.environment[Clock with Has[Nodes] with Logging with Has[IncarnationSequence]])
      .map { case ((store, startTimeout), env) =>
        val nodes = env.get[Nodes]

        new SuspicionTimeout {

          override def cancelTimeout(node: NodeName): USTM[Unit] =
            store
              .get(node)
              .flatMap[Any, Nothing, Unit] {
                case Some(entry) =>
                  entry.queue.offer(TimeoutCmd.Cancel)
                case None        => STM.unit
              }
              .unit

          override def registerTimeout[A](node: NodeName): STM[SuspicionTimeoutAlreadyStarted, Timeout] =
            for {
              _             <- STM.fail(SuspicionTimeoutAlreadyStarted(node)).whenM(store.contains(node))
              queue         <- TQueue.bounded[TimeoutCmd](1)
              numberOfNodes <- nodes.numberOfNodes
              nodeScale      = math.max(1.0, math.log10(math.max(1.0, numberOfNodes.toDouble)))
              min            = protocolInterval * suspicionAlpha.toDouble * nodeScale
              max            = min * suspicionBeta.toDouble
              entry          = SuspicionTimeoutEntry(queue)
              _             <- store.put(node, entry)
              startTimeTRef <- TRef.make[Option[Long]](None)
              endTimeTRef   <- TRef.make[Option[Long]](None)
              confirmations <- TSet.make[NodeName]()
              onComplete    <- TPromise.make[Error, Message[FailureDetection]]
              timeout        = new Timeout(
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
              _             <- startTimeout.offer(timeout)
            } yield timeout

          override def incomingSuspect(node: NodeName, from: NodeName): USTM[Unit] =
            store
              .get(node)
              .flatMap {
                case Some(entry) =>
                  entry.queue.offer(TimeoutCmd.NewConfirmation(from))
                case _           => STM.unit
              }

        }
      }

  def live(
    protocolInterval: Duration,
    suspicionAlpha: Int,
    suspicionBeta: Int,
    suspicionRequiredConfirmations: Int
  ): ZLayer[Clock with Has[Nodes] with Logging with Has[IncarnationSequence], Nothing, Has[SuspicionTimeout]] =
    live0(protocolInterval, suspicionAlpha, suspicionBeta, suspicionRequiredConfirmations).toLayer

  val liveWithConfig: ZLayer[Has[
    MemberlistConfig
  ] with Clock with Has[Nodes] with Logging with Has[IncarnationSequence], Nothing, Has[SuspicionTimeout]] =
    getConfig[MemberlistConfig]
      .flatMap(config =>
        live0(
          config.protocolInterval,
          config.suspicionAlpha,
          config.suspicionBeta,
          config.suspicionRequiredConfirmations
        )
      )
      .toLayer

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
