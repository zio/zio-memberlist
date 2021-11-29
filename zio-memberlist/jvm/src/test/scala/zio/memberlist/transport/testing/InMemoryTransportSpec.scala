package zio.memberlist.transport.testing

import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.memberlist.KeeperSpec
import zio.memberlist.transport.Transport
import zio.memberlist.transport.testing.InMemoryTransport.asNode
import zio.random.Random
import zio.test.Assertion._
import zio.test.TestAspect.nonFlaky
import zio.test._
import zio.test.environment.{TestClock, TestConsole, TestRandom, TestSystem}
import zio.{Chunk, Has, Promise, Schedule, ZLayer}

object InMemoryTransportSpec extends KeeperSpec {

  val spec: Spec[Has[TestConfig.Service] with Has[Random.Service] with Has[Sized.Service] with Has[
    Clock.Service
  ] with Has[zio.console.Console.Service] with Has[zio.system.System.Service] with Has[Random.Service] with Has[
    Blocking.Service
  ] with Has[Clock.Service] with Has[zio.console.Console.Service] with Has[zio.system.System.Service] with Has[
    Random.Service
  ] with Has[Blocking.Service] with Has[TestClock.Service] with Has[TestConsole.Service] with Has[
    TestRandom.Service
  ] with Has[TestSystem.Service] with Has[Annotations.Service], TestFailure[Any], TestSuccess] = {

    val environment = Clock.live >>> (ZLayer.identity[Clock] ++ InMemoryTransport.make())

    suite("InMemoryTransport")(
      testM("can send and receive messages") {
        checkM(Gen.listOf(Gen.anyByte)) { bytes =>
          val payload = Chunk.fromIterable(bytes)
          val io      = for {
            chunk  <- asNode(address(0)) {
                        Transport
                          .bind(address(0))
                          .flatMap(c => c.receive.take(1).ensuring(c.close))
                          .take(1)
                          .runHead
                      }.fork
            _      <- asNode(address(1)) {
                        Transport.send(address(0), payload).retry(Schedule.spaced(10.milliseconds))
                      }
            result <- chunk.join
          } yield result
          assertM(io.run)(succeeds(isSome(equalTo(payload)))).provideCustomLayer(environment)
        }
      },
      testM("handles interrupts") {
        val payload = Chunk.single(Byte.MaxValue)

        val io = for {
          latch  <- Promise.make[Nothing, Unit]
          fiber  <- asNode(address(0)) {
                      Transport
                        .bind(address(0))
                        .flatMap(c => c.receive.take(1).tap(_ => latch.succeed(()).ensuring(c.close)))
                        .take(2)
                        .runDrain
                    }.fork
          _      <- asNode(address(1)) {
                      Transport.send(address(0), payload).retry(Schedule.spaced(10.milliseconds))
                    }
          result <- latch.await *> fiber.interrupt
        } yield result
        assertM(io.run)(succeeds(isInterrupted)).provideCustomLayer(environment)
      },
      testM("can receive messages after bind stream is closed") {
        checkM(Gen.listOf(Gen.anyByte)) { bytes =>
          val payload = Chunk.fromIterable(bytes)

          val io = for {
            fiber  <- asNode(address(0)) {
                        Transport
                          .bind(address(0))
                          .flatMap(c => c.receive.take(1).ensuring(c.close))
                          .take(1)
                          .runHead
                      }.fork
            _      <- asNode(address(1)) {
                        Transport.send(address(0), payload).retry(Schedule.spaced(10.milliseconds))
                      }
            result <- fiber.join
          } yield result
          assertM(io.run)(succeeds(isSome(equalTo(payload)))).provideCustomLayer(environment)
        }
      },
      testM("Fails receive if connectivity is interrupted") {
        val io = for {
          fiber  <- asNode(address(0)) {
                      Transport
                        .bind(address(0))
                        .flatMap(c => c.receive.take(1).ensuring(c.close))
                        .take(1)
                        .runHead
                    }.fork
          result <- asNode(address(1)) {
                      Transport.connect(address(0)).retry(Schedule.spaced(10.milliseconds)).use_ {
                        InMemoryTransport.setConnectivity((_, _) => false) *> fiber.join
                      }
                    }
        } yield result
        assertM(io.run)(fails(anything)).provideCustomLayer(environment)
      } @@ nonFlaky(20),
      testM("Fails send if connectivity is interrupted") {

        checkM(Gen.listOf(Gen.anyByte)) { bytes =>
          val io =
            for {
              fiber  <- asNode(address(0)) {
                          Transport
                            .bind(address(0))
                            .flatMap(c => c.receive.take(1).ensuring(c.close))
                            .take(1)
                            .runHead
                        }.fork
              result <- asNode(address(1)) {
                          Transport.connect(address(0)).retry(Schedule.spaced(10.milliseconds)).use { channel =>
                            InMemoryTransport.setConnectivity((_, _) => false) *> channel
                              .send(Chunk.fromIterable(bytes)) <* fiber.await
                          }
                        }
            } yield result
          assertM(io.run)(fails(anything)).provideCustomLayer(environment)
        }
      }
    )
  }
}
