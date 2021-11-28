package zio.memberlist

import zio.{UIO, ZIO}

import java.util.UUID

object uuid {

  val makeRandomUUID: UIO[UUID] = ZIO.effectTotal(UUID.randomUUID())

}
