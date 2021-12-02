package zio.memberlist.discovery

import zio.duration.Duration
import zio.logging.{Logger, Logging}
import zio.memberlist.Error
import zio.nio.core.{InetAddress, InetSocketAddress}
import zio.{Has, IO, Layer, UIO, ZIO, ZLayer}

trait Discovery {
  def discoverNodes: IO[Error, Set[InetSocketAddress]]
}

object Discovery {

  def discoverNodes: ZIO[Has[Discovery], Error, Set[InetSocketAddress]] =
    ZIO.accessM[Has[Discovery]](_.get.discoverNodes)

  def staticList(addresses: Set[InetSocketAddress]): Layer[Nothing, Has[Discovery]] =
    ZLayer.succeed {
      new Discovery {
        final override val discoverNodes: UIO[Set[InetSocketAddress]] =
          UIO.succeed(addresses)
      }
    }

  /**
   * This discovery strategy uses K8 service headless service dns to find other members of the cluster.
   *
   * Headless service is a service of type ClusterIP with the clusterIP property set to None.
   */
  def k8Dns(address: InetAddress, timeout: Duration, port: Int): ZLayer[Logging, Nothing, Has[Discovery]] =
    ZLayer.fromFunction { logging =>
      new K8DnsDiscovery(logging.get[Logger[String]], address, timeout, port)
    }
}
