package zio.memberlist.discovery

import zio.duration.Duration
import zio.logging.Logger
import zio.memberlist.{Error, ServiceDiscoveryError}
import zio.nio.core.{InetAddress, InetSocketAddress}
import zio.{IO, UIO, ZIO}

import java.net.UnknownHostException
import java.util
import javax.naming.directory.InitialDirContext
import javax.naming.{Context, NamingException}

private class K8DnsDiscovery(
  log: Logger[String],
  serviceDns: InetAddress,
  serviceDnsTimeout: Duration,
  servicePort: Int
) extends Discovery {

  final override val discoverNodes: IO[Error, Set[InetSocketAddress]] = {
    for {
      _         <- log.info(s"k8s dns dicovery: $serviceDns, port: $servicePort, timeout: $serviceDnsTimeout")
      addresses <- lookup(serviceDns, serviceDnsTimeout)
      nodes     <- IO.foreach(addresses)(addr => InetSocketAddress.inetAddress(addr, servicePort))
    } yield nodes
  }.catchAllCause { ex =>
    log.error(s"discovery strategy ${this.getClass.getSimpleName} failed.", ex) *>
      IO.halt(ex.map(e => ServiceDiscoveryError(e.getMessage)))
  }

  private def lookup(
    serviceDns: InetAddress,
    serviceDnsTimeout: Duration
  ): IO[Exception, Set[InetAddress]] = {
    import scala.jdk.CollectionConverters._

    val env = new util.Hashtable[String, String]
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory")
    env.put(Context.PROVIDER_URL, "dns:")
    env.put("com.sun.jndi.dns.timeout.initial", serviceDnsTimeout.toMillis.toString)

    for {
      dirContext  <- IO.effect(new InitialDirContext(env)).refineToOrDie[NamingException]
      attributes  <- UIO.effectTotal(dirContext.getAttributes(serviceDns.hostName, Array("SRV")))
      srvAttribute = Option(attributes.get("srv")).toList.flatMap(_.getAll.asScala)
      addresses   <- ZIO.foldLeft(srvAttribute)(Set.empty[InetAddress]) {
                       case (acc, address: String) =>
                         extractHost(address)
                           .flatMap(InetAddress.byName)
                           .map(acc + _)
                           .refineToOrDie[UnknownHostException]
                       case (acc, _)               =>
                         UIO.succeed(acc)
                     }
    } yield addresses
  }

  private def extractHost(server: String): UIO[String] =
    log.debug(s"k8 dns on response: $server") *>
      UIO.effectTotal {
        val host = server.split(" ")(3)
        host.replaceAll("\\\\.$", "")
      }
}
