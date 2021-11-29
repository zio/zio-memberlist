---
id: overview_index
title: "Getting Started"
---

ZIO-memberlist let you form cluster of multiply machines and by using gossip protocol which sends periodically messages between nodes maintain cluster and detects failining nodes. 

Target for this library are users that builds distributed systems from the groud up. It could potentially be used to build next generation analytics platform, distributed caching solution or monitoring solutions to name a few. 

Name of the project come from name of Hashicorp project [memberlist](https://github.com/hashicorp/memberlist). 

## Installation

`ZIO-memberlist` is available via maven repo so import in `build.sbt` is sufficient:

```scala
libraryDependencies += "dev.zio" %% "zio-memberlist" % version
```

## First Cluster

### Seeds nodes

In most if not all of membership protocols we have a concept of seeds nodes. It is list of addresses to cluster nodes that new nodes will use to join a cluster. This list could be static list of IP addresses or it could be dns name that is dynamically updated when members join and leave. In `ZIO-memberslist` we represent this via [`zio.memberlist.discovery.Discovery.Service`](https://github.com/zio/zio-memberlist/blob/master/memberlist/src/main/scala/zio/memberlist/discovery/Discovery.scala).
We provide two implementations that should allow you to start cluster.

#### Static list of nodes
```scala
import zio.nio.core.SocketAddress._

val seeds = Discovery.staticList(
              Set(
                inetSocketAddress("0.0.0.0", 5555), 
                inetSocketAddress("0.0.0.0", 5556)
              )
            )
```

#### Kubernetes service DNS

To make this work you need to create headless service in k8 to represent cluster. Below example of service configuration
```
apiVersion: v1
kind: Service
metadata:
  name: zio-memberlist-srv
spec:
  clusterIP: None
  publishNotReadyAddresses=True
  ports:
    - port: 5555
      targetPort: swim
      protocol: UDP
      name: swim
  selector:
    app: zio-memberlist-node

```

and code

```scala
import zio.memberslist.discovery.Discovery
import zio.duration._
import zio.nio.core.InetAddress

val seeds = Discovery.k8Dns(
              InetAddress.byName(
                "name-of-namespace.zio-memberlist-srv", 
                10.seconds, 
                5555
              )
            )
```

## Define messages

ZIO-memberlist allow you to send messages to other nodes in the cluster. However there is important limitation. Since ZIO-memberlist uses UDP for comunication your messages need to respect MTU. 

Here is example of messages that we could use to run Chaos experiment on the cluster
```scala
import upickle.default._
import zio.memberlist._

sealed trait ChaosMonkey

object ChaosMonkey {
  final case object SimulateCpuSpike extends ChaosMonkey

  implicit val cpuSpikeCodec: ByteCodec[SimulateCpuSpike.type] =
    ByaateCodec.fromReadWriter(macroRW[SimulateCpuSpike.type])

  implicit val codec: ByteCodec[ChaosMonkey] =
    ByteCodec.tagged[ChaosMonkey][
      SimulateCpuSpike.type
    ]
}
```
As you see apart of messages we need to provide `ByteCodec` instances that will be used to decode and encode your messages.  

## Start Cluster

We need to put this all together. Let start with dependencies.
```scala
val config      = SwimConfig.fromEnv.orDie
val logging     = Logging.console((_, msg) => msg)
val dependencie = (ZLayer.requires[Clock] ++ logging ++ config) >+> seeds >+>  Memberlist.live[ChaosMonkey]
```

And program.
```scala
val program = receive[ChaosMonkey]
      .foreach {
        case (sender, message) =>
          log.info(s"receive message: $message from: $sender") *>
            ZIO.whenCase(message) {
              case SimulateCpuSpike => log.info("simulating cpu spike")
            }
      }
      .as(ExitCode.succeed)
```
