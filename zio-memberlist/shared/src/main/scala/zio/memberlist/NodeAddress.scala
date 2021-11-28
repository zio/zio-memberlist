package zio.memberlist

import upickle.default._
import zio.memberlist.TransportError._
import zio.memberlist.encoding.ByteCodec
import zio.nio.core.{InetAddress, InetSocketAddress, SocketAddress}
import zio.{Chunk, IO, UIO}

final case class NodeAddress(ip: Chunk[Byte], port: Int) {

  override def equals(obj: Any): Boolean = obj match {
    case NodeAddress(ip, port) => this.port == port && ip == this.ip
    case _                     => false
  }

  override def hashCode(): Int = port

  def socketAddress: IO[TransportError, InetSocketAddress] =
    (for {
      addr <- InetAddress.byAddress(ip)
      sa   <- InetSocketAddress.inetAddress(addr, port)
    } yield sa).mapError(ExceptionWrapper)

  override def toString: String = ip.mkString(".") + ": " + port
}

object NodeAddress {

  def fromSocketAddress(addr: InetSocketAddress): UIO[NodeAddress] =
    addr.hostString.flatMap(host =>
      InetAddress
        .byName(host)
        .map(inet => NodeAddress(inet.address, addr.port))
        .orDie
    )

  def local(port: Int): UIO[NodeAddress] =
    InetAddress.localHost
      .map(addr => NodeAddress(addr.address, port))
      .orDie

  implicit val nodeAddressRw: ReadWriter[NodeAddress] = macroRW[NodeAddress]

  implicit val byteCodec: ByteCodec[NodeAddress] =
    ByteCodec.fromReadWriter(nodeAddressRw)

}
