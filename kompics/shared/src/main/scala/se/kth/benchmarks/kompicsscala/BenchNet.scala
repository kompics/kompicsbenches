package se.kth.benchmarks.kompicsscala

import java.net.{InetAddress, InetSocketAddress}
import se.sics.kompics.network.{Address, Header, Msg, Transport}
import se.sics.kompics.KompicsEvent
import scala.util.{Failure, Success, Try}
import se.sics.kompics.network.netty.serialization.{Serializer, Serializers}
import java.util.Optional
import io.netty.buffer.ByteBuf
import se.kth.benchmarks.kompicsjava.net.{NetAddress => JNetAddress}

object BenchNet {
  def registerSerializers(): Unit = {
    Serializers.register(BenchNetSerializer, "bench-net");
    Serializers.register(classOf[NetAddress], "bench-net");
    Serializers.register(classOf[NetHeader], "bench-net");
    Serializers.register(classOf[NetMessage[_]], "bench-net");
  }
}

final case class NetAddress(isa: InetSocketAddress) extends Address {
  override def asSocket(): InetSocketAddress = isa;
  override def getIp(): InetAddress = isa.getAddress;
  override def getPort(): Int = isa.getPort;
  override def sameHostAs(other: Address): Boolean = {
    this.isa.equals(other.asSocket())
  }

  def asJava: JNetAddress = new JNetAddress(isa);

  def asString: String = s"${isa.getHostString()}:${getPort()}";
}
object NetAddress {
  def from(ip: String, port: Int): Try[NetAddress] = Try {
    val isa = new InetSocketAddress(ip, port);
    NetAddress(isa)
  };

  def fromString(str: String): Try[NetAddress] =
    Try {
      val split = str.split(":");
      assert(split.length == 2);
      val ipStr = split(0); //.replaceAll("""/""", "");
      val portStr = split(1);
      val port = portStr.toInt;
      NetAddress.from(ipStr, port)
    }.flatten;
}

final case class NetHeader(src: NetAddress, dst: NetAddress, proto: Transport) extends Header[NetAddress] {
  override def getDestination(): NetAddress = dst;
  override def getProtocol(): Transport = proto;
  override def getSource(): NetAddress = src;
}

final case class NetMessage[C <: KompicsEvent](header: NetHeader, payload: C) extends Msg[NetAddress, NetHeader] {
  override def getDestination(): NetAddress = header.dst;
  override def getHeader(): NetHeader = header;
  override def getProtocol(): Transport = header.proto;
  override def getSource(): NetAddress = header.src;

  def reply[C2 <: KompicsEvent](rsrc: NetAddress)(payload: C2) =
    NetMessage(NetHeader(rsrc, header.src, header.proto), payload);
}

object NetMessage {
  def viaTCP[C <: KompicsEvent](src: NetAddress, dst: NetAddress)(payload: C) =
    NetMessage(NetHeader(src, dst, Transport.TCP), payload);
}

object BenchNetSerializer extends Serializer {

  val NO_HINT: Optional[Object] = Optional.empty();

  private val NET_ADDRESS_FLAG: Byte = 1;
  private val NET_HEADER_FLAG: Byte = 2;
  private val NET_MSG_FLAG: Byte = 3;

  override def identifier(): Int = se.kth.benchmarks.kompics.SerializerIds.S_BENCH_NET;

  override def toBinary(o: Any, buf: ByteBuf): Unit = {
    o match {
      case addr: NetAddress => {
        buf.writeByte(NET_ADDRESS_FLAG);
        serNetAddress(addr, buf)
      }
      case header: NetHeader => {
        buf.writeByte(NET_HEADER_FLAG);
        serNetHeader(header, buf)
      }
      case msg: NetMessage[_] => {
        buf.writeByte(NET_MSG_FLAG);
        serNetMsg(msg, buf)
      }
    }
  }

  override def fromBinary(buf: ByteBuf, hint: Optional[Object]): Object = {
    val flag = buf.readByte();
    flag match {
      case NET_ADDRESS_FLAG => deserNetAddress(buf)
      case NET_HEADER_FLAG  => deserNetHeader(buf)
      case NET_MSG_FLAG     => deserNetMsg(buf)
      case _ => {
        Console.err.print(s"Got invalid ser flag: $flag");
        null
      }
    }
  }

  def serNetAddress(o: NetAddress, buf: ByteBuf): Unit = {
    val ip = o.isa.getAddress.getAddress;
    assert(ip.length == 4);
    buf.writeBytes(ip);
    val port = o.getPort();
    buf.writeShort(port);
  }
  def deserNetAddress(buf: ByteBuf): NetAddress = {
    val ipBytes: Array[Byte] = Array(0x0, 0x0, 0x0, 0x0);
    buf.readBytes(ipBytes);
    val ip = InetAddress.getByAddress(ipBytes);
    val port = buf.readUnsignedShort();
    val isa = new InetSocketAddress(ip, port);
    NetAddress(isa)
  }

  def serNetHeader(o: NetHeader, buf: ByteBuf): Unit = {
    serNetAddress(o.src, buf);
    serNetAddress(o.dst, buf);
    val protocolId = o.proto.ordinal();
    buf.writeByte(protocolId);
  }
  def deserNetHeader(buf: ByteBuf): NetHeader = {
    val src = deserNetAddress(buf);
    val dst = deserNetAddress(buf);
    val protocolId = buf.readByte();
    val proto = Transport.values()(protocolId);
    NetHeader(src, dst, proto)
  }

  def serNetMsg(o: NetMessage[_], buf: ByteBuf): Unit = {
    serNetHeader(o.header, buf);
    Serializers.toBinary(o.payload, buf);
  }
  def deserNetMsg(buf: ByteBuf): NetMessage[_] = {
    val header = deserNetHeader(buf);
    val payload = Serializers.fromBinary(buf, NO_HINT).asInstanceOf[KompicsEvent];
    NetMessage(header, payload)
  }
}
