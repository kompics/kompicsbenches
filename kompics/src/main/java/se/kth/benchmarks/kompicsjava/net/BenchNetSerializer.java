package se.kth.benchmarks.kompicsjava.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import se.sics.benchmarks.kompics.SerializerHelper;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.network.Transport;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

public class BenchNetSerializer implements Serializer {

    public static void register() {
        Serializers.register(new BenchNetSerializer(), "jbenchnet");
        Serializers.register(NetAddress.class, "jbenchnet");
        Serializers.register(NetHeader.class, "jbenchnet");
        Serializers.register(NetMessage.class, "jbenchnet");
    }

    public static final Optional<Object> NO_HINT = Optional.empty();

    private static final byte NET_ADDRESS_FLAG = 1;
    private static final byte NET_HEADER_FLAG = 2;
    private static final byte NET_MSG_FLAG = 3;

    @Override
    public int identifier() {
        return se.sics.benchmarks.kompics.SerializerIds.J_BENCH_NET;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof NetAddress) {
            NetAddress addr = (NetAddress) o;
            buf.writeByte(NET_ADDRESS_FLAG);
            serNetAddress(addr, buf);
        } else if (o instanceof NetHeader) {
            NetHeader header = (NetHeader) o;
            buf.writeByte(NET_HEADER_FLAG);
            serNetHeader(header, buf);
        } else if (o instanceof NetMessage) {
            NetMessage msg = (NetMessage) o;
            buf.writeByte(NET_MSG_FLAG);
            serNetMsg(msg, buf);
        } else {
            throw SerializerHelper.notSerializable(o.getClass().getName());
        }

    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte flag = buf.readByte();
        switch (flag) {
        case NET_ADDRESS_FLAG:
            return deserNetAddress(buf);
        case NET_HEADER_FLAG:
            return deserNetHeader(buf);
        case NET_MSG_FLAG:
            return deserNetMsg(buf);
        default:
            throw SerializerHelper.notSerializable("Invalid flag: " + flag);
        }
    }

    public void serNetAddress(NetAddress o, ByteBuf buf) {
        byte[] ip = o.isa.getAddress().getAddress();
        assert (ip.length == 4);
        buf.writeBytes(ip);
        int port = o.getPort();
        buf.writeShort(port);
    }

    public NetAddress deserNetAddress(ByteBuf buf) {
        try {
            byte[] ipBytes = new byte[4];
            buf.readBytes(ipBytes);
            InetAddress ip = InetAddress.getByAddress(ipBytes);
            int port = buf.readUnsignedShort();
            InetSocketAddress isa = new InetSocketAddress(ip, port);
            return new NetAddress(isa);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public void serNetHeader(NetHeader o, ByteBuf buf) {
        serNetAddress(o.src, buf);
        serNetAddress(o.dst, buf);
        int protocolId = o.proto.ordinal();
        buf.writeByte(protocolId);
    }

    public NetHeader deserNetHeader(ByteBuf buf) {
        NetAddress src = deserNetAddress(buf);
        NetAddress dst = deserNetAddress(buf);
        int protocolId = buf.readByte();
        Transport proto = Transport.values()[protocolId];
        return new NetHeader(src, dst, proto);
    }

    public void serNetMsg(NetMessage o, ByteBuf buf) {
        serNetHeader(o.header, buf);
        Serializers.toBinary(o.payload, buf);
    }

    public NetMessage deserNetMsg(ByteBuf buf) {
        NetHeader header = deserNetHeader(buf);
        KompicsEvent payload = (KompicsEvent) Serializers.fromBinary(buf, NO_HINT);
        return new NetMessage(header, payload);
    }
}