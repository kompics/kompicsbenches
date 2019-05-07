package se.kth.benchmarks.kompicsjava.bench.netpingpong;

import java.util.Optional;

import io.netty.buffer.ByteBuf;
import se.sics.benchmarks.kompics.SerializerHelper;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

public class NetPingPongSerializer implements Serializer {

    public static void register() {
        Serializers.register(new NetPingPongSerializer(), "netpingpong");
        Serializers.register(Ping.class, "netpingpong");
        Serializers.register(Pong.class, "netpingpong");
    }

    public static final Optional<Object> NO_HINT = Optional.empty();

    private static final byte PING_FLAG = 1;
    private static final byte PONG_FLAG = 2;

    public int identifier() {
        return se.sics.benchmarks.kompics.SerializerIds.J_NETPP;
    }

    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof Ping) {
            buf.writeByte(PING_FLAG);
        } else if (o instanceof Pong) {
            buf.writeByte(PONG_FLAG);
        } else {
            throw SerializerHelper.notSerializable(o.getClass().getName());
        }
    }

    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte flag = buf.readByte();
        switch (flag) {
        case PING_FLAG:
            return Ping.event;
        case PONG_FLAG:
            return Pong.event;
        default:
            throw SerializerHelper.notSerializable("Invalid flag: " + flag);
        }
    }
}
