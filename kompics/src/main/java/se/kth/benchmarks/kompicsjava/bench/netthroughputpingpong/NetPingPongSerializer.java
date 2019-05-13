package se.kth.benchmarks.kompicsjava.bench.netthroughputpingpong;

import java.util.Optional;

import io.netty.buffer.ByteBuf;
import se.sics.benchmarks.kompics.SerializerHelper;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

public class NetPingPongSerializer implements Serializer {

    public static final String NAME = "nettppingpong";

    public static void register() {
        Serializers.register(new NetPingPongSerializer(), NAME);
        Serializers.register(Ping.class, NAME);
        Serializers.register(Pong.class, NAME);
        Serializers.register(StaticPing.class, NAME);
        Serializers.register(StaticPong.class, NAME);
    }

    public static final Optional<Object> NO_HINT = Optional.empty();

    private static final byte STATIC_PING_FLAG = 1;
    private static final byte STATIC_PONG_FLAG = 2;
    private static final byte PING_FLAG = 3;
    private static final byte PONG_FLAG = 4;

    public int identifier() {
        return se.sics.benchmarks.kompics.SerializerIds.J_NETTPPP;
    }

    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof StaticPing) {
            StaticPing p = (StaticPing) o;
            buf.writeByte(STATIC_PING_FLAG);
            buf.writeInt(p.id);
        } else if (o instanceof StaticPong) {
            StaticPong p = (StaticPong) o;
            buf.writeByte(STATIC_PONG_FLAG);
            buf.writeInt(p.id);
        } else if (o instanceof Ping) {
            Ping p = (Ping) o;
            buf.writeByte(PING_FLAG);
            buf.writeLong(p.index);
            buf.writeInt(p.id);
        } else if (o instanceof Pong) {
            Pong p = (Pong) o;
            buf.writeByte(PONG_FLAG);
            buf.writeLong(p.index);
            buf.writeInt(p.id);
        } else {
            throw SerializerHelper.notSerializable(o.getClass().getName());
        }
    }

    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte flag = buf.readByte();
        switch (flag) {
        case STATIC_PING_FLAG:
            int id = buf.readInt();
            return StaticPing.event(id);
        case STATIC_PONG_FLAG:
            int id2 = buf.readInt();
            return StaticPong.event(id2);
        case PING_FLAG:
            long index = buf.readLong();
            int id3 = buf.readInt();
            return new Ping(index, id3);
        case PONG_FLAG:
            long index2 = buf.readLong();
            int id4 = buf.readInt();
            return new Pong(index2, id4);
        default:
            throw SerializerHelper.notSerializable("Invalid flag: " + flag);
        }
    }
}
