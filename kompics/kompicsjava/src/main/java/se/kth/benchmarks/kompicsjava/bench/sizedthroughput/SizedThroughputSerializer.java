package se.kth.benchmarks.kompicsjava.bench.sizedthroughput;

import java.util.Optional;

import io.netty.buffer.ByteBuf;
import se.kth.benchmarks.kompics.SerializerHelper;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

public class SizedThroughputSerializer implements Serializer {

    public static final String NAME = "sizedThroughput";

    private static final byte DATA_FLAG = 1;
    private static final byte ACK_FLAG = 2;

    public static void register() {
        Serializers.register(new SizedThroughputSerializer(), NAME);
        Serializers.register(SizedThroughputMessage.class, NAME);
        Serializers.register(SizedThroughputSink.Ack.class, NAME);
    }

    @Override
    public int identifier() {
        return se.kth.benchmarks.kompics.SerializerIds.J_SIZEDTHROUGHPUT;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        if (o instanceof SizedThroughputMessage) {
            SizedThroughputMessage msg = (SizedThroughputMessage) o;
            buf.writeByte(DATA_FLAG);
            buf.writeInt(msg.id);
            buf.writeByte(msg.aux);
            buf.writeInt(msg.data.length);
            buf.writeBytes(msg.data);
        } else if (o instanceof SizedThroughputSink.Ack) {
            SizedThroughputSink.Ack ack = (SizedThroughputSink.Ack) o;
            buf.writeByte(ACK_FLAG);
            buf.writeInt(ack.id);
        } else {
            throw SerializerHelper.notSerializable(o.getClass().getName());
        }
    }

    @Override

    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte flag = buf.readByte();

        switch (flag) {
        case DATA_FLAG:
            int pid = buf.readInt();
            byte aux = buf.readByte();
            int length = buf.readInt();
            byte[] data = new byte[length];
            buf.readBytes(data);
            return new SizedThroughputMessage(pid, aux, data);
        case ACK_FLAG:
            int pid1 = buf.readInt();
            return new SizedThroughputSink.Ack(pid1);
        default:
            throw SerializerHelper.notSerializable("Invalid flag: " + flag);
        }
    }
}
