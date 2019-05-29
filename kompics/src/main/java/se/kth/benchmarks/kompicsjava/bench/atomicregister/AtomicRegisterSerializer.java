package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import io.netty.buffer.ByteBuf;
import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.READ;
import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.VALUE;
import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.WRITE;
import se.sics.benchmarks.kompics.SerializerHelper;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

import java.util.Optional;

public class AtomicRegisterSerializer implements Serializer {

    public static void register() {
        Serializers.register(new AtomicRegisterSerializer(), "atomicregister");
        Serializers.register(READ.class, "atomicregister");
        Serializers.register(WRITE.class, "atomicregister");
        Serializers.register(se.kth.benchmarks.kompicsjava.bench.atomicregister.events.ACK.class, "atomicregister");
        Serializers.register(VALUE.class, "atomicregister");
    }

    public static final Optional<Object> NO_HINT = Optional.empty();

    private static final byte READ_FLAG = 1;
    private static final byte WRITE_FLAG = 2;
    private static final byte ACK_FLAG = 3;
    private static final byte VALUE_FLAG = 4;

    @Override
    public int identifier() {
        return se.sics.benchmarks.kompics.SerializerIds.J_NETPP;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof READ){
            READ read = (READ) o;
            buf.writeByte(READ_FLAG);
            buf.writeInt(read.rid);
        }
        else if (o instanceof WRITE){
            WRITE write = (WRITE) o;
            buf.writeByte(WRITE_FLAG);
            buf.writeInt(write.rid);
            buf.writeInt(write.ts);
            buf.writeInt(write.wr);
            buf.writeInt(write.value);
        }
        else if (o instanceof se.kth.benchmarks.kompicsjava.bench.atomicregister.events.ACK){
            se.kth.benchmarks.kompicsjava.bench.atomicregister.events.ACK ack = (se.kth.benchmarks.kompicsjava.bench.atomicregister.events.ACK) o;
            buf.writeByte(ACK_FLAG);
            buf.writeInt(ack.rid);
        }
        else if (o instanceof VALUE){
            VALUE value = (VALUE) o;
            buf.writeByte(VALUE_FLAG);
            buf.writeInt(value.rid);
            buf.writeInt(value.ts);
            buf.writeInt(value.wr);
            buf.writeInt(value.value);
        } else {
            throw SerializerHelper.notSerializable(o.getClass().getName());
        }

    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> optional) {
        byte flag = buf.readByte();
        switch (flag){
            case READ_FLAG: {
                int rid = buf.readInt();
                return new READ(rid);
            }
            case WRITE_FLAG: {
                int rid = buf.readInt();
                int ts = buf.readInt();
                int wr = buf.readInt();
                int value = buf.readInt();
                return new WRITE(rid, ts, wr, value);
            }
            case ACK_FLAG: {
                int rid = buf.readInt();
                return new se.kth.benchmarks.kompicsjava.bench.atomicregister.events.ACK(rid);
            }
            case VALUE_FLAG: {
                int rid = buf.readInt();
                int ts = buf.readInt();
                int wr = buf.readInt();
                int value = buf.readInt();
                return new VALUE(rid, ts, wr, value);
            }
            default: {
                throw SerializerHelper.notSerializable("Invalid flag: " + flag);
            }
        }
    }
}
