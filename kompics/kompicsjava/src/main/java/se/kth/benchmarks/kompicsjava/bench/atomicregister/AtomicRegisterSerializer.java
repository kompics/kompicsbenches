package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import io.netty.buffer.ByteBuf;
import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.*;
import se.kth.benchmarks.kompics.SerializerHelper;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

import java.util.Optional;

public class AtomicRegisterSerializer implements Serializer {

    public static void register() {
        Serializers.register(new AtomicRegisterSerializer(), "atomicregister");
        Serializers.register(Read.class, "atomicregister");
        Serializers.register(Write.class, "atomicregister");
        Serializers.register(Ack.class, "atomicregister");
        Serializers.register(Value.class, "atomicregister");
    }

    public static final Optional<Object> NO_HINT = Optional.empty();

    private static final byte READ_FLAG = 1;
    private static final byte WRITE_FLAG = 2;
    private static final byte ACK_FLAG = 3;
    private static final byte VALUE_FLAG = 4;

    @Override
    public int identifier() {
        return se.kth.benchmarks.kompics.SerializerIds.J_ATOMIC_REG;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof Read){
            Read read = (Read) o;
            buf.writeByte(READ_FLAG);
            buf.writeInt(read.run_id);
            buf.writeLong(read.key);
            buf.writeInt(read.rid);
        }
        else if (o instanceof Write){
            Write write = (Write) o;
            buf.writeByte(WRITE_FLAG);
            buf.writeInt(write.run_id);
            buf.writeLong(write.key);
            buf.writeInt(write.rid);
            buf.writeInt(write.ts);
            buf.writeInt(write.wr);
            buf.writeInt(write.value);
        }
        else if (o instanceof Ack){
            Ack ack = (Ack) o;
            buf.writeByte(ACK_FLAG);
            buf.writeInt(ack.run_id);
            buf.writeLong(ack.key);
            buf.writeInt(ack.rid);
        }
        else if (o instanceof Value){
            Value value = (Value) o;
            buf.writeByte(VALUE_FLAG);
            buf.writeInt(value.run_id);
            buf.writeLong(value.key);
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
                int run_id = buf.readInt();
                long key = buf.readLong();
                int rid = buf.readInt();
                return new Read(run_id, key, rid);
            }
            case WRITE_FLAG: {
                int run_id = buf.readInt();
                long key = buf.readLong();
                int rid = buf.readInt();
                int ts = buf.readInt();
                int wr = buf.readInt();
                int value = buf.readInt();
                return new Write(run_id, key, rid, ts, wr, value);
            }
            case ACK_FLAG: {
                int run_id = buf.readInt();
                long key = buf.readLong();
                int rid = buf.readInt();
                return new Ack(run_id, key, rid);
            }
            case VALUE_FLAG: {
                int run_id = buf.readInt();
                long key = buf.readLong();
                int rid = buf.readInt();
                int ts = buf.readInt();
                int wr = buf.readInt();
                int value = buf.readInt();
                return new Value(run_id, key, rid, ts, wr, value);
            }
            default: {
                throw SerializerHelper.notSerializable("Invalid flag: " + flag);
            }
        }
    }
}
