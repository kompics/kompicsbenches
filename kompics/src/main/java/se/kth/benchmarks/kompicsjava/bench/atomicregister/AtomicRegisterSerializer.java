package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import io.netty.buffer.ByteBuf;
import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.*;
import se.kth.benchmarks.kompics.SerializerHelper;
import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class AtomicRegisterSerializer implements Serializer {

    public static void register() {
        Serializers.register(new AtomicRegisterSerializer(), "atomicregister");
        Serializers.register(INIT.class, "atomicregister");
        Serializers.register(READ.class, "atomicregister");
        Serializers.register(WRITE.class, "atomicregister");
        Serializers.register(ACK.class, "atomicregister");
        Serializers.register(VALUE.class, "atomicregister");
        Serializers.register(DONE.class, "atomicregister");
    }

    public static final Optional<Object> NO_HINT = Optional.empty();

    private static final byte READ_FLAG = 1;
    private static final byte WRITE_FLAG = 2;
    private static final byte ACK_FLAG = 3;
    private static final byte VALUE_FLAG = 4;
    private static final byte DONE_FLAG = 5;

    @Override
    public int identifier() {
        return se.kth.benchmarks.kompics.SerializerIds.J_ATOMIC_REG;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof DONE) buf.writeByte(DONE_FLAG);
        else if (o instanceof READ){
            READ read = (READ) o;
            buf.writeByte(READ_FLAG);
            buf.writeInt(read.run_id);
            buf.writeLong(read.key);
            buf.writeInt(read.rid);
        }
        else if (o instanceof WRITE){
            WRITE write = (WRITE) o;
            buf.writeByte(WRITE_FLAG);
            buf.writeInt(write.run_id);
            buf.writeLong(write.key);
            buf.writeInt(write.rid);
            buf.writeInt(write.ts);
            buf.writeInt(write.wr);
            buf.writeInt(write.value);
        }
        else if (o instanceof ACK){
            ACK ack = (ACK) o;
            buf.writeByte(ACK_FLAG);
            buf.writeInt(ack.run_id);
            buf.writeLong(ack.key);
            buf.writeInt(ack.rid);
        }
        else if (o instanceof VALUE){
            VALUE value = (VALUE) o;
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
            case DONE_FLAG: return DONE.event;
            case READ_FLAG: {
                int run_id = buf.readInt();
                long key = buf.readLong();
                int rid = buf.readInt();
                return new READ(run_id, key, rid);
            }
            case WRITE_FLAG: {
                int run_id = buf.readInt();
                long key = buf.readLong();
                int rid = buf.readInt();
                int ts = buf.readInt();
                int wr = buf.readInt();
                int value = buf.readInt();
                return new WRITE(run_id, key, rid, ts, wr, value);
            }
            case ACK_FLAG: {
                int run_id = buf.readInt();
                long key = buf.readLong();
                int rid = buf.readInt();
                return new ACK(run_id, key, rid);
            }
            case VALUE_FLAG: {
                int run_id = buf.readInt();
                long key = buf.readLong();
                int rid = buf.readInt();
                int ts = buf.readInt();
                int wr = buf.readInt();
                int value = buf.readInt();
                return new VALUE(run_id, key, rid, ts, wr, value);
            }
            default: {
                throw SerializerHelper.notSerializable("Invalid flag: " + flag);
            }
        }
    }
}
