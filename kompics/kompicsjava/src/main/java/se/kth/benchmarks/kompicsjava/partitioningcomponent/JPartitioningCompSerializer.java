package se.kth.benchmarks.kompicsjava.partitioningcomponent;

import io.netty.buffer.ByteBuf;
import scala.Int;
import scala.None;
import scala.Option;
import se.kth.benchmarks.kompics.SerializerHelper;
import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.partitioningcomponent.events.*;
import se.kth.benchmarks.kompicsjava.partitioningcomponent.events.TestDone;
import se.kth.benchmarks.test.KVTestUtil;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class JPartitioningCompSerializer implements Serializer {

    public static void register() {
        Serializers.register(new JPartitioningCompSerializer(), "partitioningcomp");
        Serializers.register(Init.class, "partitioningcomp");
        Serializers.register(InitAck.class, "partitioningcomp");
        Serializers.register(Run.class, "partitioningcomp");
        Serializers.register(Done.class, "partitioningcomp");
        Serializers.register(TestDone.class, "partitioningcomp");
    }

    private static final byte INIT_FLAG = 1;
    private static final byte INIT_ACK_FLAG = 2;
    private static final byte RUN_FLAG = 3;
    private static final byte DONE_FLAG = 4;
    private static final byte TESTDONE_FLAG = 5;
    private static final byte WRITEINVOKATION_FLAG = 6;
    private static final byte READINVOKATION_FLAG = 7;
    private static final byte WRITERESPONSE_FLAG = 8;
    private static final byte READRESPONSE_FLAG = 9;

    @Override
    public int identifier() {
        return se.kth.benchmarks.kompics.SerializerIds.J_PART_COMP;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof Init){
            Init i = (Init) o;
            buf.writeByte(INIT_FLAG);
            buf.writeInt(i.rank);
            buf.writeInt(i.id);
            buf.writeLong(i.min);
            buf.writeLong(i.max);
            buf.writeInt(i.nodes.size());
            for (NetAddress node : i.nodes){
                buf.writeBytes(node.getIp().getAddress());
                buf.writeShort(node.getPort());
            }
        }
        else if (o instanceof InitAck){
            InitAck ack = (InitAck) o;
            buf.writeByte(INIT_ACK_FLAG);
            buf.writeInt(ack.init_id);
        }
        else if (o instanceof Run){
            buf.writeByte(RUN_FLAG);
        }
        else if (o instanceof Done){
            buf.writeByte(DONE_FLAG);
        }
        else if (o instanceof TestDone){
            TestDone td = (TestDone) o;
            buf.writeByte(TESTDONE_FLAG);
            buf.writeInt(td.timestamps.size());
            for (KVTestUtil.KVTimestamp ts : td.timestamps){
                buf.writeLong(ts.key());
                if (ts.operation() instanceof KVTestUtil.ReadInvokation$){
                    buf.writeByte(READINVOKATION_FLAG);
                }
                else if (ts.operation() instanceof KVTestUtil.ReadResponse$){
                    buf.writeByte(READRESPONSE_FLAG);
                    buf.writeInt((int) ts.value().get());
                }
                else if (ts.operation() instanceof KVTestUtil.WriteInvokation$){
                    buf.writeByte(WRITEINVOKATION_FLAG);
                    buf.writeInt((int) ts.value().get());
                }
                else if (ts.operation() instanceof KVTestUtil.WriteResponse$){
                    buf.writeByte(WRITERESPONSE_FLAG);
                    buf.writeInt((int) ts.value().get());
                }
                buf.writeLong(ts.time());
                buf.writeInt(ts.sender());
            }
        }
        else {
            throw SerializerHelper.notSerializable(o.getClass().getName());
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> optional) {
        byte flag = buf.readByte();
        switch (flag) {
            case INIT_FLAG: {
                int rank = buf.readInt();
                int id = buf.readInt();
                long min = buf.readLong();
                long max = buf.readLong();
                int n = buf.readInt();
                List<NetAddress> nodes = new LinkedList<>();
                for (int i = 0; i < n; i++){
                    byte[] ipBytes = new byte[4];
                    buf.readBytes(ipBytes);
                    try {
                        InetAddress address = InetAddress.getByAddress(ipBytes);
                        int port = buf.readUnsignedShort();
                        nodes.add(new NetAddress(new InetSocketAddress(address, port)));
                    } catch (UnknownHostException e) {
                        throw SerializerHelper.notSerializable("UnknownHostException when trying to create InetAddress from bytes");
                    }
                }
                return new Init(rank, id, nodes, min, max);
            }
            case INIT_ACK_FLAG: {
                return new InitAck(buf.readInt());
            }
            case RUN_FLAG: return Run.event;
            case DONE_FLAG: return Done.event;
            case TESTDONE_FLAG: {
                LinkedList<KVTestUtil.KVTimestamp> results = new LinkedList<>();
                int n = buf.readInt();
                for (int i = 0; i < n; i++){
                    long key = buf.readLong();
                    byte operation = buf.readByte();
                    if (operation == READINVOKATION_FLAG){
                        long time = buf.readLong();
                        int sender = buf.readInt();
                        results.addLast(new KVTestUtil.KVTimestamp(key, KVTestUtil.ReadInvokation$.MODULE$, Option.empty(), time, sender));
                    } else {
                        int value = buf.readInt();
                        Option val = Option.apply(value);
                        long time = buf.readLong();
                        int sender = buf.readInt();
                        if (operation == READRESPONSE_FLAG) results.addLast(new KVTestUtil.KVTimestamp(key, KVTestUtil.ReadResponse$.MODULE$, val, time, sender));
                        else if (operation == WRITEINVOKATION_FLAG) results.addLast(new KVTestUtil.KVTimestamp(key, KVTestUtil.WriteInvokation$.MODULE$, val, time, sender));
                        else results.addLast(new KVTestUtil.KVTimestamp(key, KVTestUtil.WriteResponse$.MODULE$, val, time, sender));
                    }
                }
                return new TestDone(results);
            }
            default: {
                throw SerializerHelper.notSerializable("Invalid flag: " + flag);
            }
        }
    }
}
