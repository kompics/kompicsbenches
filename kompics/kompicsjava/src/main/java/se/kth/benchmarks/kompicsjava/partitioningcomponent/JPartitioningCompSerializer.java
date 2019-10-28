package se.kth.benchmarks.kompicsjava.partitioningcomponent;

import io.netty.buffer.ByteBuf;
import se.kth.benchmarks.kompics.SerializerHelper;
import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.partitioningcomponent.events.*;
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
    }

    private static final byte INIT_FLAG = 1;
    private static final byte INIT_ACK_FLAG = 2;
    private static final byte RUN_FLAG = 3;
    private static final byte DONE_FLAG = 4;


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
            default: {
                throw SerializerHelper.notSerializable("Invalid flag: " + flag);
            }
        }
    }
}
