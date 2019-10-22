package se.kth.benchmarks.kompicsjava.bench.streamingwindows;

import java.util.Optional;

import io.netty.buffer.ByteBuf;
import se.kth.benchmarks.kompics.SerializerHelper;
import se.kth.benchmarks.kompicsjava.bench.streamingwindows.WindowerMessage.*;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

public class StreamingWindowsSerializer implements Serializer {

    public static final String NAME = "streamingwindows";

    private static final byte START_FLAG = 1;
    private static final byte STOP_FLAG = 2;
    private static final byte EVENT_FLAG = 3;
    private static final byte READY_FLAG = 4;
    private static final byte WINDOW_FLAG = 5;
    private static final byte FLUSH_FLAG = 6;
    private static final byte FLUSHED_FLAG = 7;

    public static void register() {
        Serializers.register(new StreamingWindowsSerializer(), NAME);
        Serializers.register(WindowerMessage.Start.class, NAME);
        Serializers.register(WindowerMessage.Stop.class, NAME);
        Serializers.register(WindowerMessage.Event.class, NAME);
        Serializers.register(WindowerMessage.Ready.class, NAME);
        Serializers.register(WindowerMessage.WindowAggregate.class, NAME);
        Serializers.register(WindowerMessage.Flush.class, NAME);
        Serializers.register(WindowerMessage.Flushed.class, NAME);
    }

    @Override
    public int identifier() {
        return se.kth.benchmarks.kompics.SerializerIds.J_STREAMINGWINDOWS;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {

        if (o instanceof Start) {
            Start s = (Start) o;
            buf.writeByte(START_FLAG);
            buf.writeInt(s.partitionId);
        } else if (o instanceof Stop) {
            Stop s = (Stop) o;
            buf.writeByte(STOP_FLAG);
            buf.writeInt(s.partitionId);
        } else if (o instanceof Event) {
            Event e = (Event) o;
            buf.writeByte(EVENT_FLAG);
            buf.writeLong(e.ts);
            buf.writeInt(e.partitionId);
            buf.writeLong(e.value);
        } else if (o instanceof Ready) {
            Ready r = (Ready) o;
            buf.writeByte(READY_FLAG);
            buf.writeInt(r.partitionId);
            buf.writeLong(r.numMessages);
        } else if (o instanceof WindowAggregate) {
            WindowAggregate w = (WindowAggregate) o;
            buf.writeByte(WINDOW_FLAG);
            buf.writeLong(w.ts);
            buf.writeInt(w.partitionId);
            buf.writeDouble(w.value);
        } else if (o instanceof Flush) {
            Flush f = (Flush) o;
            buf.writeByte(FLUSH_FLAG);
            buf.writeInt(f.partitionId);
        } else if (o instanceof Flushed) {
            Flushed f = (Flushed) o;
            buf.writeByte(FLUSHED_FLAG);
            buf.writeInt(f.partitionId);
        } else {
            throw SerializerHelper.notSerializable(o.getClass().getName());
        }
    }

    @Override

    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte flag = buf.readByte();

        switch (flag) {
        case START_FLAG:
            int pid = buf.readInt();
            return new Start(pid);
        case STOP_FLAG:
            int pid1 = buf.readInt();
            return new Stop(pid1);
        case EVENT_FLAG:
            long ts = buf.readLong();
            int pid2 = buf.readInt();
            long value = buf.readLong();
            return new Event(ts, pid2, value);
        case READY_FLAG:
            int pid3 = buf.readInt();
            long quota = buf.readLong();
            return new Ready(pid3, quota);
        case WINDOW_FLAG:
            long ts1 = buf.readLong();
            int pid4 = buf.readInt();
            double value1 = buf.readDouble();
            return new WindowAggregate(ts1, pid4, value1);
        case FLUSH_FLAG:
            int pid5 = buf.readInt();
            return new Flush(pid5);
        case FLUSHED_FLAG:
            int pid6 = buf.readInt();
            return new Flushed(pid6);
        default:
            throw SerializerHelper.notSerializable("Invalid flag: " + flag);
        }
    }
}
