package se.kth.benchmarks.kompics;

public abstract class SerializerHelper {

    public static RuntimeException notSerializable(String msg) {
        Exception e = new java.io.NotSerializableException(msg);
        RuntimeException re = new RuntimeException(e);
        return re;
    }
}
