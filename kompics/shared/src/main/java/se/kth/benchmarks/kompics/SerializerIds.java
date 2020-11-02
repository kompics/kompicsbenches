package se.kth.benchmarks.kompics;

/**
 * Used to keep track of serializer ids to avoid duplicate assignments.
 * 
 */
public abstract class SerializerIds {
    /*
     * Scala
     */
    public static final int S_BENCH_NET = 100;
    public static final int S_NETPP = 102;
    public static final int S_NETTPPP = 104;
    public static final int S_ATOMIC_REG = 106;
    public static final int S_PART_COMP = 108;
    public static final int S_STREAMINGWINDOWS = 110;
    public static final int S_SIZEDTHROUGHPUT = 112;

    /*
     * Java
     */
    public static final int J_BENCH_NET = 101;
    public static final int J_NETPP = 103;
    public static final int J_NETTPPP = 105;
    public static final int J_ATOMIC_REG = 107;
    public static final int J_PART_COMP = 109;
    public static final int J_STREAMINGWINDOWS = 111;
    public static final int J_SIZEDTHROUGHPUT = 113;

}
