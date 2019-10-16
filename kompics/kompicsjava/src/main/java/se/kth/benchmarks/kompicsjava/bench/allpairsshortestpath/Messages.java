package se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath;

import se.sics.kompics.KompicsEvent;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs.DoubleBlock;
import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs.DoubleGraph;

public class Messages {
    public static class ComputeFW implements KompicsEvent {
        public final DoubleGraph graph;
        public final CountDownLatch latch;

        public ComputeFW(DoubleGraph graph, CountDownLatch latch) {
            this.graph = graph;
            this.latch = latch;
        }
    }

    public static class BlockResult implements KompicsEvent {
        public final DoubleBlock block;

        public BlockResult(DoubleBlock block) {
            this.block = block;
        }
    }

    public static class PhaseResult implements KompicsEvent {
        public final int k;
        public final DoubleBlock data;

        public PhaseResult(int k, DoubleBlock data) {
            this.k = k;
            this.data = data;
        }
    }
}
