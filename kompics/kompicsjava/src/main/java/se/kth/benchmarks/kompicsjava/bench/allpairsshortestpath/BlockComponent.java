package se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath;

import se.kth.benchmarks.kompicsjava.bench.fibonacci.FibonacciPort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Fault;
import se.sics.kompics.Positive;
import se.sics.kompics.Negative;
import se.sics.kompics.Kill;
import se.sics.kompics.Start;
import se.sics.kompics.Component;
import se.sics.kompics.Channel;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.TreeMap;
import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.Messages.*;

import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs.DoubleBlock;
import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs.DoubleGraph;
import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs.GraphUtils;

import java.util.UUID;

public class BlockComponent extends ComponentDefinition {

    final Positive<ManagerPort> managerPort = requires(ManagerPort.class);
    final Negative<BlockPort> blockPortProv = provides(BlockPort.class);
    final Positive<BlockPort> blockPortReq = requires(BlockPort.class);

    final int numNodes;
    final int numNeighbours;

    private DoubleBlock currentData;
    private int k = -1;
    private TreeMap<Integer, DoubleBlock> neighbourData = new TreeMap<>();
    private TreeMap<Integer, DoubleBlock> nextNeighbourData = new TreeMap<>();
    private boolean done = false;

    public BlockComponent(Init init) {
        this.numNodes = init.numNodes;
        this.numNeighbours = init.numNeighbours;
        this.currentData = init.initialBlock;
        // subscriptions
        subscribe(startHandler, control);
        subscribe(resultHandler, blockPortReq);
    }

    final Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start _event) {
            logger.debug("Got Start at blockId={}", currentData.blockId);
            notifyNeighbours();
        }
    };

    final Handler<PhaseResult> resultHandler = new Handler<PhaseResult>() {
        @Override
        public void handle(PhaseResult event) {
            if (!done) {
                if (event.k == k) {
                    logger.debug("Got PhaseResult(k={}) at blockId={}", event.k, currentData.blockId);
                    neighbourData.put(event.data.blockId, event.data);
                    while (neighbourData.size() == numNeighbours) {
                        k++;
                        performComputation();
                        notifyNeighbours();
                        neighbourData = nextNeighbourData;
                        nextNeighbourData = new TreeMap<>();

                        if (k == numNodes - 1) {
                            trigger(new BlockResult(currentData), managerPort);
                            done = true;
                            suicide();
                        }
                    }
                } else if (event.k == k + 1) {
                    nextNeighbourData.put(event.data.blockId, event.data);
                } else {
                    throw new RuntimeException(String.format("Got k={} but I'm in phase k={}!", event.k, k));
                }
            }
        }
    };

    private void notifyNeighbours() {
        trigger(new PhaseResult(this.k, this.currentData), blockPortProv);
    }

    private void performComputation() {
        this.currentData = this.currentData.copy(); // this would be unnecessary if I could see the reference count
        this.currentData.computeFloydWarshallInner(this.neighbourData, this.k);
    }

    public static class Init extends se.sics.kompics.Init<BlockComponent> {
        public final DoubleBlock initialBlock;
        public final int numNodes;
        public final int numNeighbours;

        public Init(DoubleBlock initialBlock, int numNodes, int numNeighbours) {
            this.initialBlock = initialBlock;
            this.numNodes = numNodes;
            this.numNeighbours = numNeighbours;
        }
    }

}
