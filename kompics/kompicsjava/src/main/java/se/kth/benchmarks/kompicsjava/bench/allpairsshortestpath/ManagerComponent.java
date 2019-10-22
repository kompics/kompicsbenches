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
import java.util.ArrayList;
import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.Messages.*;

import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs.DoubleBlock;
import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs.DoubleGraph;
import se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath.graphs.GraphUtils;

import java.util.UUID;

public class ManagerComponent extends ComponentDefinition {
    final Negative<ManagerPort> managerPort = provides(ManagerPort.class);

    final int blockSize;

    public ManagerComponent(Init init) {
        this.blockSize = init.blockSize;
        // subscriptions
        subscribe(computeHandler, managerPort);
        subscribe(resultHandler, managerPort);
    }

    private final ArrayList<Component> blockWorkers = new ArrayList<>();
    private Optional<CountDownLatch> latch = Optional.empty();
    private Optional<DoubleBlock[][]> assembly = Optional.empty();
    private int missingBlocks = -1;

    // test only!
    // private Optional<DoubleGraph> result = Optional.empty();

    final Handler<ComputeFW> computeHandler = new Handler<ComputeFW>() {
        @Override
        public void handle(ComputeFW event) {
            logger.debug("Got ComputeFW!");
            distributeGraph(event.graph);
            latch = Optional.of(event.latch);
            // test only!
            // DoubleGraph myGraph = event.graph.copy();
            // myGraph.computeFloydWarshall();
            // result = Optional.of(myGraph);
        }
    };

    final Handler<BlockResult> resultHandler = new Handler<BlockResult>() {
        @Override
        public void handle(BlockResult event) {
            DoubleBlock block = event.block;
            DoubleBlock[][] blocks = assembly.get(); // better not be empty!
            int i = block.blockPositionI();
            int j = block.blockPositionJ();
            blocks[i][j] = block;
            missingBlocks--;
            if (missingBlocks == 0) {
                DoubleGraph resultGraph = DoubleGraph.assembleFromBlocks(blocks);
                latch.get().countDown();
                logger.info("Done!");
                // cleanup
                blockWorkers.clear();
                latch = Optional.empty();
                assembly = Optional.empty();
                missingBlocks = -1;
                // test only!
                // assert result.get().equals(resultGraph) : "Wrong APSP result!";
            } else {
                logger.debug("Got another {} block outstanding.", missingBlocks);
            }
        }
    };

    private void distributeGraph(DoubleGraph graph) {
        DoubleBlock[][] blocks = graph.breakIntoBlocks(blockSize);
        int numNodes = graph.numNodes;
        Positive<ManagerPort> outerPort = this.managerPort.getPair();
        int neighbourCount = blocks.length * 2 - 2; // exclude myself
        Component[][] blockComponents = new Component[blocks.length][blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            for (int j = 0; j < blocks.length; j++) {
                blockComponents[i][j] = create(BlockComponent.class,
                        new BlockComponent.Init(blocks[i][j], numNodes, neighbourCount));
                this.blockWorkers.add(blockComponents[i][j]);
            }
        }
        for (int i = 0; i < blocks.length; i++) {
            for (int j = 0; j < blocks.length; j++) {
                Component current = blockComponents[i][j];
                connect(outerPort, current.getNegative(ManagerPort.class), Channel.TWO_WAY);
                // add neighbours in the same row
                for (int r = 0; r < blocks.length; r++) {
                    if (r != i) {
                        Component neighbour = blockComponents[r][j];
                        connect(neighbour.getPositive(BlockPort.class), current.getNegative(BlockPort.class),
                                Channel.TWO_WAY);
                    }
                }
                // add neighbours in the same column
                for (int c = 0; c < blocks.length; c++) {
                    if (c != j) {
                        Component neighbour = blockComponents[i][c];
                        connect(neighbour.getPositive(BlockPort.class), current.getNegative(BlockPort.class),
                                Channel.TWO_WAY);
                    }
                }
            }
        }
        for (Component c : this.blockWorkers) {
            trigger(Start.event, c.getControl());
        }
        this.missingBlocks = this.blockWorkers.size();
        this.assembly = Optional.of(blocks);
    }

    public static class Init extends se.sics.kompics.Init<ManagerComponent> {
        public final int blockSize;

        public Init(int blockSize) {
            this.blockSize = blockSize;
        }
    }
}
