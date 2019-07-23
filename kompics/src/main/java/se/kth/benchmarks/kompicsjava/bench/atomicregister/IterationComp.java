package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;
import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.*;
import se.kth.benchmarks.kompicsscala.KompicsSystemProvider;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class IterationComp  extends ComponentDefinition {
    public static class Init extends se.sics.kompics.Init<IterationComp> {
        private final CountDownLatch prepare_latch;
        private final CountDownLatch finished_latch;
        private final int init_id;
        private final List<NetAddress> nodes;
        private final long num_keys;
        private final int partition_size;

        public Init(CountDownLatch prepare_latch, CountDownLatch finished_latch, int init_id, List<NetAddress> nodes, long num_keys, int partition_size) {
            this.prepare_latch = prepare_latch;
            this.finished_latch = finished_latch;
            this.init_id = init_id;
            this.nodes = nodes;
            this.num_keys = num_keys;
            this.partition_size = partition_size;
        }
    }

    private final int init_id;
    private Positive<Network> net = requires(Network.class);
    private final int n;
    private int init_ack_count;
    private int done_count;
    private HashMap<Long, List<NetAddress>> partitions;
    private final NetAddress selfAddr;
    private final long num_keys;
    private final int partition_size;
    private final List<NetAddress> nodes;
    private final CountDownLatch prepare_latch;
    private final CountDownLatch finished_latch;

    public IterationComp(Init init) {
        init_id = init.init_id;
        nodes = init.nodes;
        n = init.nodes.size();
        init_ack_count = n;
        done_count = n;
        selfAddr = config().getValue(KompicsSystemProvider.SELF_ADDR_KEY(), NetAddress.class);
        num_keys = init.num_keys;
        partition_size = init.partition_size;
        prepare_latch = init.prepare_latch;
        finished_latch = init.finished_latch;

        subscribe(startHandler, control);
        subscribe(runHandler, control);
        subscribe(initAckHandler, net);
        subscribe(doneHandler, net);
    }

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start start) {
            assert(selfAddr != null);
            long num_partitions = n / partition_size;
            long offset = num_keys / num_partitions;
            int rank = 0;
            int j = 0;
            partitions = new HashMap<>();
            logger.info("Start partitioning: #keys=" + num_keys + ", #partitions=" + num_partitions + ", #offset=" + offset);
            for (long i= 0; i < num_partitions - 1; i++){
                long min = i * offset;
                long max = i * offset;
                List<NetAddress> partition = nodes.subList(j, j + partition_size);
                for (NetAddress node : partition){
                    trigger(NetMessage.viaTCP(selfAddr, node, new INIT(rank, init_id, partition, min, max)), net);
                    rank++;
                }
                partitions.put(i, partition);
                j += partition_size;
            }
            // rest of the keys in the last partition
            long last_min = (num_partitions - 1) * offset;
            long last_max = num_keys - 1;
            logger.info("Last partition, keys: " + last_min + " - " + last_max);
            List<NetAddress> partition = nodes.subList(j, j + partition_size);
            for (NetAddress node : partition) {
                trigger(NetMessage.viaTCP(selfAddr, node, new INIT(rank, init_id, partition, last_min, last_max)), net);
                rank++;
            }
            partitions.put(num_partitions - 1, partition);


        }
    };

    private Handler<RUN> runHandler = new Handler<RUN>() {
        @Override
        public void handle(RUN run) {
            logger.info("Sending RUN to everybody...");
            for (NetAddress node : nodes) trigger(NetMessage.viaTCP(selfAddr, node, RUN.event), net);
        }
    };

    private ClassMatchedHandler<INIT_ACK, NetMessage> initAckHandler = new ClassMatchedHandler<INIT_ACK, NetMessage>() {
        @Override
        public void handle(INIT_ACK init_ack, NetMessage netMessage) {
            init_ack_count--;
            if (init_ack_count == 0){
                logger.info("Got INIT_ACK from everybody");
                prepare_latch.countDown();
            }
        }
    };

    private ClassMatchedHandler<DONE, NetMessage> doneHandler = new ClassMatchedHandler<DONE, NetMessage>() {
        @Override
        public void handle(DONE done, NetMessage netMessage) {
            done_count--;
            if (done_count == 0){
                logger.info("Everybody is done");
                finished_latch.countDown();
            }
        }
    };
}
