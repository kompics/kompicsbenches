package se.kth.benchmarks.kompicsjava.bench.atomicregister;

import scala.Option;
import se.kth.benchmarks.kompicsjava.partitioningcomponent.events.*;
import se.kth.benchmarks.kompicsjava.bench.atomicregister.events.*;
import se.kth.benchmarks.kompicsjava.broadcast.BEBDeliver;
import se.kth.benchmarks.kompicsjava.broadcast.BEBRequest;
import se.kth.benchmarks.kompicsjava.broadcast.BestEffortBroadcast;
import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;

import se.kth.benchmarks.kompicsscala.KompicsSystemProvider;
import se.kth.benchmarks.test.KVTestUtil;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class AtomicRegister extends ComponentDefinition {

    public static class Init extends se.sics.kompics.Init<AtomicRegister> {
        private final float read_workload;
        private final float write_workload;
        private final boolean testing;

        public Init(float read_workload, float write_workload, boolean testing) {
            this.read_workload = read_workload;
            this.write_workload = write_workload;
            this.testing = testing;
        }
    }

    private Positive<BestEffortBroadcast> beb = requires(BestEffortBroadcast.class);
    private Positive<Network> net = requires(Network.class);

    private int selfRank;
    private final NetAddress selfAddr;
    private List<NetAddress> nodes;
    private int n;
    private long min_key = -1;
    private long max_key = -1;
    private HashMap<Long, AtomicRegisterState> register_state = new HashMap<>();
    private HashMap<Long, HashMap<NetAddress, Tuple>> register_readList = new HashMap<>();

    // benchmark variables
    private long read_count;
    private long write_count;
    private float read_workload;
    private float write_workload;
    private boolean testing;
    private NetAddress master;
    private int current_run_id = -1;

    private LinkedList<KVTestUtil.KVTimestamp> timestamps;

    public AtomicRegister(Init init) {
        selfAddr = config().getValue(KompicsSystemProvider.SELF_ADDR_KEY(), NetAddress.class);
        read_workload = init.read_workload;
        write_workload = init.write_workload;
        testing = init.testing;
        if (testing) timestamps = new LinkedList<>();
        subscribe(startHandler, control);
        subscribe(readRequestHandler, beb);
        subscribe(writeRequestHandler, beb);
        subscribe(readResponseHandler, net);
        subscribe(ackHandler, net);
        subscribe(initHandler, net);
        subscribe(runHandler, net);
    }

    private void newIteration(se.kth.benchmarks.kompicsjava.partitioningcomponent.events.Init i){
        current_run_id = i.id;
        nodes = i.nodes;
        n = nodes.size();
        selfRank = i.rank;
        min_key = i.min;
        max_key = i.max;
        register_state.clear();
        register_readList.clear();
        for (long l = min_key; l <= max_key; l++){
            register_state.put(l, new AtomicRegisterState());
            register_readList.put(l, new HashMap<>());
        }
    }

    private void invokeRead(long key){
        AtomicRegisterState register = register_state.get(key);
        register.rid++;
        register.acks = 0;
        register_readList.get(key).clear();
        register.reading = true;
        if (testing) timestamps.addLast(new KVTestUtil.KVTimestamp(key, KVTestUtil.ReadInvokation$.MODULE$, Option.empty(), System.currentTimeMillis(), selfRank));
        trigger(new BEBRequest(nodes, new Read(current_run_id, key, register.rid)), beb);
    }

    private void invokeWrite(long key){
        int wval = selfRank;
        AtomicRegisterState register = register_state.get(key);
        register.rid++;
        register.writeval = wval;
        register.acks = 0;
        register.reading = false;
        register_readList.get(key).clear();
        if (testing) timestamps.addLast(new KVTestUtil.KVTimestamp(key, KVTestUtil.WriteInvokation$.MODULE$, Option.apply(selfRank), System.currentTimeMillis(), selfRank));
        trigger(new BEBRequest(nodes, new Read(current_run_id, key, register.rid)), beb);
    }

    private void invokeOperations(){
        long num_keys = max_key - min_key + 1;
        long num_reads = (long) (num_keys * read_workload);
        long num_writes = (long) (num_keys * write_workload);
        read_count = num_reads;
        write_count = num_writes;

        if (selfRank % 2 == 0){
            for (long l = 0; l < num_reads; l++) invokeRead(min_key + l);
            for (long l = 0; l < num_writes; l++) invokeWrite(min_key + num_reads + l);
        } else {
            for (long l = 0; l < num_writes; l++) invokeWrite(min_key + l);
            for (long l = 0; l < num_reads; l++) invokeRead(min_key + num_writes + l);
        }
    }

    private void sendDone(){
        if (!testing) trigger(se.kth.benchmarks.kompicsscala.NetMessage.viaTCP(selfAddr.asScala(), master.asScala(), se.kth.benchmarks.kompicsjava.partitioningcomponent.events.Done.event), net);
        else trigger(se.kth.benchmarks.kompicsscala.NetMessage.viaTCP(selfAddr.asScala(), master.asScala(), new se.kth.benchmarks.kompicsjava.partitioningcomponent.events.TestDone(timestamps)), net);
    }

    private void readResponse(long key, int read_value){
        read_count--;
        if (testing) timestamps.addLast(new KVTestUtil.KVTimestamp(key, KVTestUtil.ReadResponse$.MODULE$, Option.apply(read_value), System.currentTimeMillis(), selfRank));
        if (read_count == 0 && write_count == 0) sendDone();
    }

    private void writeResponse(long key){
        write_count--;
        if (testing) timestamps.addLast(new KVTestUtil.KVTimestamp(key, KVTestUtil.WriteResponse$.MODULE$, Option.apply(selfRank), System.currentTimeMillis(), selfRank));
        if (read_count == 0 && write_count == 0) sendDone();
    }

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            assert(selfAddr != null);
        }

    };

    private ClassMatchedHandler<Read, BEBDeliver> readRequestHandler = new ClassMatchedHandler<Read, BEBDeliver>() {
        @Override
        public void handle(Read read, BEBDeliver bebDeliver) {
            if (read.run_id == current_run_id) {
                AtomicRegisterState current_state = register_state.get(read.key);
                trigger(NetMessage.viaTCP(selfAddr, bebDeliver.src, new Value(read.run_id, read.key, read.rid, current_state.ts, current_state.wr, current_state.value)), net);
            }
        }
    };

    private ClassMatchedHandler<Write, BEBDeliver> writeRequestHandler = new ClassMatchedHandler<Write, BEBDeliver>() {
        @Override
        public void handle(Write write, BEBDeliver bebDeliver) {
            if (write.run_id == current_run_id){
                AtomicRegisterState current_state = register_state.get(write.key);
                if (write.ts > current_state.ts || (write.ts == current_state.ts && write.wr > current_state.wr)) {
                    current_state.ts = write.ts;
                    current_state.wr = write.wr;
                    current_state.value = write.value;
                }
            }
            trigger(NetMessage.viaTCP(selfAddr, bebDeliver.src, new Ack(write.run_id, write.key, write.rid)), net);
        }
    };

    private ClassMatchedHandler<se.kth.benchmarks.kompicsjava.partitioningcomponent.events.Init, NetMessage> initHandler = new ClassMatchedHandler<se.kth.benchmarks.kompicsjava.partitioningcomponent.events.Init, NetMessage>() {
        @Override
        public void handle(se.kth.benchmarks.kompicsjava.partitioningcomponent.events.Init init, NetMessage netMessage) {
            newIteration(init);
            master = netMessage.getSource();
            trigger(se.kth.benchmarks.kompicsscala.NetMessage.viaTCP(selfAddr.asScala(), netMessage.getSource().asScala(), new InitAck(init.id)), net);
        }
    };

    private ClassMatchedHandler<Value, NetMessage> readResponseHandler = new ClassMatchedHandler<Value, NetMessage>() {
        @Override
        public void handle(Value v, NetMessage msg) {
            if (v.run_id == current_run_id) {
                AtomicRegisterState current_register = register_state.get(v.key);
                if (v.rid == current_register.rid) {
                    HashMap<NetAddress, Tuple> readList = register_readList.get(v.key);
                    if (current_register.reading) {
                        if (readList.isEmpty()) {
                            current_register.first_received_ts = v.ts;
                            current_register.readval = v.value;
                        } else if (current_register.skip_impose) {
                            if (current_register.first_received_ts != v.ts) current_register.skip_impose = false;
                        }
                    }
                    readList.put(msg.getSource(), new Tuple(v.ts, v.wr, v.value));
                    if (readList.size() > n / 2) {
                        if (current_register.reading && current_register.skip_impose) {
                            current_register.value = current_register.readval;
                            readList.clear();
                            readResponse(v.key, current_register.readval);
                        } else {
                            Tuple maxtuple = getMaxTuple(v.key);
                            int maxts = maxtuple.ts;
                            int rr = maxtuple.wr;
                            current_register.readval = maxtuple.value;
                            readList.clear();
                            int bcastval;
                            if (current_register.reading) {
                                bcastval = current_register.readval;
                            } else {
                                rr = selfRank;
                                maxts++;
                                bcastval = current_register.writeval;
                            }
                            trigger(new BEBRequest(nodes, new Write(v.run_id, v.key, v.rid, maxts, rr, bcastval)), beb);
                        }
                    }
                }
            }
        }
    };

    private ClassMatchedHandler<Ack, NetMessage> ackHandler = new ClassMatchedHandler<Ack, NetMessage>() {
        @Override
        public void handle(Ack a, NetMessage msg) {
            if (a.run_id == current_run_id) {  // avoid redundant acks from previous runs
                AtomicRegisterState current_register = register_state.get(a.key);
                if (a.rid == current_register.rid) {
                    current_register.acks++;
                    if (current_register.acks > n / 2) {
                        current_register.acks = 0;
                        if (current_register.reading) readResponse(a.key, current_register.readval);
                        else writeResponse(a.key);
                    }
                }
            }
        }
    };

    private ClassMatchedHandler<Run, NetMessage> runHandler = new ClassMatchedHandler<Run, NetMessage>() {
        @Override
        public void handle(Run run, NetMessage netMessage) {
            invokeOperations();
        }
    };

    private Tuple getMaxTuple(long key){
        Collection<Tuple> readListValues = register_readList.get(key).values();
        Tuple maxtuple = readListValues.iterator().next();
        for (Tuple t : readListValues){
            if (t.ts > maxtuple.ts){
                maxtuple = t;
            }
        }
        return maxtuple;
    }

    private class Tuple{
        int ts;     // seq nr
        int wr;     // rank
        int value;

        Tuple(int ts, int wr, int value){
            this.ts = ts;
            this.wr = wr;
            this.value = value;
        }
    }

    private class AtomicRegisterState{
        private int ts, wr;
        private int value;
        private int acks;
        private int rid;
        private Boolean reading;
        private int readval;
        private int writeval;
        private int first_received_ts = -1;
        private Boolean skip_impose = true;
    }

}
