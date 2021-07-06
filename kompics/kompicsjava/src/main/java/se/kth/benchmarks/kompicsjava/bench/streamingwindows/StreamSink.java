
package se.kth.benchmarks.kompicsjava.bench.streamingwindows;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Start;
import se.kth.benchmarks.kompics.ConfigKeys;
import se.kth.benchmarks.kompicsjava.bench.streamingwindows.WindowerMessage;
import java.util.concurrent.CountDownLatch;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.kth.benchmarks.kompicsjava.net.NetMessage;

public class StreamSink extends ComponentDefinition {
    private final Positive<Network> net = requires(Network.class);

    final int partitionId;
    final CountDownLatch latch;
    final long numberOfWindows;
    final NetAddress upstream;
    final NetAddress selfAddr;

    private long windowCount = 0l;

    public StreamSink(Init init) {
        this.partitionId = init.partitionId;
        this.latch = init.latch;
        this.numberOfWindows = init.numberOfWindows;
        this.upstream = init.upstream;
        this.selfAddr = config().getValue(ConfigKeys.SELF_ADDR_KEY, NetAddress.class);

        // subscriptions
        subscribe(startHandler, control);
        subscribe(windowHandler, net);
    }

    private Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            logger.trace("Got Start!");
            sendUpstream(new WindowerMessage.Start(partitionId));
        }
    };

    private ClassMatchedHandler<WindowerMessage.WindowAggregate, NetMessage> windowHandler = new ClassMatchedHandler<WindowerMessage.WindowAggregate, NetMessage>() {

        @Override
        public void handle(WindowerMessage.WindowAggregate content, NetMessage context) {
            if (content.partitionId == partitionId) {
	            logger.debug("Got window with median={}", content.value);
	            windowCount++;
	            if (windowCount == numberOfWindows) {
	                latch.countDown();
	                sendUpstream(new WindowerMessage.Stop(partitionId));
	                logger.debug("Done!");
	            } else {
	                logger.debug("Got {}/{} windows.", windowCount, numberOfWindows);
	            }
        	}
        }
    };

    private void sendUpstream(KompicsEvent event) {
        NetMessage mgs = NetMessage.viaTCP(selfAddr, upstream, event);
        trigger(mgs, net);
    }

    public static class Init extends se.sics.kompics.Init<StreamSink> {
        public final int partitionId;
        public final CountDownLatch latch;
        public final long numberOfWindows;
        public final NetAddress upstream;

        public Init(int partitionId, CountDownLatch latch, long numberOfWindows, NetAddress upstream) {
            this.partitionId = partitionId;
            this.latch = latch;
            this.numberOfWindows = numberOfWindows;
            this.upstream = upstream;
        }
    }
}
