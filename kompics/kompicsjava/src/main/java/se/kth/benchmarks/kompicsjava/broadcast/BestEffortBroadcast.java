package se.kth.benchmarks.kompicsjava.broadcast;

import se.sics.kompics.PortType;

public class BestEffortBroadcast extends PortType {
    {
        request(BEBRequest.class);
        indication(BEBDeliver.class);
    }
}
