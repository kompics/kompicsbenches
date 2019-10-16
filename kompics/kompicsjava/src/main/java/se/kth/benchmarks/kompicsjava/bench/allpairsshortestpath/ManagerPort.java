package se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath;

import se.sics.kompics.PortType;

public class ManagerPort extends PortType {
    {
        request(Messages.ComputeFW.class);
        request(Messages.BlockResult.class);
    }
}
