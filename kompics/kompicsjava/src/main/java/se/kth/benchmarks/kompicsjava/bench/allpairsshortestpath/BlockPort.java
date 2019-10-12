package se.kth.benchmarks.kompicsjava.bench.allpairsshortestpath;

import se.sics.kompics.PortType;

public class BlockPort extends PortType {
    {
        indication(Messages.PhaseResult.class);
    }
}
