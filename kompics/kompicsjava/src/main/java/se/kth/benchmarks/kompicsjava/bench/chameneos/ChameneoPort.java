package se.kth.benchmarks.kompicsjava.bench.chameneos;

import se.sics.kompics.PortType;

public class ChameneoPort extends PortType {
    {
        request(Messages.Change.class);
    }
}
