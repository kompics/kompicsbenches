package se.kth.benchmarks.kompicsjava.bench.fibonacci;

import se.sics.kompics.PortType;

public class FibonacciPort extends PortType {
    {
        request(Messages.FibRequest.class);
        indication(Messages.FibResponse.class);
    }
}
