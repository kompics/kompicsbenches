package se.kth.benchmarks.kompicsjava.bench.chameneos;

import se.sics.kompics.PortType;

public class MallPort extends PortType {
    {
        request(Messages.MeetMe.class);
        request(Messages.MeetingCount.class);
        indication(Messages.MeetUp.class);
        indication(Messages.Exit.class);
    }
}
