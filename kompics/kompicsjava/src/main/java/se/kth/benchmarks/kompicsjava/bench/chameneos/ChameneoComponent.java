package se.kth.benchmarks.kompicsjava.bench.chameneos;

import se.kth.benchmarks.kompicsjava.bench.fibonacci.FibonacciPort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Fault;
import se.sics.kompics.Positive;
import se.sics.kompics.Negative;
import se.sics.kompics.Kill;
import se.sics.kompics.Start;
import se.sics.kompics.Component;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import java.util.UUID;

import se.kth.benchmarks.kompicsjava.bench.chameneos.ChameneoPort;
import se.kth.benchmarks.kompicsjava.bench.chameneos.ChameneosColour;
import se.kth.benchmarks.kompicsjava.bench.chameneos.Messages.*;

public class ChameneoComponent extends ComponentDefinition {

    final Positive<MallPort> mallPort = requires(MallPort.class);
    final Negative<ChameneoPort> chameneoPortProv = provides(ChameneoPort.class);
    final Positive<ChameneoPort> chameneoPortReq = requires(ChameneoPort.class);

    private ChameneosColour colour;

    final UUID myId = this.id();

    public ChameneoComponent(Init init) {
        this.colour = init.initialColour;
        // subscriptions
        subscribe(startHandler, control);
        subscribe(meetHandler, mallPort);
        subscribe(exitHandler, mallPort);
        subscribe(changeHandler, chameneoPortProv);
    }

    private long meetings = 0l;

    final Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start _event) {
            trigger(new MeetMe(colour, myId), mallPort);
        }
    };

    final Handler<MeetUp> meetHandler = new Handler<MeetUp>() {
        @Override
        public void handle(MeetUp event) {
            if (event.target == myId) {
                colour = colour.complement(event.colour);
                meetings++;
                trigger(new Change(colour, event.chameneo), chameneoPortReq);
                trigger(new MeetMe(colour, myId), mallPort);
            }
        }
    };

    final Handler<Exit> exitHandler = new Handler<Exit>() {
        @Override
        public void handle(Exit _event) {
            colour = ChameneosColour.FADED;
            trigger(new MeetingCount(meetings), mallPort);
            suicide();
        }
    };

    final Handler<Change> changeHandler = new Handler<Change>() {
        @Override
        public void handle(Change event) {
            if (event.target == myId) {
                colour = event.colour;
                meetings++;
                trigger(new MeetMe(colour, myId), mallPort);
            }
        }
    };

    public static class Init extends se.sics.kompics.Init<ChameneoComponent> {
        public final ChameneosColour initialColour;

        public Init(ChameneosColour initialColour) {
            this.initialColour = initialColour;
        }
    }

}
