package se.kth.benchmarks.kompicsjava.bench.sizedthroughput;

import java.util.Random;
import se.sics.kompics.KompicsEvent;

public class SizedThroughputMessage implements KompicsEvent {
    byte[] data;
    byte aux;
    int id;

    public SizedThroughputMessage (int size, int id) {
        Random rng = new Random();
        this.data = new byte[size];
        rng.nextBytes(this.data);
        this.aux = 1;
        this.id = id;
    };

    public SizedThroughputMessage (int id, byte aux, byte[] data) {
        this.id = id;
        this.aux = aux;
        this.data = data;
    }
}
