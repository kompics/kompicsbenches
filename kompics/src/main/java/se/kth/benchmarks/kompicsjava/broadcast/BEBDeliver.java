package se.kth.benchmarks.kompicsjava.broadcast;

import se.kth.benchmarks.kompicsjava.net.NetAddress;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PatternExtractor;

public class BEBDeliver implements KompicsEvent, PatternExtractor<Class<Object>, KompicsEvent> {
    public final KompicsEvent payload;
    public final NetAddress src;

    public BEBDeliver(KompicsEvent payload, NetAddress src){
        this.payload = payload;
        this.src = src;
    }

    public KompicsEvent extractValue(){
        return this.payload;
    }

    
    @SuppressWarnings("unchecked")
    @Override
    public Class<Object> extractPattern() {
        Class<?> c = payload.getClass();
        return (Class<Object>) c;
    }

}
