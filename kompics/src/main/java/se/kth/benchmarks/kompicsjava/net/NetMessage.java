package se.kth.benchmarks.kompicsjava.net;

import java.util.Objects;

import se.sics.kompics.KompicsEvent;
import se.sics.kompics.PatternExtractor;

import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Transport;

public class NetMessage implements Msg<NetAddress, NetHeader>, PatternExtractor<Class<Object>, KompicsEvent> {

    protected final NetHeader header;
    protected final KompicsEvent payload;

    public NetMessage(NetHeader header, KompicsEvent payload) {
        this.header = header;
        this.payload = payload;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Object> extractPattern() {
        Class<?> c = payload.getClass();
        return (Class<Object>) c;
    }

    @Override
    public KompicsEvent extractValue() {
        return payload;
    }

    @Override
    public NetHeader getHeader() {
        return header;
    }

    @Override
    public NetAddress getSource() {
        return header.getSource();
    }

    @Override
    public NetAddress getDestination() {
        return header.getDestination();
    }

    @Override
    public Transport getProtocol() {
        return header.getProtocol();
    }

    @Override
    public int hashCode() {
        return Objects.hash(header, payload);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof NetMessage)) {
            return false;
        }
        NetMessage other = (NetMessage) obj;
        return Objects.equals(header, other.header) && Objects.equals(payload, other.payload);
    }

    public static NetMessage viaTCP(NetAddress src, NetAddress dst, KompicsEvent payload) {
        return new NetMessage(new NetHeader(src, dst, Transport.TCP), payload);
    }

    public NetMessage reply(NetAddress rsrc, KompicsEvent payload) {
        return new NetMessage(new NetHeader(rsrc, header.getSource(), Transport.TCP), payload);
    }
}