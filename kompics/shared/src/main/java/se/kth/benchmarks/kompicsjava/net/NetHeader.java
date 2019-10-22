package se.kth.benchmarks.kompicsjava.net;

import java.util.Objects;

import se.sics.kompics.network.Header;
import se.sics.kompics.network.Transport;

public class NetHeader implements Header<NetAddress> {

    protected final NetAddress src;
    protected final NetAddress dst;
    protected final Transport proto;

    public NetHeader(NetAddress src, NetAddress dst, Transport proto) {
        this.src = src;
        this.dst = dst;
        this.proto = proto;
    }

    @Override
    public NetAddress getSource() {
        return src;
    }

    @Override
    public NetAddress getDestination() {
        return dst;
    }

    @Override
    public Transport getProtocol() {
        return proto;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dst, proto, src);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof NetHeader)) {
            return false;
        }
        NetHeader other = (NetHeader) obj;
        return Objects.equals(dst, other.dst) && proto == other.proto && Objects.equals(src, other.src);
    }

}