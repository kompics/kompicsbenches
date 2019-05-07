package se.kth.benchmarks.kompicsjava.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

import se.sics.kompics.network.Address;

public class NetAddress implements Address {

    protected final InetSocketAddress isa;

    public NetAddress(InetSocketAddress isa) {
        this.isa = isa;
    }

    @Override
    public InetAddress getIp() {
        return isa.getAddress();
    }

    @Override
    public int getPort() {
        return isa.getPort();
    }

    @Override
    public InetSocketAddress asSocket() {
        return isa;
    }

    @Override
    public boolean sameHostAs(Address other) {
        return this.isa.equals(other.asSocket());
    }

    @Override
    public int hashCode() {
        return Objects.hash(isa);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof NetAddress)) {
            return false;
        }
        NetAddress other = (NetAddress) obj;
        return Objects.equals(isa, other.isa);
    }

    public static NetAddress from(String ip, int port) {
        return new NetAddress(new InetSocketAddress(ip, port));
    }
}