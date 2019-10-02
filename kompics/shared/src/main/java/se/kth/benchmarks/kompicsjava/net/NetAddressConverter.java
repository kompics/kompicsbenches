package se.kth.benchmarks.kompicsjava.net;

import se.sics.kompics.config.Converter;

public class NetAddressConverter implements Converter<NetAddress> {

    @Override
    public NetAddress convert(Object o) {
        if (o instanceof se.kth.benchmarks.kompicsscala.NetAddress) {
            se.kth.benchmarks.kompicsscala.NetAddress addr = (se.kth.benchmarks.kompicsscala.NetAddress) o;
            return addr.asJava();
        }
        return null;
    }

    @Override
    public Class<NetAddress> type() {
        return NetAddress.class;
    }

}
