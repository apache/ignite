package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

/**
 * Created by vozerov on 10/17/2016.
 */
public class MessageProblemReproducer {
    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Ignite node1 = Ignition.start(config("1"));
        Ignite node2 = Ignition.start(config("2"));

        node1.message().localListen("topic", new IgniteBiPredicate<UUID, byte[]>() {
            @Override public boolean apply(UUID uuid, byte[] o) {
                System.out.println("Received: " + Arrays.toString(o));

                return true;
            }
        });

        node2.message().send("topic", new byte[70000]);

        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * Create configuration.
     *
     * @param name Name.
     * @return Configuration.
     */
    private static IgniteConfiguration config(String name) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(name);
        cfg.setLocalHost("127.0.0.1");

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi().setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }
}
