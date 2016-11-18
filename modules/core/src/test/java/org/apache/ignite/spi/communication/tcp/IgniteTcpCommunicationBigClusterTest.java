package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Testing {@link TcpCommunicationSpi} under big cluster conditions (long DiscoverySpi delivery)
 *
 * @author Alexandr Kuramshin <ein.nsk.ru@gmail.com>
 */
public class IgniteTcpCommunicationBigClusterTest extends GridCommonAbstractTest {

    public static final int IGNITE_NODES_NUMBER = 5;

    public static final long NODE_ADDED_MESSAGE_DELAY = 1_000L;

    public static final long BROADCAST_PERIOD = 100L;

    /** */
    private static IgniteConfiguration config(String gridName) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setGridName(gridName);
        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discovery = new SlowTcpDiscoverySpi();
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47510"));
        discovery.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discovery);

        /*CacheConfiguration cacheCache = new CacheConfiguration();
        cacheCache.setName("cache");
        cacheCache.setCacheMode(CacheMode.PARTITIONED);
        cacheCache.setBackups(0);
        cacheCache.setAtomicityMode(CacheAtomicityMode.ATOMIC);*/

        /** ONHEAP_TIERED
         cacheCache.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);
         cacheCache.setOffHeapMaxMemory(0); */

        /** OFFHEAP_TIERED
         cacheCache.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
         cacheCache.setOffHeapMaxMemory(512L << 20); */

        // cfg.setCacheConfiguration(cacheCache);
        return cfg;
    }

    public void testBigCluster() throws Exception {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < IGNITE_NODES_NUMBER; ++i) {
            final int nodeIndex = i;
            executorService.execute(() -> {
                startNode("testBigClusterNode-" + nodeIndex);
            });
        }
    }

    private void startNode(String name) {
        try (final Ignite ignite = Ignition.start(config(name))) {
            try {
                for (; ; ) {
                    Thread.sleep(BROADCAST_PERIOD);
                    ignite.compute().broadcast(() -> {
                        // no-op
                    });
                }
            }
            catch (Throwable ex) {
                System.err.printf("Node thread exit on error: node = %s%d", name);
                ex.printStackTrace();
            }
        }
    }

    private static class SlowTcpDiscoverySpi extends TcpDiscoverySpi {
        @Override protected boolean ensured(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                try {
                    Thread.sleep(NODE_ADDED_MESSAGE_DELAY);
                }
                catch (InterruptedException ex) {
                    System.err.println("Long delivery of TcpDiscoveryNodeAddFinishedMessage interrupted");
                    ex.printStackTrace();
                }
            }
            return super.ensured(msg);
        }
    }
}
