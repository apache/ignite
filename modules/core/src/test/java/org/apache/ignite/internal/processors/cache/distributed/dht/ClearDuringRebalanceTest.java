package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class ClearDuringRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
            .setCacheMode(PARTITIONED));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearAll() throws Exception {
        final IgniteEx node = startGrid(0);

        for (int i = 0; i < 10; i++) {
            populate(node);

            try {
                startGrid(1).cache(CACHE_NAME).clear();
            }
            finally {
                stopGrid(1);
            }
        }
    }

    /**
     * @param node Ignite node;
     * @throws Exception If failed.
     */
    private void populate(final Ignite node) throws Exception {
        final AtomicInteger id = new AtomicInteger();
        final int tCnt = Runtime.getRuntime().availableProcessors();
        final byte[] data = new byte[1024];

        ThreadLocalRandom.current().nextBytes(data);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try (IgniteDataStreamer<Object, Object> str = node.dataStreamer(CACHE_NAME)) {
                    int idx = id.getAndIncrement();

                    str.autoFlushFrequency(0);

                    for (int i = idx; i < 500_000; i += tCnt) {
                        str.addData(i, data);

                        if (i % (100 * tCnt) == idx)
                            str.flush();
                    }

                    str.flush();
                }
            }
        }, tCnt, "ldr");

        assertEquals(500_000, node.cache(CACHE_NAME).size());
    }
}
