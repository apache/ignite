package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class CacheNoAffinityExchangeTest extends GridCommonAbstractTest {

    private volatile boolean startClient;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (startClient)
            cfg.setClientMode(true);

        cfg.setDiscoverySpi(new TestDiscoverySpi());

        return cfg;
    }

    public void testNoAffinityChangeOnClientJoin() throws Exception {
        Ignite ig = startGrids(4);

        ig.cluster().active(true);

        IgniteCache<Integer, Integer> atomicCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
                .setName("atomic").setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IgniteCache<Integer, Integer> txCache = ig.createCache(new CacheConfiguration<Integer, Integer>()
                .setName("tx").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        TestDiscoverySpi discoSpi = (TestDiscoverySpi) grid(2).context().discovery().getInjectedDiscoverySpi();

        CountDownLatch latch = new CountDownLatch(1);

        discoSpi.nodeAddFinishLatch = latch;

        startClient = true;

        GridTestUtils.runAsync(() -> {
            try {
                startGrid(4);
            }
            catch (Exception ex) {
                throw new IgniteException(ex);
            }
        });

        U.sleep(5_000);

        assertEquals(new AffinityTopologyVersion(5, 0), grid(0).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(5, 0), grid(1).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(4, 3), grid(2).context().discovery().topologyVersionEx());
        assertEquals(new AffinityTopologyVersion(4, 3), grid(3).context().discovery().topologyVersionEx());

        for (int k = 0; k < 100; k++) {
            atomicCache.put(k, k);
            txCache.put(k, k);
        }

        for (int k = 0; k < 100; k++) {
            assertEquals(Integer.valueOf(k), atomicCache.get(k));
            assertEquals(Integer.valueOf(k), txCache.get(k));
        }

        latch.countDown();
    }

    public static class TestDiscoverySpi extends TcpDiscoverySpi {
        private volatile CountDownLatch nodeAddFinishLatch;

        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddFinishedMessage) {
                CountDownLatch latch0 = nodeAddFinishLatch;

                if (latch0 != null)
                    try {
                        latch0.await();
                    }
                    catch (InterruptedException ex) {
                        throw new IgniteException(ex);
                    }
            }

            super.startMessageProcess(msg);
        }
    }

}
