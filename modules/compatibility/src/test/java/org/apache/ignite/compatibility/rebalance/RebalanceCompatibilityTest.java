package org.apache.ignite.compatibility.rebalance;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Assert;

/**
 * An simple test to check compatibility during rebalance process.
 */
public class RebalanceCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /**
     *
     * @throws Exception
     */
    public void testRebalanceCompatibility() throws Exception {
        doTestNewSupplierOldDemander("2.3.0");
        doTestNewDemanderOldSupplier("2.3.0");
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setCacheConfiguration(
                new CacheConfiguration(CACHE_NAME)
                        .setAffinity(new RendezvousAffinityFunction(false, 32))

        );

        return cfg;
    }

    private void doTestNewSupplierOldDemander(String ver) throws Exception {
        try {
            IgniteEx grid = startGrid(0);

            // Populate cache with data.
            final int entitiesCount = 100_000;
            try (IgniteDataStreamer streamer = grid.dataStreamer(CACHE_NAME)) {
                streamer.allowOverwrite(true);

                for (int k = 0; k < entitiesCount; k++) {
                    streamer.addData(k, k);
                }
            }

            IgniteEx oldGrid = startGrid(1, ver, (cfg) -> {
                cfg.setLocalHost("127.0.0.1");

                cfg.setPeerClassLoadingEnabled(false);

                TcpDiscoverySpi disco = new TcpDiscoverySpi();
                disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

                cfg.setDiscoverySpi(disco);

                cfg.setCacheConfiguration(
                        new CacheConfiguration(CACHE_NAME)
                                .setAffinity(new RendezvousAffinityFunction(false, 32))

                );
            });

            grid.cache(CACHE_NAME).rebalance().get();

            // Check no data loss.
            for (int k = 0; k < entitiesCount; k++) {
                Assert.assertEquals("Check failed for key " + k, k, oldGrid.cache(CACHE_NAME).get(k));
            }
        } finally {
            stopAllGrids();
        }
    }

    private void doTestNewDemanderOldSupplier(String ver) throws Exception {
        try {
            final int entitiesCount = 100_000;

            IgniteEx oldGrid = startGrid(1, ver, (cfg) -> {
                cfg.setLocalHost("127.0.0.1");

                TcpDiscoverySpi disco = new TcpDiscoverySpi();
                disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

                cfg.setDiscoverySpi(disco);

                cfg.setCacheConfiguration(
                        new CacheConfiguration(CACHE_NAME)
                                .setAffinity(new RendezvousAffinityFunction(false, 32))

                );
            }, (ignite) -> {
                // Populate cache with data.
                try (IgniteDataStreamer streamer = ignite.dataStreamer(CACHE_NAME)) {
                    streamer.allowOverwrite(true);

                    for (int k = 0; k < entitiesCount; k++) {
                        streamer.addData(k, k);
                    }
                }
            });

            Thread.sleep(5000);

            IgniteEx grid = startGrid(0);

            grid.cache(CACHE_NAME).rebalance().get();

            // Check no data loss.
            for (int k = 0; k < entitiesCount; k++) {
                Assert.assertEquals("Check failed for key " + k, k, oldGrid.cache(CACHE_NAME).get(k));
            }
        } finally {
            stopAllGrids();
        }
    }
}
