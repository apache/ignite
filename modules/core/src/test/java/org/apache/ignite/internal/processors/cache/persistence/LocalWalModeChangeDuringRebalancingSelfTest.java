package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class LocalWalModeChangeDuringRebalancingSelfTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(200 * 1024 * 1024)
                        .setInitialSize(200 * 1024 * 1024)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setRebalanceDelay(-1)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalDisabledDuringRebalancing() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1000; k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        ignite.cluster().setBaselineTopology(4);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertTrue("WAL should be disabled until rebalancing is finished", !grpCtx.walEnabled());

        for (int i = 0; i < 4; i++)
            grid(i).cache(DEFAULT_CACHE_NAME).rebalance().get();

        awaitPartitionMapExchange();

        assertTrue("WAL should be enabled after rebalancing is finished", grpCtx.walEnabled());

        for (Integer k = 0; k < 1000; k++)
            assertEquals("k=" + k, k, cache.get(k));
    }
}
