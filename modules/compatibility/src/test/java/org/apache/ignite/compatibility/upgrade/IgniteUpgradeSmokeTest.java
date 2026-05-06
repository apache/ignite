package org.apache.ignite.compatibility.upgrade;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Smoke test for rolling upgrade with persistence.
 */
public class IgniteUpgradeSmokeTest extends IgniteUpgradeAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "transactional-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(300 * 1024 * 1024)));

        cfg.setCacheConfiguration(getCacheConfiguration());

        return cfg;
    }

    /** */
    @Test
    public void testRollingUpgrade() throws Exception {
        IgniteEx ign = startBaseCluster(3);

        ign.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ign.cache(CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        upgradeNode(1);
        upgradeNode(2);
        ign = upgradeNode(3);

        IgniteCache<Integer, Integer> targetCache = ign.cache(CACHE_NAME);

        for (int i = 0; i < 1000; i++)
            assertEquals("Data mismatch after upgrade at key: " + i, i, (int)targetCache.get(i));

        targetCache.put(1001, 1001);

        assertEquals(1001, (int)targetCache.get(1001));
    }

    /** */
    private CacheConfiguration<Integer, Integer> getCacheConfiguration() {
        return new CacheConfiguration<Integer, Integer>(CACHE_NAME)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    }
}
