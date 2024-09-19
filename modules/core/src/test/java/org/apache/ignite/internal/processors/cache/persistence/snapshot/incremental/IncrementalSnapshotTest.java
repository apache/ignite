package org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IncrementalSnapshotTest  extends GridCommonAbstractTest {
    /** */
    protected static final String SNP = "base";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalCompactionEnabled(true)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(1);
        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void test() {
        IgniteEx ignite = grid(0);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 5; i++)
            cache.put(i, i);

        ignite.snapshot().createSnapshot(SNP).get();

        for (int j = 5; j < 10; j++)
            cache.put(j, j);

        ignite.snapshot().createIncrementalSnapshot(SNP).get();

        ignite.destroyCache(DEFAULT_CACHE_NAME);

        ignite.snapshot().restoreSnapshot(SNP, F.asList(DEFAULT_CACHE_NAME)).get(getTestTimeout());

        assertEquals(4, cache.get(4).intValue());

        ignite.destroyCache(DEFAULT_CACHE_NAME);

        ignite.snapshot().restoreSnapshot(SNP, F.asList(DEFAULT_CACHE_NAME), 1).get(getTestTimeout());

        assertEquals(6, cache.get(6).intValue());
    }
}
