package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class WalRebalanceBasicTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS = 8;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setBackups(1);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value="1")
    public void checkBasicWALRebalancing() throws Exception {
        IgniteEx node1 = startGrid(0);
        IgniteEx node2 = startGrid(1);

        node1.cluster().active(true);

        IgniteCache<Integer, Integer> cache1 = node1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < PARTS * 2; i++)
            cache1.put(i, i);

        forceCheckpoint();

        stopGrid(1);

        awaitPartitionMapExchange();

        for (int i = PARTS * 2; i < PARTS * 4; i++)
            cache1.put(i, i);

        startGrid(1);

        awaitPartitionMapExchange();
    }
}
