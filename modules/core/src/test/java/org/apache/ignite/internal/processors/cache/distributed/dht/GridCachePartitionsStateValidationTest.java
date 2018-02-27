package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCachePartitionsStateValidationTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
        ));

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        return cfg;
    }

    /** {@inheritDoc */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test that partitions state validation works correctly.
     *
     * @throws Exception If failed.
     */
    public void testValidationIfPartitionCountersAreInconsistent() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrids(2);
        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        // Modify update counter.
        for (GridDhtLocalPartition partition : ignite.cachex(CACHE_NAME).context().topology().localPartitions()) {
            partition.updateCounter(100500L);
            break;
        }

        // Trigger exchange.
        startGrid(2);

        awaitPartitionMapExchange();

        // Nothing should happen and we're able to put data to corrupted cache.
        ignite.cache(CACHE_NAME).put(0, 0);

        stopAllGrids();
    }
}
