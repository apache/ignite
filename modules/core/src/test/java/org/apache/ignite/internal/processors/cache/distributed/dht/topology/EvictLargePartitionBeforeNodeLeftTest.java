package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * Tests if currently evicting partition is owned after last supplier has left.
 *
 * Add a test for persistent mode with delayed checkpoint after eviction.
 */
public class EvictLargePartitionBeforeNodeLeftTest extends GridCommonAbstractTest {
    /** */
    private boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOwnedAfterEviction() throws Exception {
        try {
            IgniteEx g0 = startGrid(0);

            IgniteCache<Object, Object> cache = g0.getOrCreateCache(DEFAULT_CACHE_NAME);

            int p0 = movingKeysAfterJoin(g0, DEFAULT_CACHE_NAME, 1).get(0);

            List<Integer> keys = partitionKeys(g0.cache(DEFAULT_CACHE_NAME), p0, 20_000, 0);

            try(IgniteDataStreamer<Object, Object> ds = g0.dataStreamer(DEFAULT_CACHE_NAME)){
                for (Integer key : keys)
                    ds.addData(key, key);
            }

            IgniteEx g1 = startGrid(1);

            awaitPartitionMapExchange();

            GridDhtPartitionState state = g0.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(p0).state();

            assertTrue(state == RENTING || state == EVICTED);

            g1.close();

            GridDhtTopologyFuture fut = g0.cachex(DEFAULT_CACHE_NAME).context().topology().topologyVersionFuture();

            assertEquals(new AffinityTopologyVersion(3, 0), fut.initialVersion());

            fut.get();

            GridDhtLocalPartition part = g0.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(p0);

            assertEquals(OWNING, part.state());
        }
        finally {
            stopAllGrids();
        }
    }
}
