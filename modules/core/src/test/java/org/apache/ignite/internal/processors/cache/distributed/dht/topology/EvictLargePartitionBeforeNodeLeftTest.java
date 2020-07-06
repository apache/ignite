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

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Tests if currently evicting partition is owned after last supplier has left.
 */
public class EvictLargePartitionBeforeNodeLeftTest extends GridCommonAbstractTest {
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

            String CACHE = "dotnet_binary_cache";

            IgniteCache<Object, Object> cache = g0.getOrCreateCache(CACHE);

            int p0 = movingKeysAfterJoin(g0, CACHE, 1).get(0);

            List<Integer> keys = partitionKeys(g0.cache(CACHE), p0, 20_000, 0);

            try(IgniteDataStreamer<Object, Object> ds = g0.dataStreamer(CACHE)){
                for (Integer key : keys)
                    ds.addData(key, key);
            }

            IgniteEx g1 = startGrid(1);

            awaitPartitionMapExchange();

            assertEquals(GridDhtPartitionState.RENTING, g0.cachex(CACHE).context().topology().localPartition(p0).state());

            g1.close();

            GridDhtTopologyFuture fut = g0.cachex(CACHE).context().topology().topologyVersionFuture();

            assertEquals(new AffinityTopologyVersion(3, 0), fut.topologyVersion());

            fut.get();

            assertEquals(OWNING, g0.cachex(CACHE).context().topology().localPartition(p0).state());
        }
        finally {
            stopAllGrids();
        }
    }
}
