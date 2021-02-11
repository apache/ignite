/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;

/**
 * ScanQuery failover test. Tests scenario where user supplied closures throw unhandled errors.
 */
public class CacheScanQueryFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final String LOCAL_CACHE_NAME = "local";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryWithFailedClosures() throws Exception {
        Ignite srv = startGridsMultiThreaded(4);
        Ignite client = startClientGrid("client");

        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME).setCacheMode(PARTITIONED);

        // Test query from client node.
        queryCachesWithFailedPredicates(client, cfg);

        // Test query from server node.
        queryCachesWithFailedPredicates(srv, cfg);

        assertEquals(client.cluster().nodes().size(), 5);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScanQueryOverLocalCacheWithFailedClosures() throws Exception {
        Ignite srv = startGridsMultiThreaded(4);

        queryCachesWithFailedPredicates(srv, new CacheConfiguration(LOCAL_CACHE_NAME).setCacheMode(LOCAL));

        assertEquals(srv.cluster().nodes().size(), 4);
    }

    /**
     * Test scan query when partitions are concurrently evicting.
     */
    @Test
    public void testScanQueryOnEvictedPartition() throws Exception {
        cleanPersistenceDir();

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(true));

        IgniteEx grid0 = startGrid(getConfiguration("grid0").setDataStorageConfiguration(dsCfg));

        grid0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache1 = grid0.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>("cache1")
                .setAffinity(new RoundRobinAffinityFunction(2))
        );

        IgniteCache<Integer, Integer> cache2 = grid0.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>("cache2")
                .setAffinity(new RoundRobinAffinityFunction(2))
        );

        cache1.put(0, 0); // Put to partition 0.
        cache1.put(1, 1); // Put to partition 1.

        cache2.put(0, 0); // Put to partition 0.

        for (int i = 1; i < 1_000; i += 2)
            cache2.put(i, i); // Put to partition 1.

        Iterator iter1 = cache1.query(new ScanQuery<>().setPageSize(1)).iterator();
        Iterator iter2 = cache1.query(new ScanQuery<>().setPageSize(1)).iterator();

        // Iter 1 check case, when cursor is switched to evicted partition.
        iter1.next();

        // Iter 2 check case, when cursor already moving by partition and this partition is evicted.
        iter2.next();
        iter2.next();

        startGrid(getConfiguration("grid1").setDataStorageConfiguration(dsCfg));

        grid0.cluster().setBaselineTopology(grid0.cluster().topologyVersion());

        // Wait for rebalance and evition of partition 1 to grid 1 for each cache.
        awaitPartitionMapExchange();

        assertTrue(GridTestUtils.waitForCondition(() ->
            grid0.cachex("cache1").context().topology().localPartition(1).state() == EVICTED &&
                grid0.cachex("cache2").context().topology().localPartition(1).state() == EVICTED,
            1_000L));

        // Force checkpoint to destroy evicted partitions store.
        forceCheckpoint(grid0);

        GridTestUtils.assertThrowsAnyCause(log, iter1::next, IgniteException.class, "Failed to get next data row");

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            while (iter2.hasNext())
                iter2.next();

            return null;
        }, IgniteException.class, "Failed to get next data row");
    }

    /**
     * @param ignite Ignite instance.
     * @param configs Cache configurations.
     */
    private void queryCachesWithFailedPredicates(Ignite ignite, CacheConfiguration... configs) {
        if (configs == null)
            return;

        for (CacheConfiguration cfg: configs) {
            IgniteCache cache = ignite.getOrCreateCache(cfg);

            populateCache(ignite, cache.getName());

            // Check that exception propagates to client from filter failure.
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor =
                             cache.withKeepBinary().query(new ScanQuery<>(filter))) {
                    for (Cache.Entry<Integer, BinaryObject> entry : cursor)
                        log.info("Entry " + entry.toString());
                }

                return null;
            }, Error.class, "Poison pill");

            // Check that exception propagates to client from transformer failure.
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                try (QueryCursor<Cache.Entry<Integer, BinaryObject>> cursor =
                             cache.withKeepBinary().query(new ScanQuery<>(), transformer)) {
                    for (Cache.Entry<Integer, BinaryObject> entry : cursor)
                        log.info("Entry " + entry.toString());
                }

                return null;
            }, Error.class, "Poison pill");
        }
    }

    /**
     * @param ignite Ignite instance.
     * @param cacheName Cache name.
     */
    private void populateCache(Ignite ignite, String cacheName) {
        IgniteBinary binary = ignite.binary();

        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < 1_000; i++)
                streamer.addData(i, binary.builder("type_name").setField("f_" + i, "v_" + i).build());
        }
    }

    /** Failed filter. */
    private static IgniteBiPredicate<Integer, BinaryObject> filter = (key, value) -> {
            throw new Error("Poison pill");
        };

    /** Failed entry transformer. */
    private static IgniteClosure<Cache.Entry<Integer, BinaryObject>, Cache.Entry<Integer, BinaryObject>> transformer =
            integerBinaryObjectEntry -> {
                throw new Error("Poison pill");
        };

    /**
     * Affinity function to distribute partitions by round robin to each node.
     */
    private static class RoundRobinAffinityFunction implements AffinityFunction {
        /** Partitions count. */
        private final int partitions;

        /**
         * @param partitions Partitions count.
         */
        public RoundRobinAffinityFunction(int partitions) {
            this.partitions = partitions;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return partitions;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return key.hashCode() % partitions;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>(partitions);
            List<ClusterNode> nodes = affCtx.currentTopologySnapshot();
            nodes.sort(NodeOrderComparator.getInstance());

            for (int i = 0; i < partitions; i++)
                res.add(Collections.singletonList(nodes.get(i % nodes.size())));

            return res;
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }
}
