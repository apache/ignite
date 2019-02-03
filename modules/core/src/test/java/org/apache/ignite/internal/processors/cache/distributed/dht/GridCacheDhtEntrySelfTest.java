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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Unit tests for dht entry.
 */
public class GridCacheDhtEntrySelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        cacheCfg.setBackups(0);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SizeReplaceableByIsEmpty"})
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);

        for (int i = 0; i < GRID_CNT; i++) {
            assert near(grid(i)).size() == 0 : "Near cache size is not zero for grid: " + i;
            assert dht(grid(i)).size() == 0 : "DHT cache size is not zero for grid: " + i;

            assert near(grid(i)).localSize() == 0 : "Near cache is not empty for grid: " + i;
            assert dht(grid(i)).isEmpty() : "DHT cache is not empty for grid: " + i;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            near(grid(i)).removeAll();

            assertEquals("Near cache size is not zero for grid: " + i, 0, near(grid(i)).size());
            assertEquals("DHT cache size is not zero for grid: " + i, 0, dht(grid(i)).size());

            assert near(grid(i)).localSize() == 0 : "Near cache is not empty for grid: " + i;
            assert dht(grid(i)).isEmpty() : "DHT cache is not empty for grid: " + i;
        }

        for (int i = 0; i < GRID_CNT; i++) {
            Transaction tx = grid(i).transactions().tx();

            if (tx != null)
                tx.close();
        }
    }

    /**
     * @param g Grid.
     * @return Near cache.
     */
    private IgniteCache<Integer, String> near(Ignite g) {
        return g.cache(DEFAULT_CACHE_NAME);
    }

    /**
     * @param g Grid.
     * @return Dht cache.
     */
    @SuppressWarnings({"unchecked"})
    private GridDhtCacheAdapter<Integer, String> dht(Ignite g) {
        return ((GridNearCacheAdapter)((IgniteKernal)g).internalCache(DEFAULT_CACHE_NAME)).dht();
    }

    /**
     * @param nodeId Node ID.
     * @return Grid.
     */
    private Ignite grid(UUID nodeId) {
        return G.ignite(nodeId);
    }

    /** @throws Exception If failed. */
    @Test
    public void testClearWithReaders() throws Exception {
        Integer key = 1;

        IgniteBiTuple<ClusterNode, ClusterNode> t = getNodes(key);

        ClusterNode primary = t.get1();
        ClusterNode other = t.get2();

        IgniteCache<Integer, String> near0 = near(grid(primary.id()));
        IgniteCache<Integer, String> near1 = near(grid(other.id()));

        assert near0 != near1;

        GridDhtCacheAdapter<Integer, String> dht0 = dht(grid(primary.id()));
        GridDhtCacheAdapter<Integer, String> dht1 = dht(grid(other.id()));

        // Put on primary node.
        String val = "v1";

        near0.put(key, val);

        GridDhtCacheEntry e0 = (GridDhtCacheEntry)dht0.peekEx(key);
        GridDhtCacheEntry e1 = (GridDhtCacheEntry)dht1.peekEx(key);

        assert e0 == null || e0.readers().isEmpty();
        assert e1 == null || e1.readers().isEmpty();

        // Get value on other node.
        assertEquals(val, near1.get(key));

        e0 = (GridDhtCacheEntry)dht0.peekEx(key);
        e1 = (GridDhtCacheEntry)dht1.peekEx(key);

        assert e0 != null;

        assert e0.readers().contains(other.id());
        assert e1 == null || e1.readers().isEmpty();

        assert !internalCache(near0).clearLocally(key);

        assertEquals(1, near0.localSize(CachePeekMode.ALL));
        assertEquals(1, dht0.size());

        assertEquals(1, near1.localSize(CachePeekMode.ALL));
        assertEquals(0, dht1.size());
    }

    /** @throws Exception If failed. */
    @Test
    public void testRemoveWithReaders() throws Exception {
        Integer key = 1;

        IgniteBiTuple<ClusterNode, ClusterNode> t = getNodes(key);

        ClusterNode primary = t.get1();
        ClusterNode other = t.get2();

        IgniteCache<Integer, String> near0 = near(grid(primary.id()));
        IgniteCache<Integer, String> near1 = near(grid(other.id()));

        assert near0 != near1;

        GridDhtCacheAdapter<Integer, String> dht0 = dht(grid(primary.id()));
        GridDhtCacheAdapter<Integer, String> dht1 = dht(grid(other.id()));

        // Put on primary node.
        String val = "v1";

        near0.put(key, val);

        GridDhtCacheEntry e0 = (GridDhtCacheEntry)dht0.peekEx(key);
        GridDhtCacheEntry e1 = (GridDhtCacheEntry)dht1.peekEx(key);

        assert e0 == null || e0.readers().isEmpty();
        assert e1 == null || e1.readers().isEmpty();

        // Get value on other node.
        assertEquals(val, near1.get(key));

        e0 = (GridDhtCacheEntry)dht0.peekEx(key);
        e1 = (GridDhtCacheEntry)dht1.peekEx(key);

        assert e0 != null;

        assert e0.readers().contains(other.id());
        assert e1 == null || e1.readers().isEmpty();

        assert near0.remove(key);

        assertEquals(0, near0.size());
        assertEquals(0, dht0.size());

        assertEquals(0, near1.size());
        assertEquals(0, dht1.size());
    }

    /** @throws Exception If failed. */
    @Test
    public void testEvictWithReaders() throws Exception {
        Integer key = 1;

        IgniteBiTuple<ClusterNode, ClusterNode> t = getNodes(key);

        ClusterNode primary = t.get1();
        ClusterNode other = t.get2();

        IgniteCache<Integer, String> near0 = near(grid(primary.id()));
        IgniteCache<Integer, String> near1 = near(grid(other.id()));

        assert near0 != near1;

        GridDhtCacheAdapter<Integer, String> dht0 = dht(grid(primary.id()));
        GridDhtCacheAdapter<Integer, String> dht1 = dht(grid(other.id()));

        // Put on primary node.
        String val = "v1";

        near0.put(key, val);

        GridDhtCacheEntry e0 = (GridDhtCacheEntry)dht0.peekEx(key);
        GridDhtCacheEntry e1 = (GridDhtCacheEntry)dht1.peekEx(key);

        assert e0 == null || e0.readers().isEmpty();
        assert e1 == null || e1.readers().isEmpty();

        // Get value on other node.
        assertEquals(val, near1.get(key));

        e0 = (GridDhtCacheEntry)dht0.peekEx(key);
        e1 = (GridDhtCacheEntry)dht1.peekEx(key);

        assert e0 != null;

        assert e0.readers().contains(other.id());
        assert e1 == null || e1.readers().isEmpty();

        assert !e0.evictInternal(dht0.context().versions().next(), null, false);

        assertEquals(1, near0.localSize(CachePeekMode.ALL));
        assertEquals(1, dht0.localSize(null));

        assertEquals(1, near1.localSize(CachePeekMode.ALL));
        assertEquals(0, dht1.localSize(null));

        assert !e0.evictInternal(dht0.context().versions().next(), null, false);

        assertEquals(1, near0.localSize(CachePeekMode.ALL));
        assertEquals(1, dht0.localSize(null));

        assertEquals(1, near1.localSize(CachePeekMode.ALL));
        assertEquals(0, dht1.localSize(null));
    }

    /**
     * @param key Key.
     * @return For the given key pair {primary node, some other node}.
     */
    private IgniteBiTuple<ClusterNode, ClusterNode> getNodes(Integer key) {
        Affinity<Integer> aff = grid(0).affinity(DEFAULT_CACHE_NAME);

        int part = aff.partition(key);

        ClusterNode primary = aff.mapPartitionToNode(part);

        assert primary != null;

        Collection<ClusterNode> nodes = new ArrayList<>(grid(0).cluster().nodes());

        nodes.remove(primary);

        ClusterNode other = F.first(nodes);

        assert other != null;

        assert !F.eqNodes(primary, other);

        return F.t(primary, other);
    }
}
