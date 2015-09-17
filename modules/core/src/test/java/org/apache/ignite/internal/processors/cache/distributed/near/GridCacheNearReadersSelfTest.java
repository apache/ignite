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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Checks that readers are properly handled.
 */
public class GridCacheNearReadersSelfTest extends GridCommonAbstractTest {
    /** Number of grids. */
    private int grids = 2;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Grid counter. */
    private static AtomicInteger cntr = new AtomicInteger(0);

    /** Test cache affinity. */
    private GridCacheModuloAffinityFunction aff = new GridCacheModuloAffinityFunction();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setRebalanceMode(NONE);

        cacheCfg.setAffinity(aff);
        cacheCfg.setSwapEnabled(false);
        cacheCfg.setEvictSynchronized(true);
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setBackups(aff.backups());

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        cacheCfg.setNearConfiguration(nearCfg);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, cntr.getAndIncrement()));

        return cfg;
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** @throws Exception If failed. */
    private void startGrids() throws Exception {
        assert grids > 0;
        assert aff.backups() >= 0;

        startGrids(grids);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        grids = -1;

        aff.reset();

        cntr.set(0);
    }

    /**
     * @param nodeId Node ID.
     * @return Grid.
     */
    private Ignite grid(UUID nodeId) {
        return G.ignite(nodeId);
    }

    /** @throws Exception If failed. */
    public void testTwoNodesTwoKeysNoBackups() throws Exception {
        aff.backups(0);
        grids = 2;
        aff.partitions(grids);

        startGrids();

        ClusterNode n1 = F.first(aff.nodes(aff.partition(1), grid(0).cluster().nodes()));
        ClusterNode n2 = F.first(aff.nodes(aff.partition(2), grid(0).cluster().nodes()));

        assertNotNull(n1);
        assertNotNull(n2);
        assertNotSame(n1, n2);
        assertFalse("Nodes cannot be equal: " + n1, n1.equals(n2));

        Ignite g1 = grid(n1.id());
        Ignite g2 = grid(n2.id());

        IgniteCache<Integer, String> cache1 = g1.cache(null);
        IgniteCache<Integer, String> cache2 = g2.cache(null);

        // Store some values in cache.
        assertNull(cache1.getAndPut(1, "v1"));
        assertNull(cache1.getAndPut(2, "v2"));

        GridDhtCacheEntry e1 = (GridDhtCacheEntry)dht(cache1).entryEx(1);
        GridDhtCacheEntry e2 = (GridDhtCacheEntry)dht(cache2).entryEx(2);

        assertNotNull(e1.readers());

        assertTrue(cache1.containsKey(1));
        assertTrue(cache1.containsKey(2));

        assertNotNull(nearPeek(cache1, 1));
        assertNotNull(nearPeek(cache1, 2));
        assertNotNull(dhtPeek(cache1, 1));
        assertNull(dhtPeek(cache1, 2));

        assertNull(nearPeek(cache2, 1));
        assertNotNull(dhtPeek(cache2, 2));

        // Node2 should have node1 in reader's map, since request to
        // put key 2 came from node1.
        assertTrue(e2.readers().contains(n1.id()));

        // Node1 should not have node2 in readers map yet.
        assertFalse(e1.readers().contains(n2.id()));

        // Get key1 on node2.
        assertEquals("v1", cache2.get(1));

        // Check that key1 is in near cache of cache2.
        assertNotNull(nearPeek(cache2, 1));

        // Now node1 should have node2 in readers map.
        assertTrue(e1.readers().contains(n2.id()));

        // Evict locally from cache2.
        cache2.localEvict(Collections.singleton(1));

        assertNull(nearPeek(cache2, 1));
        assertNull(dhtPeek(cache2, 1));

        // Node 1 still has node2 in readers map.
        assertTrue(e1.readers().contains(n2.id()));

        assertNotNull(cache1.getAndPut(1, "z1"));

        // Node 1 still has node2 in readers map.
        assertFalse(e1.readers().contains(n2.id()));
    }

    /** @throws Exception If failed. */
    public void testTwoNodesTwoKeysOneBackup() throws Exception {
        aff.backups(1);
        grids = 2;
        aff.partitions(grids);

        startGrids();

        ClusterNode n1 = F.first(aff.nodes(aff.partition(1), grid(0).cluster().nodes()));
        ClusterNode n2 = F.first(aff.nodes(aff.partition(2), grid(0).cluster().nodes()));

        assertNotNull(n1);
        assertNotNull(n2);
        assertNotSame(n1, n2);
        assertFalse("Nodes cannot be equal: " + n1, n1.equals(n2));

        Ignite g1 = grid(n1.id());
        Ignite g2 = grid(n2.id());

        awaitPartitionMapExchange();

        GridCacheContext ctx = ((IgniteKernal) g1).internalCache(null).context();

        List<KeyCacheObject> cacheKeys = F.asList(ctx.toCacheKeyObject(1), ctx.toCacheKeyObject(2));

        ((IgniteKernal)g1).internalCache(null).preloader().request(cacheKeys, new AffinityTopologyVersion(2)).get();
        ((IgniteKernal)g2).internalCache(null).preloader().request(cacheKeys, new AffinityTopologyVersion(2)).get();

        IgniteCache<Integer, String> cache1 = g1.cache(null);
        IgniteCache<Integer, String> cache2 = g2.cache(null);

        assertEquals(g1.affinity(null).mapKeyToNode(1), g1.cluster().localNode());
        assertFalse(g1.affinity(null).mapKeyToNode(2).equals(g1.cluster().localNode()));

        assertEquals(g1.affinity(null).mapKeyToNode(2), g2.cluster().localNode());
        assertFalse(g2.affinity(null).mapKeyToNode(1).equals(g2.cluster().localNode()));

        // Store first value in cache.
        assertNull(cache1.getAndPut(1, "v1"));

        assertTrue(cache1.containsKey(1));
        assertTrue(cache2.containsKey(1));

        assertEquals("v1", nearPeek(cache1, 1));
        assertEquals("v1", nearPeek(cache2, 1));
        assertEquals("v1", dhtPeek(cache1, 1));
        assertEquals("v1", dhtPeek(cache2, 1));

        assertNull(near(cache1).peekEx(1));
        assertNull(near(cache2).peekEx(1));

        GridDhtCacheEntry e1 = (GridDhtCacheEntry)dht(cache1).entryEx(1);

        // Store second value in cache.
        assertNull(cache1.getAndPut(2, "v2"));

        assertTrue(cache1.containsKey(2));
        assertTrue(cache2.containsKey(2));

        assertEquals("v2", nearPeek(cache1, 2));
        assertEquals("v2", nearPeek(cache2, 2));
        assertEquals("v2", dhtPeek(cache1, 2));
        assertEquals("v2", dhtPeek(cache2, 2));

        assertNull(near(cache1).peekEx(2));
        assertNull(near(cache2).peekEx(2));

        GridDhtCacheEntry c2e2 = (GridDhtCacheEntry)dht(cache2).entryEx(2);

        // Nodes are backups of each other, so no readers should be added.
        assertFalse(c2e2.readers().contains(n1.id()));
        assertFalse(e1.readers().contains(n2.id()));

        // Get key1 on node2 (value should come from local DHT cache, as it has a backup).
        assertEquals("v1", cache2.get(1));

        // Since DHT cache2 has the value, Near cache2 should not have it.
        assertNull(near(cache2).peekEx(1));

        // Since v1 was retrieved locally from cache2, cache1 should not know about it.
        assertFalse(e1.readers().contains(n2.id()));

        // Evict locally from cache2.
        // It should not be successful since it's not allowed to evict entry on backup node.
        cache2.localEvict(Collections.singleton(1));

        assertNull(near(cache2).peekEx(1));
        assertEquals("v1", dhtPeek(cache2, 1));

        assertEquals("v1", cache1.getAndPut(1, "z1"));

        // Node 1 should not have node2 in readers map.
        assertFalse(e1.readers().contains(n2.id()));

        assertNull(near(cache2).peekEx(1));
        assertEquals("z1", dhtPeek(cache2, 1));
    }

    /** @throws Exception If failed. */
    public void testPutAllManyKeysOneReader() throws Exception {
        aff.backups(1);
        grids = 4;
        aff.partitions(grids);

        startGrids();

        try {
            IgniteCache<Object, Object> prj0 = grid(0).cache(null);
            IgniteCache<Object, Object> prj1 = grid(1).cache(null);

            Map<Integer, Integer> putMap = new HashMap<>();

            int size = 100;

            for (int i = 0; i < size; i++)
                putMap.put(i, i);

            prj0.putAll(putMap);

            for (int i = 0; i < size; i++)
                putMap.put(i, i * i);

            prj1.putAll(putMap);

            for (int i = 0; i < size; i++) {
                assertEquals(i * i, prj0.get(i));
                assertEquals(i * i, prj1.get(i));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testPutAllManyKeysTwoReaders() throws Exception {
        aff.backups(1);
        grids = 5;
        aff.partitions(grids);

        startGrids();

        try {
            IgniteCache<Object, Object> prj0 = grid(0).cache(null);
            IgniteCache<Object, Object> prj1 = grid(1).cache(null);
            IgniteCache<Object, Object> prj2 = grid(2).cache(null);

            Map<Integer, Integer> putMap = new HashMap<>();

            int size = 100;

            for (int i = 0; i < size; i++)
                putMap.put(i, i);

            prj0.putAll(putMap);

            for (int i = 0; i < size; i++)
                putMap.put(i, i * i);

            prj1.putAll(putMap);

            for (int i = 0; i < size; i++)
                putMap.put(i, i * i * i);

            prj2.putAll(putMap);

            for (int i = 0; i < size; i++) {
                assertEquals(i * i * i, prj0.get(i));
                assertEquals(i * i * i, prj1.get(i));
                assertEquals(i * i * i, prj2.get(i));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testBackupEntryReaders() throws Exception {
        aff.backups(1);
        grids = 2;
        aff.partitions(grids);

        startGrids();

        Collection<ClusterNode> nodes = new ArrayList<>(aff.nodes(aff.partition(1), grid(0).cluster().nodes()));

        ClusterNode primary = F.first(nodes);

        assert primary != null;

        nodes.remove(primary);

        ClusterNode backup = F.first(nodes);

        assert backup != null;

        assertNotSame(primary, backup);

        assertFalse("Nodes cannot be equal: " + primary, primary.equals(backup));

        IgniteCache<Integer, String> cache1 = grid(primary.id()).cache(null);
        IgniteCache<Integer, String> cache2 = grid(backup.id()).cache(null);

        // Store a values in cache.
        assertNull(cache1.getAndPut(1, "v1"));

        GridDhtCacheEntry e1 = (GridDhtCacheEntry)dht(cache1).peekEx(1);
        GridDhtCacheEntry e2 = (GridDhtCacheEntry)dht(cache2).peekEx(1);

        assert e1 != null;
        assert e2 != null;

        // Check entry on primary node.
        assertTrue(grid(primary.id()).affinity(null).isPrimary(primary, e1.key()));
        assertNotNull(e1.readers());
        assertTrue(e1.readers().isEmpty());

        // Check entry on backup node.
        assertFalse(grid(backup.id()).affinity(null).isPrimary(backup, e2.key()));
        assertNotNull(e2.readers());
        assertTrue(e2.readers().isEmpty());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings({"SizeReplaceableByIsEmpty"})
    public void testImplicitLockReaders() throws Exception {
        grids = 3;
        aff.reset(grids, 1);

        startGrids();

        int key1 = 3;
        String val1 = Integer.toString(key1);

        assertEquals(grid(0).localNode(), F.first(aff.nodes(aff.partition(key1), grid(0).cluster().nodes())));

        int key2 = 1;
        String val2 = Integer.toString(key2);

        assertEquals(grid(1).localNode(), F.first(aff.nodes(aff.partition(key2), grid(1).cluster().nodes())));

        IgniteCache<Integer, String> cache = jcache(0);

        assertNull(cache.getAndPut(key1, val1));

        assertEquals(val1, dhtPeek(0, key1));
        assertEquals(val1, dhtPeek(1, key1));
        assertNull(dhtPeek(2, key1));

        assertNull(near(0).peekEx(key1));
        assertNull(near(1).peekEx(key1));
        assertNull(near(2).peekEx(key1));

        cache.put(key2, val2);

        assertNull(dhtPeek(0, key2));
        assertEquals(val2, dhtPeek(1, key2));
        assertEquals(val2, dhtPeek(2, key2));

        assertEquals(val2, near(0).peekEx(key2).wrap().getValue());
        assertNull(near(1).peekEx(key2));
        assertNull(near(2).peekEx(key2));

        String val22 = val2 + "2";

        cache.put(key2, val22);

        assertNull(dhtPeek(0, key2));
        assertEquals(val22, dhtPeek(1, key2));
        assertEquals(val22, dhtPeek(2, key2));

        assertEquals(val22, near(0).peekEx(key2).wrap().getValue());
        assertNull(near(1).peekEx(key2));
        assertNull(near(2).peekEx(key2));

        cache.remove(key2);

        assertNull(dhtPeek(0, key2));
        assertNull(dhtPeek(1, key2));
        assertNull(dhtPeek(2, key2));

        assertTrue(near(0).peekEx(key2) == null || near(0).peekEx(key2).deleted());
        assertNull(near(1).peekEx(key2));
        assertNull(near(2).peekEx(key2));

        cache.remove(key1);

        assertNull(dhtPeek(0, key1));
        assertNull(dhtPeek(1, key1));
        assertNull(dhtPeek(2, key1));

        assertNull(near(0).peekEx(key1));
        assertNull(near(1).peekEx(key1));
        assertNull(near(2).peekEx(key1));

        for (int i = 0; i < grids; i++) {
            assert !jcache(i).isLocalLocked(key1, false);
            assert !jcache(i).isLocalLocked(key2, false);

            assert jcache(i).localSize() == 0;
        }
    }

    /** @throws Exception If failed. */
    public void testExplicitLockReaders() throws Exception {
        if (atomicityMode() == ATOMIC)
            return;

        grids = 3;
        aff.reset(grids, 1);

        startGrids();

        int key1 = 3;
        String val1 = Integer.toString(key1);

        assertEquals(grid(0).localNode(), F.first(aff.nodes(aff.partition(key1), grid(0).cluster().nodes())));

        int key2 = 1;
        String val2 = Integer.toString(key2);

        assertEquals(grid(1).localNode(), F.first(aff.nodes(aff.partition(key2), grid(1).cluster().nodes())));

        IgniteCache<Integer, String> cache = jcache(0);

        Lock lock1 = cache.lock(key1);

        lock1.lock();

        try {
            // Nested lock.
            Lock lock2 = cache.lock(key2);

            lock2.lock();

            try {
                assertNull(cache.getAndPut(key1, val1));

                assertEquals(val1, dhtPeek(0, key1));
                assertEquals(val1, dhtPeek(1, key1));
                assertNull(dhtPeek(2, key1));

                // Since near entry holds the lock, it should
                // contain correct value.
                assertEquals(val1, near(0).peekEx(key1).wrap().getValue());

                assertNull(near(1).peekEx(key1));
                assertNull(near(2).peekEx(key1));

                cache.put(key2, val2);

                assertNull(dhtPeek(0, key2));
                assertEquals(val2, dhtPeek(1, key2));
                assertEquals(val2, dhtPeek(2, key2));

                assertEquals(val2, near(0).peekEx(key2).wrap().getValue());
                assertNull(near(1).peekEx(key2));
                assertNull(near(2).peekEx(key2));

                String val22 = val2 + "2";

                cache.put(key2, val22);

                assertNull(dhtPeek(0, key2));
                assertEquals(val22, dhtPeek(1, key2));
                assertEquals(val22, dhtPeek(2, key2));

                assertEquals(val22, near(0).peekEx(key2).wrap().getValue());
                assertNull(near(1).peekEx(key2));
                assertNull(near(2).peekEx(key2));

                cache.remove(key2);

                assertNull(dhtPeek(0, key2));
                assertNull(dhtPeek(1, key2));
                assertNull(dhtPeek(2, key2));

                assertNull(dht(0).peekEx(key2));
                assertNotNull(dht(1).peekEx(key2));
                assertNotNull(dht(2).peekEx(key2));

                assertNotNull(near(0).peekEx(key2));
                assertNull(near(1).peekEx(key2));
                assertNull(near(2).peekEx(key2));
            }
            finally {
                lock2.unlock();
            }
        }
        finally {
            lock1.unlock();
        }
    }
}