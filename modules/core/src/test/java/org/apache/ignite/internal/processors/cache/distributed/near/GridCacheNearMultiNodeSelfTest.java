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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Multi node test for near cache.
 */
public class GridCacheNearMultiNodeSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** */
    private static final int BACKUPS = 1;

    /** Cache store. */
    private static TestStore store = new TestStore();

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Grid counter. */
    private AtomicInteger cntr = new AtomicInteger(0);

    /** Affinity based on node index mode. */
    private AffinityFunction aff = new GridCacheModuloAffinityFunction(GRID_CNT, BACKUPS);

    /** Debug flag for mappings. */
    private boolean mapDebug = true;

    /**
     *
     */
    public GridCacheNearMultiNodeSelfTest() {
        super(false /* don't start grid. */);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);
        spi.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        cfg.setDiscoverySpi(spi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setCacheStoreFactory(singletonFactory(store));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinity(aff);
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setBackups(BACKUPS);
        cacheCfg.setNearConfiguration(new NearCacheConfiguration());

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, cntr.getAndIncrement()));

        return cfg;
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return {@code True} if tests transactional cache.
     */
    protected boolean transactional() {
        return atomicityMode() == TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        assert G.allGrids().isEmpty();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SizeReplaceableByIsEmpty"})
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            assert jcache(i).localSize() == 0 : "Near cache size is not zero for grid: " + i;
            assert dht(grid(i)).size() == 0 : "DHT cache size is not zero for grid: " + i;

            assert jcache(i).localSize() == 0 : "Near cache is not empty for grid: " + i;
            assert dht(grid(i)).isEmpty() : "DHT cache is not empty for grid: " + i;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SizeReplaceableByIsEmpty"})
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            jcache(i).removeAll();

            assertEquals("Near cache size is not zero for grid: " + i, 0, jcache(i).localSize());
            assertEquals("DHT cache size is not zero for grid: " + i, 0, dht(grid(i)).size());

            assert jcache(i).localSize() == 0 : "Near cache is not empty for grid: " + i;
            assert dht(grid(i)).isEmpty() : "DHT cache is not empty for grid: " + i;
        }

        store.reset();

        for (int i = 0; i < GRID_CNT; i++) {
            Transaction tx = grid(i).transactions().tx();

            if (tx != null) {
                error("Ending zombie transaction: " + tx);

                tx.close();
            }
        }
    }

    /**
     * @param g Grid.
     * @return Dht cache.
     */
    @SuppressWarnings({"unchecked", "TypeMayBeWeakened"})
    private GridDhtCacheAdapter<Integer, String> dht(Ignite g) {
        return ((GridNearCacheAdapter)((IgniteKernal)g).internalCache()).dht();
    }

    /**
     * @param idx Index.
     * @return Affinity.
     */
    private Affinity<Object> affinity(int idx) {
        return grid(idx).affinity(null);
    }

    /** @param cnt Count. */
    private Map<UUID, T2<Set<Integer>, Set<Integer>>> mapKeys(int cnt) {
        Affinity<Object> aff = affinity(0);

        //Mapping primary and backup keys on node
        Map<UUID, T2<Set<Integer>, Set<Integer>>> map = new HashMap<>();

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteEx grid = grid(i);

            map.put(grid.cluster().localNode().id(), new T2<Set<Integer>, Set<Integer>>(new HashSet<Integer>(),
                new HashSet<Integer>()));
        }

        for (int key = 1; key <= cnt; key++) {
            Integer part = aff.partition(key);

            assert part != null;

            Collection<ClusterNode> nodes = aff.mapPartitionToPrimaryAndBackups(part);

            ClusterNode primary = F.first(nodes);

            map.get(primary.id()).get1().add(key);

            if (mapDebug)
                info("Mapped key to primary node [key=" + key + ", node=" + U.toShortString(primary));

            for (ClusterNode n : nodes) {
                if (n != primary) {
                    map.get(n.id()).get2().add(key);

                    if (mapDebug)
                        info("Mapped key to backup node [key=" + key + ", node=" + U.toShortString(n));
                }
            }
        }

        return map;
    }

    /**  Test mappings. */
    public void testMappings() {
        mapDebug = false;

        int cnt = 100000;

        Map<UUID, T2<Set<Integer>, Set<Integer>>> map = mapKeys(cnt);

        for (ClusterNode n : grid(0).cluster().nodes()) {
            Set<Integer> primary = map.get(n.id()).get1();
            Set<Integer> backups = map.get(n.id()).get2();

            if (backups == null)
                backups = Collections.emptySet();

            info("Grid node [primaries=" + primary.size() + ", backups=" + backups.size() + ']');

            assert !F.isEmpty(primary);

            assertEquals(backups.size(), cnt - primary.size());
        }
    }

    /**
     * @param key Key.
     * @return Primary node for the key.
     */
    @Nullable private ClusterNode primaryNode(Integer key) {
        return affinity(0).mapKeyToNode(key);
    }

    /**
     * @param key Key.
     * @return Primary node for the key.
     */
    @Nullable private Ignite primaryGrid(Integer key) {
        ClusterNode n = affinity(0).mapKeyToNode(key);

        assert n != null;

        return G.ignite(n.id());
    }

    /**
     * @param key Key.
     * @return Primary node for the key.
     */
    @Nullable private Collection<Ignite> backupGrids(Integer key) {
        Collection<ClusterNode> nodes = affinity(0).mapKeyToPrimaryAndBackups(key);

        Collection<ClusterNode> backups = CU.backups(nodes);

        return F.viewReadOnly(backups,
            new C1<ClusterNode, Ignite>() {
                @Override public Ignite apply(ClusterNode node) {
                    return G.ignite(node.id());
                }
            });
    }

    /** @throws Exception If failed. */
    public void testReadThroughAndPut() throws Exception {
        Integer key = 100000;

        Ignite primary;
        Ignite backup;

        if (grid(0) == primaryGrid(key)) {
            primary = grid(0);
            backup = grid(1);
        }
        else {
            primary = grid(1);
            backup = grid(0);
        }

        assertEquals(String.valueOf(key), backup.cache(null).get(key));

        primary.cache(null).put(key, "a");

        assertEquals("a", backup.cache(null).get(key));
    }

    /** @throws Exception If failed. */
    public void testReadThrough() throws Exception {
        ClusterNode loc = grid(0).localNode();

        info("Local node: " + U.toShortString(loc));

        IgniteCache<Integer, String> near = jcache(0);

        int cnt = 10;

        Map<UUID, T2<Set<Integer>, Set<Integer>>> mapKeys = mapKeys(cnt);

        for (int key = 1; key <= cnt; key++) {
            String s = near.get(key);

            info("Read key [key=" + key + ", val=" + s + ']');

            assert s != null;
        }

        info("Read all keys.");

        for (int key = 1; key <= cnt; key++) {
            ClusterNode n = primaryNode(key);

            info("Primary node for key [key=" + key + ", node=" + U.toShortString(n) + ']');

            assert n != null;

            assert mapKeys.get(n.id()).get1().contains(key);

            GridCacheAdapter<Integer, String> dhtCache = dht(G.ignite(n.id()));

            String s = dhtCache.localPeek(key, new CachePeekMode[] {CachePeekMode.ONHEAP}, null);

            assert s != null : "Value is null for key: " + key;
            assertEquals(s, Integer.toString(key));
        }
    }

    /**
     * Test Optimistic repeatable read write-through.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ConstantConditions"})
    public void testOptimisticWriteThrough() throws Exception {
        IgniteCache<Integer, String> near = jcache(0);

        if (transactional()) {
            try (Transaction tx = grid(0).transactions().txStart(OPTIMISTIC, REPEATABLE_READ, 0, 0)) {
                near.put(2, "2");

                String s = near.getAndPut(3, "3");

                assertNotNull(s);
                assertEquals("3", s);

                assertEquals("2", near.get(2));
                assertEquals("3", near.get(3));

                GridDhtCacheEntry entry = (GridDhtCacheEntry)dht(primaryGrid(2)).peekEx(2);

                if (entry != null)
                    assertNull("Unexpected entry: " + entry, entry.rawGetOrUnmarshal(false));

                assertNotNull(localPeek(dht(primaryGrid(3)), 3));

                tx.commit();
            }
        }
        else {
            near.put(2, "2");

            String s = near.getAndPut(3, "3");

            assertNotNull(s);
            assertEquals("3", s);
        }

        assertEquals("2", near.localPeek(2, CachePeekMode.ONHEAP));
        assertEquals("3", near.localPeek(3, CachePeekMode.ONHEAP));

        assertEquals("2", localPeek(dht(primaryGrid(2)), 2));
        assertEquals("3", localPeek(dht(primaryGrid(3)), 3));

        assertEquals(2, near.localSize(CachePeekMode.ALL));
        assertEquals(2, near.localSize(CachePeekMode.ALL));
    }

    /** @throws Exception If failed. */
    public void testNoTransactionSinglePutx() throws Exception {
        IgniteCache<Integer, String> near = jcache(0);

        near.put(2, "2");

        assertEquals("2", near.localPeek(2, CachePeekMode.ONHEAP));
        assertEquals("2", near.get(2));

        assertEquals("2", localPeek(dht(primaryGrid(2)), 2));

        assertEquals(1, near.localSize());
        assertEquals(1, near.localSize());

        assertEquals(1, dht(primaryGrid(2)).size());
    }

    /** @throws Exception If failed. */
    public void testNoTransactionSinglePut() throws Exception {
        IgniteCache<Integer, String> near = jcache(0);

        // There should be a not-null previously mapped value because
        // we use a store implementation that just returns values which
        // are string representations of requesting integer keys.
        String s = near.getAndPut(3, "3");

        assertNotNull(s);
        assertEquals("3", s);

        assertEquals("3", near.localPeek(3, CachePeekMode.ONHEAP));
        assertEquals("3", near.get(3));

        Ignite primaryIgnite = primaryGrid(3);

        assert primaryIgnite != null;

        info("Primary grid for key 3: " + U.toShortString(primaryIgnite.cluster().localNode()));

        assertEquals("3", localPeek(dht(primaryIgnite), 3));

        assertEquals(1, near.localSize(CachePeekMode.ALL));
        assertEquals(1, near.localSize(CachePeekMode.ALL));

        assertEquals(1, dht(primaryIgnite).size());

        // Check backup nodes.
        Collection<Ignite> backups = backupGrids(3);

        assert backups != null;

        for (Ignite b : backups) {
            info("Backup grid for key 3: " + U.toShortString(b.cluster().localNode()));

            assertEquals("3", localPeek(dht(b), 3));

            assertEquals(1, dht(b).size());
        }
    }

    /** @throws Exception If failed. */
    public void testNoTransactionWriteThrough() throws Exception {
        IgniteCache<Integer, String> near = jcache(0);

        near.put(2, "2");

        String s = near.getAndPut(3, "3");

        assertNotNull(s);
        assertEquals("3", s);

        assertEquals("2", near.localPeek(2, CachePeekMode.ONHEAP));
        assertEquals("3", near.localPeek(3, CachePeekMode.ONHEAP));

        assertEquals("2", near.get(2));
        assertEquals("3", near.get(3));

        assertEquals("2", localPeek(dht(primaryGrid(2)), 2));
        assertEquals("3", localPeek(dht(primaryGrid(3)), 3));

        assertEquals(2, near.localSize(CachePeekMode.ALL));
        assertEquals(2, near.localSize(CachePeekMode.ALL));
    }

    /**
     * Test Optimistic repeatable read write-through.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ConstantConditions"})
    public void testPessimisticWriteThrough() throws Exception {
        IgniteCache<Integer, String> near = jcache(0);

        if (transactional()) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 0)) {
                near.put(2, "2");

                String s = near.getAndPut(3, "3");

                assertEquals("3", s);

                assertEquals("2", near.get(2));
                assertEquals("3", near.get(3));

                assertNotNull(dht(primaryGrid(3)).localPeek(3, new CachePeekMode[] {CachePeekMode.ONHEAP}, null));

                tx.commit();
            }
        }
        else {
            near.put(2, "2");

            String s = near.getAndPut(3, "3");

            assertNotNull(s);
            assertEquals("3", s);
        }

        assertEquals("2", near.localPeek(2, CachePeekMode.ONHEAP));
        assertEquals("3", near.localPeek(3, CachePeekMode.ONHEAP));

        assertEquals("2", localPeek(dht(primaryGrid(2)), 2));
        assertEquals("3", localPeek(dht(primaryGrid(3)), 3));

        assertEquals(2, near.localSize(CachePeekMode.ALL));
        assertEquals(2, near.localSize(CachePeekMode.ALL));
    }

    /** @throws Exception If failed. */
    public void testConcurrentOps() throws Exception {
        // Don't create missing values.
        store.create(false);

        IgniteCache<Integer, String> near = jcache(0);

        int key = 1;

        assertTrue(near.putIfAbsent(key, "1"));
        assertFalse(near.putIfAbsent(key, "1"));
        assertEquals("1", near.getAndPutIfAbsent(key, "2"));

        assertEquals("1", near.localPeek(key, CachePeekMode.ONHEAP));
        assertEquals(1, near.localSize(CachePeekMode.ALL));
        assertEquals(1, near.localSize(CachePeekMode.ALL));

        assertEquals("1", near.getAndReplace(key, "2"));
        assertEquals("2", near.localPeek(key, CachePeekMode.ONHEAP));

        assertTrue(near.replace(key, "2"));

        assertEquals("2", near.localPeek(key, CachePeekMode.ONHEAP));
        assertEquals(1, near.localSize(CachePeekMode.ALL));
        assertEquals(1, near.localSize(CachePeekMode.ALL));

        assertTrue(near.remove(key, "2"));

        assertEquals(0, near.localSize(CachePeekMode.ALL));
    }

    /** @throws Exception If failed. */
    public void testBackupsLocalAffinity() throws Exception {
        checkBackupConsistency(2);
    }

    /** @throws Exception If failed. */
    public void testBackupsRemoteAffinity() throws Exception {
        checkBackupConsistency(1);
    }

    /**
     * @param key Key to check.
     * @throws Exception If failed.
     */
    private void checkBackupConsistency(int key) throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        String val = Integer.toString(key);

        cache.put(key, val);

        GridDhtCacheAdapter<Integer, String> dht0 = dht(0);
        GridDhtCacheAdapter<Integer, String> dht1 = dht(1);

        assertNull(near(0).peekEx(key));
        assertNull(near(1).peekEx(key));

        assertEquals(val, localPeek(dht0, key));
        assertEquals(val, localPeek(dht1, key));
    }

    /** @throws Exception If failed. */
    public void testSingleLockLocalAffinity() throws Exception {
        checkSingleLock(2);
    }

    /** @throws Exception If failed. */
    public void testSingleLockRemoteAffinity() throws Exception {
        checkSingleLock(1);
    }

    /**
     * @param idx Grid index.
     * @param key Key.
     * @return Near entry.
     */
    @Nullable private GridNearCacheEntry nearEntry(int idx, int key) {
        return (GridNearCacheEntry)near(idx).peekEx(key);
    }

    /**
     * @param o Object.
     * @return Hash value.
     */
    private int hash(Object o) {
        return System.identityHashCode(o);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkSingleLock(int key) throws Exception {
        if (!transactional())
            return;

        IgniteCache<Integer, String> cache = jcache(0);

        String val = Integer.toString(key);

        Collection<ClusterNode> affNodes = grid(0).affinity(null).mapKeyToPrimaryAndBackups(key);

        info("Affinity for key [nodeId=" + U.nodeIds(affNodes) + ", key=" + key + ']');

        assertEquals(2, affNodes.size());

        ClusterNode primary = F.first(affNodes);

        assertNotNull(primary);

        info("Primary local: " + primary.isLocal());

        Lock lock = cache.lock(key);

        lock.lock();

        try {
            AffinityTopologyVersion topVer = new AffinityTopologyVersion(grid(0).cluster().topologyVersion());

            GridNearCacheEntry nearEntry1 = nearEntry(0, key);

            info("Peeked entry after lock [hash=" + hash(nearEntry1) + ", nearEntry=" + nearEntry1 + ']');

            assertNotNull(nearEntry1);
            assertTrue("Invalid near entry: " + nearEntry1, nearEntry1.valid(topVer));

            assertTrue(cache.isLocalLocked(key, false));
            assertTrue(cache.isLocalLocked(key, true));

            cache.put(key, val);

            GridNearCacheEntry nearEntry2 = nearEntry(0, key);

            info("Peeked entry after put [hash=" + hash(nearEntry1) + ", nearEntry=" + nearEntry2 + ']');

            assert nearEntry1 == nearEntry2;

            assertNotNull(nearEntry2);
            assertTrue("Invalid near entry [hash=" + nearEntry2, nearEntry2.valid(topVer));

            assertEquals(val, cache.localPeek(key, CachePeekMode.ONHEAP));
            assertEquals(val, dhtPeek(0, key));
            assertEquals(val, dhtPeek(1, key));

            GridNearCacheEntry nearEntry3 = nearEntry(0, key);

            info("Peeked entry after peeks [hash=" + hash(nearEntry1) + ", nearEntry=" + nearEntry3 + ']');

            assert nearEntry2 == nearEntry3;

            assertNotNull(nearEntry3);
            assertTrue("Invalid near entry: " + nearEntry3, nearEntry3.valid(topVer));

            assertNotNull(near(0).peekEx(key));
            assertNull(near(1).peekEx(key));

            assertEquals(val, cache.get(key));
            assertEquals(val, cache.getAndRemove(key));

            assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));
            assertNull(localPeek(dht(primaryGrid(key)), key));

            assertTrue(cache.isLocalLocked(key, false));
            assertTrue(cache.isLocalLocked(key, true));
        }
        finally {
            lock.unlock();
        }

        assertNull(near(0).peekEx(key));
        assertNull(near(1).peekEx(key));

        assertFalse(near(0).isLockedNearOnly(key));
        assertFalse(cache.isLocalLocked(key, true));
    }

    /** @throws Throwable If failed. */
    public void testSingleLockReentryLocalAffinity() throws Throwable {
        checkSingleLockReentry(2);
    }

    /** @throws Throwable If failed. */
    public void testSingleLockReentryRemoteAffinity() throws Throwable {
        checkSingleLockReentry(1);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkSingleLockReentry(int key) throws Throwable {
        if (!transactional())
            return;

        IgniteCache<Integer, String> near = jcache(0);

        String val = Integer.toString(key);

        Lock lock = near.lock(key);

        lock.lock();

        try {
            near.put(key, val);

            assertEquals(val, near.localPeek(key, CachePeekMode.ONHEAP));
            assertEquals(val, localPeek(dht(primaryGrid(key)), key));

            assertTrue(near.isLocalLocked(key, false));
            assertTrue(near.isLocalLocked(key, true));

            lock.lock(); // Reentry.

            try {
                assertEquals(val, near.get(key));
                assertEquals(val, near.getAndRemove(key));

                assertNull(near.localPeek(key, CachePeekMode.ONHEAP));
                assertNull(localPeek(dht(primaryGrid(key)), key));

                assertTrue(near.isLocalLocked(key, false));
                assertTrue(near.isLocalLocked(key, true));
            }
            finally {
                lock.unlock();
            }

            assertTrue(near.isLocalLocked(key, false));
            assertTrue(near.isLocalLocked(key, true));
        }
        catch (Throwable t) {
            error("Test failed.", t);

            throw t;
        }
        finally {
            lock.unlock();
        }

        assertFalse(near(0).isLockedNearOnly(key));
        assertFalse(near.isLocalLocked(key, true));
    }

    /** @throws Exception If failed. */
    public void testTransactionSingleGetLocalAffinity() throws Exception {
        checkTransactionSingleGet(2);
    }

    /** @throws Exception If failed. */
    public void testTransactionSingleGetRemoteAffinity() throws Exception {
        checkTransactionSingleGet(1);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkTransactionSingleGet(int key) throws Exception {
        IgniteCache<Integer, String> cache = jcache(0);

        String val = Integer.toString(key);

        cache.put(key, val);

        assertEquals(val, dhtPeek(0, key));
        assertEquals(val, dhtPeek(1, key));

        assertNull(near(0).peekEx(key));
        assertNull(near(1).peekEx(key));

        if (transactional()) {

            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                // Simple transaction get.
                assertEquals(val, cache.get(key));

                tx.commit();
            }
        }
        else
            assertEquals(val, cache.get(key));

        assertEquals(val, dhtPeek(0, key));
        assertEquals(val, dhtPeek(1, key));

        assertNull(near(0).peekEx(key));
        assertNull(near(1).peekEx(key));
    }

    /** @throws Exception If failed. */
    public void testTransactionSingleGetRemoveLocalAffinity() throws Exception {
        checkTransactionSingleGetRemove(2);
    }

    /** @throws Exception If failed. */
    public void testTransactionSingleGetRemoveRemoteAffinity() throws Exception {
        checkTransactionSingleGetRemove(1);
    }

    /**
     * @param key Key
     * @throws Exception If failed.
     */
    public void checkTransactionSingleGetRemove(int key) throws Exception {
        IgniteCache<Object, Object> cache = jcache(0);

        String val = Integer.toString(key);

        cache.put(key, val);

        assertEquals(val, dhtPeek(0, key));
        assertEquals(val, dhtPeek(1, key));

        assertNull(near(0).peekEx(key));
        assertNull(near(1).peekEx(key));

        if (transactional()) {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                // Read.
                assertEquals(val, cache.get(key));

                // Remove.
                assertTrue(cache.remove(key));

                tx.commit();
            }
        }
        else {
            // Read.
            assertEquals(val, cache.get(key));

            // Remove.
            assertTrue(cache.remove(key));
        }

        assertNull(dhtPeek(0, key));
        assertNull(dhtPeek(1, key));

        assertNull(near(0).peekEx(key));
        assertNull(near(1).peekEx(key));
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Integer, String> {
        /** Map. */
        private ConcurrentMap<Integer, String> map = new ConcurrentHashMap<>();

        /** Create flag. */
        private volatile boolean create = true;

        /**
         *
         */
        void reset() {
            map.clear();

            create = true;
        }

        /** @param create Create flag. */
        void create(boolean create) {
            this.create = create;
        }

        /** @return Create flag. */
        boolean isCreate() {
            return create;
        }

        /**
         * @param key Key.
         * @return Value.
         */
        String value(Integer key) {
            return map.get(key);
        }

        /** @return {@code True} if empty. */
        boolean isEmpty() {
            return map.isEmpty();
        }

        /** {@inheritDoc} */
        @Override public String load(Integer key) {
            if (!create)
                return map.get(key);

            String s = map.putIfAbsent(key, key.toString());

            return s == null ? key.toString() : s;
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends String> e) {
            map.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            map.remove(key);
        }
    }
}