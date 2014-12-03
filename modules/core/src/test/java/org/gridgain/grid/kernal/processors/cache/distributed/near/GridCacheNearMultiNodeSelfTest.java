/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

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
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Grid counter. */
    private AtomicInteger cntr = new AtomicInteger(0);

    /** Affinity based on node index mode. */
    private GridCacheAffinityFunction aff = new GridCacheModuloAffinityFunction(GRID_CNT, BACKUPS);

    /** Debug flag for mappings. */
    private boolean mapDebug = true;

    /**
     *
     */
    public GridCacheNearMultiNodeSelfTest() {
        super(false /* don't start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);
        spi.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        cfg.setDiscoverySpi(spi);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setStore(store);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinity(aff);
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setBackups(BACKUPS);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, cntr.getAndIncrement()));

        return cfg;
    }

    /**
     * @return Atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
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
            assert cache(grid(i)).size() == 0 : "Near cache size is not zero for grid: " + i;
            assert dht(grid(i)).size() == 0 : "DHT cache size is not zero for grid: " + i;

            assert cache(grid(i)).isEmpty() : "Near cache is not empty for grid: " + i;
            assert dht(grid(i)).isEmpty() : "DHT cache is not empty for grid: " + i;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SizeReplaceableByIsEmpty"})
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            cache(grid(i)).removeAll();

            assertEquals("Near cache size is not zero for grid: " + i, 0, cache(grid(i)).size());
            assertEquals("DHT cache size is not zero for grid: " + i, 0, dht(grid(i)).size());

            assert cache(grid(i)).isEmpty() : "Near cache is not empty for grid: " + i;
            assert dht(grid(i)).isEmpty() : "DHT cache is not empty for grid: " + i;
        }

        store.reset();

        for (int i = 0; i < GRID_CNT; i++) {
            GridCacheTx tx = grid(i).cache(null).tx();

            if (tx != null) {
                error("Ending zombie transaction: " + tx);

                tx.close();
            }
        }
    }

    /**
     * @param g Grid.
     * @return Near cache.
     */
    private GridCacheProjection<Integer, String> cache(Grid g) {
        return g.cache(null);
    }

    /**
     * @param g Grid.
     * @return Dht cache.
     */
    @SuppressWarnings({"unchecked", "TypeMayBeWeakened"})
    private GridDhtCacheAdapter<Integer, String> dht(Grid g) {
        return ((GridNearCacheAdapter)((GridKernal)g).internalCache()).dht();
    }

    /**
     * @param g Grid.
     * @return Dht cache.
     */
    @SuppressWarnings({"unchecked", "TypeMayBeWeakened"})
    private GridNearCacheAdapter<Integer, String> near(Grid g) {
        return (GridNearCacheAdapter)((GridKernal)g).internalCache();
    }

    /**
     * @param idx Index.
     * @return Affinity.
     */
    private GridCacheAffinity<Object> affinity(int idx) {
        return grid(idx).cache(null).affinity();
    }

    /** @param cnt Count. */
    private void mapKeys(int cnt) {
        GridCacheAffinity<Object> aff = affinity(0);

        for (int key = 1; key <= cnt; key++) {
            Integer part = aff.partition(key);

            assert part != null;

            Collection<GridNode> nodes = aff.mapPartitionToPrimaryAndBackups(part);

            GridNode primary = F.first(nodes);

            Set<Integer> keys = primary.addMetaIfAbsent("primary", F.<Integer>newSet());

            assert keys != null;

            keys.add(key);

            if (mapDebug)
                info("Mapped key to primary node [key=" + key + ", node=" + U.toShortString(primary));

            for (GridNode n : nodes) {
                if (n != primary) {
                    keys = n.addMetaIfAbsent("backups", F.<Integer>newSet());

                    assert keys != null;

                    keys.add(key);

                    if (mapDebug)
                        info("Mapped key to backup node [key=" + key + ", node=" + U.toShortString(n));
                }
            }
        }
    }

    /** Test mappings. */
    public void testMappings() {
        mapDebug = false;

        int cnt = 100000;

        mapKeys(cnt);

        for (GridNode n : grid(0).nodes()) {
            Set<Integer> primary = n.meta("primary");
            Set<Integer> backups = n.meta("backups");

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
    @Nullable private GridNode primaryNode(Integer key) {
        return affinity(0).mapKeyToNode(key);
    }

    /**
     * @param key Key.
     * @return Primary node for the key.
     */
    @Nullable private Grid primaryGrid(Integer key) {
        GridNode n = affinity(0).mapKeyToNode(key);

        assert n != null;

        return G.grid(n.id());
    }

    /**
     * @param key Key.
     * @return Primary node for the key.
     */
    @Nullable private Collection<Grid> backupGrids(Integer key) {
        Collection<GridNode> nodes = affinity(0).mapKeyToPrimaryAndBackups(key);

        Collection<GridNode> backups = CU.backups(nodes);

        return F.viewReadOnly(backups,
            new C1<GridNode, Grid>() {
                @Override public Grid apply(GridNode node) {
                    return G.grid(node.id());
                }
            });
    }

    /** @throws Exception If failed. */
    public void testReadThroughAndPut() throws Exception {
        Integer key = 100000;

        Grid primary;
        Grid backup;

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
        GridNode loc = grid(0).localNode();

        info("Local node: " + U.toShortString(loc));

        GridCache<Integer, String> near = cache(0);

        int cnt = 10;

        mapKeys(cnt);

        for (int key = 1; key <= cnt; key++) {
            String s = near.get(key);

            info("Read key [key=" + key + ", val=" + s + ']');

            assert s != null;
        }

        info("Read all keys.");

        for (int key = 1; key <= cnt; key++) {
            GridNode n = primaryNode(key);

            info("Primary node for key [key=" + key + ", node=" + U.toShortString(n) + ']');

            assert n != null;

            assert ((Collection)n.meta("primary")).contains(key);

            GridCache<Integer, String> dhtCache = dht(G.grid(n.id()));

            String s = dhtCache.peek(key);

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
        GridCache<Integer, String> near = cache(0);

        if (transactional()) {
            try (GridCacheTx tx = near.txStart(OPTIMISTIC, REPEATABLE_READ, 0, 0)) {
                near.putx(2, "2");

                String s = near.put(3, "3");

                assertNotNull(s);
                assertEquals("3", s);

                assertEquals("2", near.get(2));
                assertEquals("3", near.get(3));

                assertNull(dht(primaryGrid(2)).peek(2));
                assertNotNull(dht(primaryGrid(3)).peek(3));

                tx.commit();
            }
        }
        else {
            near.putx(2, "2");

            String s = near.put(3, "3");

            assertNotNull(s);
            assertEquals("3", s);
        }

        assertEquals("2", near.peek(2));
        assertEquals("3", near.peek(3));

        assertEquals("2", dht(primaryGrid(2)).peek(2));
        assertEquals("3", dht(primaryGrid(3)).peek(3));

        assertEquals(2, near.size());
        assertEquals(2, near.size());
    }

    /** @throws Exception If failed. */
    public void testNoTransactionSinglePutx() throws Exception {
        GridCache<Integer, String> near = cache(0);

        near.putx(2, "2");

        assertEquals("2", near.peek(2));
        assertEquals("2", near.get(2));

        assertEquals("2", dht(primaryGrid(2)).peek(2));

        assertEquals(1, near.size());
        assertEquals(1, near.size());

        assertEquals(1, dht(primaryGrid(2)).size());
    }

    /** @throws Exception If failed. */
    public void testNoTransactionSinglePut() throws Exception {
        GridCache<Integer, String> near = cache(0);

        // There should be a not-null previously mapped value because
        // we use a store implementation that just returns values which
        // are string representations of requesting integer keys.
        String s = near.put(3, "3");

        assertNotNull(s);
        assertEquals("3", s);

        assertEquals("3", near.peek(3));
        assertEquals("3", near.get(3));

        Grid primaryGrid = primaryGrid(3);

        assert primaryGrid != null;

        info("Primary grid for key 3: " + U.toShortString(primaryGrid.cluster().localNode()));

        assertEquals("3", dht(primaryGrid).peek(3));

        assertEquals(1, near.size());
        assertEquals(1, near.size());

        assertEquals(1, dht(primaryGrid).size());

        // Check backup nodes.
        Collection<Grid> backups = backupGrids(3);

        assert backups != null;

        for (Grid b : backups) {
            info("Backup grid for key 3: " + U.toShortString(b.cluster().localNode()));

            assertEquals("3", dht(b).peek(3));

            assertEquals(1, dht(b).size());
        }
    }

    /** @throws Exception If failed. */
    public void testNoTransactionWriteThrough() throws Exception {
        GridCache<Integer, String> near = cache(0);

        near.putx(2, "2");

        String s = near.put(3, "3");

        assertNotNull(s);
        assertEquals("3", s);

        assertEquals("2", near.peek(2));
        assertEquals("3", near.peek(3));

        assertEquals("2", near.get(2));
        assertEquals("3", near.get(3));

        assertEquals("2", dht(primaryGrid(2)).peek(2));
        assertEquals("3", dht(primaryGrid(3)).peek(3));

        assertEquals(2, near.size());
        assertEquals(2, near.size());
    }

    /**
     * Test Optimistic repeatable read write-through.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ConstantConditions"})
    public void testPessimisticWriteThrough() throws Exception {
        GridCache<Integer, String> near = cache(0);

        if (transactional()) {
            try (GridCacheTx tx = near.txStart(PESSIMISTIC, REPEATABLE_READ, 0, 0)) {
                assertTrue(near.putx(2, "2"));

                String s = near.put(3, "3");

                assertNotNull(s);
                assertEquals("3", s);

                assertEquals("2", near.peek(2));
                assertEquals("3", near.peek(3));

                assertNotNull(dht(primaryGrid(3)).peek(3, F.asList(GLOBAL)));

                // This assertion no longer makes sense as we bring the value over whenever
                // there is a version mismatch.
                // assertNull(dht(primaryGrid(2)).peek(2, new GridCachePeekMode[]{GLOBAL}));

                tx.commit();
            }
        }
        else {
            assertTrue(near.putx(2, "2"));

            String s = near.put(3, "3");

            assertNotNull(s);
            assertEquals("3", s);
        }

        assertEquals("2", near.peek(2));
        assertEquals("3", near.peek(3));

        assertEquals("2", dht(primaryGrid(2)).peek(2));
        assertEquals("3", dht(primaryGrid(3)).peek(3));

        assertEquals(2, near.size());
        assertEquals(2, near.size());
    }

    /** @throws Exception If failed. */
    public void testConcurrentOps() throws Exception {
        // Don't create missing values.
        store.create(false);

        GridCache<Integer, String> near = cache(0);

        int key = 1;

        assertTrue(near.putxIfAbsent(key, "1"));
        assertFalse(near.putxIfAbsent(key, "1"));
        assertEquals("1", near.putIfAbsent(key, "2"));

        assertEquals("1", near.peek(key));
        assertEquals(1, near.size());
        assertEquals(1, near.size());

        assertEquals("1", near.replace(key, "2"));
        assertEquals("2", near.peek(key));

        assertTrue(near.replacex(key, "2"));

        assertEquals("2", near.peek(key));
        assertEquals(1, near.size());
        assertEquals(1, near.size());

        assertTrue(near.remove(key, "2"));

        assertEquals(0, near.size());
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
        GridCache<Integer, String> cache = cache(0);

        String val = Integer.toString(key);

        cache.put(key, val);

        GridNearCacheAdapter<Integer, String> near0 = near(0);
        GridNearCacheAdapter<Integer, String> near1 = near(1);

        GridDhtCacheAdapter<Integer, String> dht0 = dht(0);
        GridDhtCacheAdapter<Integer, String> dht1 = dht(1);

        assertNull(near0.peekNearOnly(key));
        assertNull(near1.peekNearOnly(key));

        assertEquals(val, dht0.peek(key));
        assertEquals(val, dht1.peek(key));
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
    @Nullable private GridNearCacheEntry<Integer, String> nearEntry(int idx, int key) {
        return this.<Integer, String>near(idx).peekExx(key);
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

        GridCache<Integer, String> cache = cache(0);

        String val = Integer.toString(key);

        Collection<GridNode> affNodes = cache.affinity().mapKeyToPrimaryAndBackups(key);

        info("Affinity for key [nodeId=" + U.nodeIds(affNodes) + ", key=" + key + ']');

        assertEquals(2, affNodes.size());

        GridNode primary = F.first(affNodes);

        assertNotNull(primary);

        info("Primary local: " + primary.isLocal());

        cache.lock(key, 0);

        try {
            long topVer = grid(0).topologyVersion();

            GridNearCacheEntry<Integer, String> nearEntry1 = nearEntry(0, key);

            info("Peeked entry after lock [hash=" + hash(nearEntry1) + ", nearEntry=" + nearEntry1 + ']');

            assertNotNull(nearEntry1);
            assertTrue("Invalid near entry: " + nearEntry1, nearEntry1.valid(topVer));

            assertTrue(cache.isLocked(key));
            assertTrue(cache.isLockedByThread(key));

            cache.put(key, val);

            GridNearCacheEntry<Integer, String> nearEntry2 = nearEntry(0, key);

            info("Peeked entry after put [hash=" + hash(nearEntry1) + ", nearEntry=" + nearEntry2 + ']');

            assert nearEntry1 == nearEntry2;

            assertNotNull(nearEntry2);
            assertTrue("Invalid near entry [hash=" + nearEntry2, nearEntry2.valid(topVer));

            assertEquals(val, cache.peek(key));
            assertEquals(val, dht(0).peek(key));
            assertEquals(val, dht(1).peek(key));

            GridNearCacheEntry<Integer, String> nearEntry3 = nearEntry(0, key);

            info("Peeked entry after peeks [hash=" + hash(nearEntry1) + ", nearEntry=" + nearEntry3 + ']');

            assert nearEntry2 == nearEntry3;

            assertNotNull(nearEntry3);
            assertTrue("Invalid near entry: " + nearEntry3, nearEntry3.valid(topVer));

            assertNotNull(near(0).peekNearOnly(key));
            assertNull(near(1).peekNearOnly(key));

            assertEquals(val, cache.get(key));
            assertEquals(val, cache.remove(key));

            assertNull(cache.peek(key));
            assertNull(dht(primaryGrid(key)).peek(key));

            assertTrue(cache.isLocked(key));
            assertTrue(cache.isLockedByThread(key));
        }
        finally {
            cache.unlock(key);
        }

        assertNull(near(0).peekNearOnly(key));
        assertNull(near(1).peekNearOnly(key));

        assertFalse(near(0).isLockedNearOnly(key));
        assertFalse(cache.isLockedByThread(key));
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

        GridCache<Integer, String> near = cache(0);

        String val = Integer.toString(key);

        near.lock(key, 0);

        try {
            near.put(key, val);

            assertEquals(val, near.peek(key));
            assertEquals(val, dht(primaryGrid(key)).peek(key));

            assertTrue(near.isLocked(key));
            assertTrue(near.isLockedByThread(key));

            near.lock(key, 0); // Reentry.

            try {
                assertEquals(val, near.get(key));
                assertEquals(val, near.remove(key));

                assertNull(near.peek(key));
                assertNull(dht(primaryGrid(key)).peek(key));

                assertTrue(near.isLocked(key));
                assertTrue(near.isLockedByThread(key));
            }
            finally {
                near.unlock(key);
            }

            assertTrue(near.isLocked(key));
            assertTrue(near.isLockedByThread(key));
        }
        catch (Throwable t) {
            error("Test failed.", t);

            throw t;
        }
        finally {
            near.unlock(key);
        }

        assertFalse(near(0).isLockedNearOnly(key));
        assertFalse(near.isLockedByThread(key));
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
        GridCache<Integer, String> cache = cache(0);

        String val = Integer.toString(key);

        cache.put(key, val);

        assertEquals(val, dht(0).peek(key));
        assertEquals(val, dht(1).peek(key));

        assertNull(near(0).peekNearOnly(key));
        assertNull(near(1).peekNearOnly(key));

        if (transactional()) {

            try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                // Simple transaction get.
                assertEquals(val, cache.get(key));

                tx.commit();
            }
        }
        else
            assertEquals(val, cache.get(key));

        assertEquals(val, dht(0).peek(key));
        assertEquals(val, dht(1).peek(key));

        assertNull(near(0).peekNearOnly(key));
        assertNull(near(1).peekNearOnly(key));
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
        GridCache<Integer, String> cache = cache(0);

        String val = Integer.toString(key);

        cache.put(key, val);

        assertEquals(val, dht(0).peek(key));
        assertEquals(val, dht(1).peek(key));

        assertNull(near(0).peekNearOnly(key));
        assertNull(near(1).peekNearOnly(key));

        if (transactional()) {
            try (GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                // Read.
                assertEquals(val, cache.get(key));

                // Remove.
                assertTrue(cache.removex(key));

                tx.commit();
            }
        }
        else {
            // Read.
            assertEquals(val, cache.get(key));

            // Remove.
            assertTrue(cache.removex(key));
        }

        assertNull(dht(0).peek(key));
        assertNull(dht(1).peek(key));

        assertNull(near(0).peekNearOnly(key));
        assertNull(near(1).peekNearOnly(key));
    }

    /**
     *
     */
    private static class TestStore extends GridCacheStoreAdapter<Integer, String> {
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

        /**
         * @param key Key.
         * @return {@code True} if has value.
         */
        boolean hasValue(Integer key) {
            return map.containsKey(key);
        }

        /** @return {@code True} if empty. */
        boolean isEmpty() {
            return map.isEmpty();
        }

        /** {@inheritDoc} */
        @Override public String load(GridCacheTx tx, Integer key) throws GridException {
            if (!create)
                return map.get(key);

            String s = map.putIfAbsent(key, key.toString());

            return s == null ? key.toString() : s;
        }

        /** {@inheritDoc} */
        @Override public void put(GridCacheTx tx, Integer key, @Nullable String val)
            throws GridException {
            map.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheTx tx, Integer key) throws GridException {
            map.remove(key);
        }
    }
}
