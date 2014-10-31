/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Multi-node tests for partitioned cache.
 */
public class GridCachePartitionedMultiNodeFullApiSelfTest extends GridCachePartitionedFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cc = super.cacheConfiguration(gridName);

        cc.setPreloadMode(SYNC);

        return cc;
    }

    /**
     * @return Affinity nodes for this cache.
     */
    public Collection<GridNode> affinityNodes() {
        return grid(0).nodes();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllRemoveAll() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info(">>>>> Grid" + i + ": " + grid(i).localNode().id());

        Map<Integer, Integer> putMap = new LinkedHashMap<>();

        int size = 100;

        for (int i = 0; i < size; i++)
            putMap.put(i, i * i);

        GridCache<Object, Object> prj0 = grid(0).cache(null);
        GridCache<Object, Object> prj1 = grid(1).cache(null);

        prj0.putAll(putMap);

        prj1.removeAll(putMap.keySet());

        for (int i = 0; i < size; i++) {
            assertNull(prj0.get(i));
            assertNull(prj1.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPutAll() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info(">>>>> Grid" + i + ": " + grid(i).localNode().id());

        Map<Integer, Integer> putMap = new LinkedHashMap<>();

        int size = 100;

        for (int i = 0; i < size; i++)
            putMap.put(i, i);

        GridCache<Object, Object> prj0 = grid(0).cache(null);
        GridCache<Object, Object> prj1 = grid(1).cache(null);

        prj0.putAll(putMap);

        for (int i = 0; i < size; i++) {
            assertEquals(i, prj0.get(i));
            assertEquals(i, prj1.get(i));
        }

        for (int i = 0; i < size; i++)
            putMap.put(i, i * i);

        info(">>> Before second put.");

        prj1.putAll(putMap);

        info(">>> After second put.");

        for (int i = 0; i < size; i++) {
            assertEquals(i * i, prj0.get(i));
            assertEquals(i * i, prj1.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutDebug() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info(">>>>> Grid" + i + ": " + grid(i).localNode().id());

        int size = 10;

        GridCache<Object, Object> prj0 = grid(0).cache(null);

        for (int i = 0; i < size; i++) {
            info("Putting value [i=" + i + ']');

            assertNull(prj0.put(i, i));

            info("Finished putting value [i=" + i + ']');
        }

        for (int i = 0; i < gridCount(); i++) {
            assertEquals(0, context(i).tm().idMapSize());

            GridCache<Object, Object> cache = grid(i).cache(null);
            GridNode node = grid(i).localNode();

            for (int k = 0; k < size; k++) {
                if (cache.affinity().isPrimaryOrBackup(node, k))
                    assertEquals("Check failed for node: " + node.id(), k, cache.peek(k));
            }
        }

        for (int i = 0; i < size; i++) {
            info("Putting value 2 [i=" + i + ']');

            assertEquals(i, prj0.putIfAbsent(i, i * i));

            info("Finished putting value 2 [i=" + i + ']');
        }

        for (int i = 0; i < size; i++)
            assertEquals(i, prj0.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnswapShort() throws Exception {
        final AtomicInteger swapEvts = new AtomicInteger(0);
        final AtomicInteger unswapEvts = new AtomicInteger(0);

        for (int i = 0; i < gridCount(); i++) {
            grid(i).events().localListen(new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    info("Received event: " + evt);

                    switch (evt.type()) {
                        case EVT_CACHE_OBJECT_SWAPPED:
                            swapEvts.incrementAndGet();

                            break;
                        case EVT_CACHE_OBJECT_UNSWAPPED:
                            unswapEvts.incrementAndGet();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);
        }

        cache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            GridCacheEntry<String, Integer> e = cache(i).entry("key");

            if (e.backup()) {
                assert cache(i).evict("key") : "Entry was not evicted [idx=" + i + ", entry=" +
                    (nearEnabled() ? dht(i).entryEx("key") : colocated(i).entryEx("key")) + ']';

                assert cache(i).peek("key") == null;

                assert cache(i).get("key") == 1;

                assert swapEvts.get() == 1 : "Swap events: " + swapEvts.get();

                assert unswapEvts.get() == 1 : "Unswap events: " + unswapEvts.get();

                break;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekPartitionedModes() throws Exception {
        cache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            boolean nearEnabled = nearEnabled(cache(i));

            Integer nearPeekVal = nearEnabled ? 1 : null;

            GridCache<String, Integer> c = cache(i);

            GridCacheEntry<String, Integer> e = c.entry("key");

            if (e.backup()) {
                assertNull("NEAR_ONLY for cache: " + i, e.peek(F.asList(NEAR_ONLY)));
                assertEquals((Integer)1, e.peek(F.asList(PARTITIONED_ONLY)));

                assertNull(c.peek("key", F.asList(NEAR_ONLY)));

                assertEquals((Integer)1, c.peek("key", F.asList(PARTITIONED_ONLY)));
            }
            else if (!e.primary() && !e.backup()) {
                assertEquals((Integer)1, e.get());

                assertEquals(nearPeekVal, e.peek(Arrays.asList(NEAR_ONLY)));

                assert e.peek(Arrays.asList(PARTITIONED_ONLY)) == null;

                assertEquals(nearPeekVal, c.peek("key", Arrays.asList(NEAR_ONLY)));

                assert c.peek("key", Arrays.asList(PARTITIONED_ONLY)) == null;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekAsyncPartitionedModes() throws Exception {
        cache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            boolean nearEnabled = nearEnabled(cache(i));

            Integer nearPeekVal = nearEnabled ? 1 : null;

            GridCache<String, Integer> c = cache(i);

            GridCacheEntry<String, Integer> e = c.entry("key");

            if (e.backup()) {
                assert e.peek(F.asList(NEAR_ONLY)) == null;

                assert e.peek(Arrays.asList(PARTITIONED_ONLY)) == 1;

                assert c.peek("key", Arrays.asList(NEAR_ONLY)) == null;

                assert c.peek("key", Arrays.asList(PARTITIONED_ONLY)) == 1;
            }
            else if (!e.primary() && !e.backup()) {
                assert e.get() == 1;

                assertEquals(nearPeekVal, e.peek(Arrays.asList(NEAR_ONLY)));

                assert e.peek(Arrays.asList(PARTITIONED_ONLY)) == null;

                assertEquals(nearPeekVal, c.peek("key", Arrays.asList(NEAR_ONLY)));

                assert c.peek("key", Arrays.asList(PARTITIONED_ONLY)) == null;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testNearDhtKeySize() throws Exception {
        List<String> keys = new ArrayList<>(5);

        info("Generating keys for test...");

        for (int i = 0; i < 5; i++) {
            while (true) {
                String key = UUID.randomUUID().toString();

                if (cache().affinity().isPrimary(grid(0).localNode(), key) &&
                    cache().affinity().isBackup(grid(1).localNode(), key)) {
                    keys.add(key);

                    assertTrue(cache(0).putx(key, i));

                    break;
                }
            }
        }

        info("Finished generating keys for test.");

        assertEquals(Integer.valueOf(0), cache(2).get(keys.get(0)));
        assertEquals(Integer.valueOf(1), cache(2).get(keys.get(1)));

        assertEquals(0, cache(0).nearSize());
        assertEquals(5, cache(0).size() - cache(0).nearSize());

        assertEquals(0, cache(1).nearSize());
        assertEquals(5, cache(1).size() - cache(1).nearSize());

        assertEquals(nearEnabled() ? 2 : 0, cache(2).nearSize());
        assertEquals(0, cache(2).size() - cache(2).nearSize());

        GridBiPredicate<String, Integer> prjFilter = new P2<String, Integer>() {
            @Override public boolean apply(String key, Integer val) {
                return val >= 1 && val <= 3;
            }
        };

        assertEquals(0, cache(0).projection(prjFilter).nearSize());
        assertEquals(3, cache(0).projection(prjFilter).size() - cache(0).projection(prjFilter).nearSize());

        assertEquals(0, cache(1).projection(prjFilter).nearSize());
        assertEquals(3, cache(1).projection(prjFilter).size() - cache(1).projection(prjFilter).nearSize());

        assertEquals(nearEnabled() ? 1 : 0, cache(2).projection(prjFilter).nearSize());
        assertEquals(0, cache(2).projection(prjFilter).size() - cache(2).projection(prjFilter).nearSize());
    }

    /** {@inheritDoc} */
    @Override public void testLockAsyncWithTimeoutEntry() throws Exception {
        // No-op, since all cases are tested separately.
    }

    /**
     * @throws Exception In case of error.
     */
    public void testLockAsyncWithTimeoutEntryPrimary() throws Exception {
        if (!lockingEnabled())
            return;

        GridNode node = CU.primary(cache().affinity().mapKeyToPrimaryAndBackups("key"));

        assert node != null;

        info("Node: " + node);

        GridCache<String, Integer> cache = G.grid(node.id()).cache(null);

        checkLockAsyncWithTimeoutEntry("key", cache);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testLockAsyncWithTimeoutEntryBackup() throws Exception {
        if (!lockingEnabled())
            return;

        GridNode node = F.first(CU.backups(cache().affinity().mapKeyToPrimaryAndBackups("key")));

        assert node != null;

        info("Node: " + node);

        GridCache<String, Integer> cache = G.grid(node.id()).cache(null);

        checkLockAsyncWithTimeoutEntry("key", cache);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testLockAsyncWithTimeoutEntryNear() throws Exception {
        if (!lockingEnabled())
            return;

        Collection<GridNode> affNodes = cache().affinity().mapKeyToPrimaryAndBackups("key");

        GridNode node = null;

        for (GridNode n : grid(0).nodes()) {
            if (!affNodes.contains(n)) {
                node = n;

                break;
            }
        }

        assert node != null;

        info("Node: " + node);

        GridCache<String, Integer> cache = G.grid(node.id()).cache(null);

        checkLockAsyncWithTimeoutEntry("key", cache);
    }

    /**
     * @param key Key.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkLockAsyncWithTimeoutEntry(String key, GridCacheProjection<String,Integer> cache)
        throws Exception {
        assert lockingEnabled();

        final GridCacheEntry<String, Integer> e = cache.entry(key);

        info("Entry [e=" + e + ", primary=" + e.primary() + ", backup=" + e.backup() + ']');

        assert e != null;

        assert !e.isLocked();

        final AtomicBoolean locked = new AtomicBoolean(e.lock(0));

        try {
            assert e.isLocked();
            assert e.isLockedByThread();

            assert cache.isLockedByThread(key);

            final CountDownLatch syncLatch = new CountDownLatch(1);

            GridCompute comp = compute(dfltGrid.cluster().forLocal()).enableAsync();

            comp.call(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        syncLatch.countDown();

                        GridFuture<Boolean> f = e.lockAsync(15000);

                        try {
                            f.get(100);

                            fail();
                        } catch (GridFutureTimeoutException ex) {
                            info("Caught expected exception: " + ex);
                        }

                        try {
                            assert f.get();
                        } finally {
                            e.unlock();

                            locked.set(false);
                        }

                        return true;
                    }
                });

            GridFuture<Boolean> f = comp.future();

            syncLatch.await();

            // Make 1st future in closure fail.
            Thread.sleep(300);

            assert e.isLocked();
            assert e.isLockedByThread();

            cache.unlock(key);

            locked.set(false);

            assert f.get();

            for (int i = 0; i < 100; i++)
                if (cache.isLocked(key) || cache.isLockedByThread(key))
                    U.sleep(10);
                else
                    break;

            assert !cache.isLocked(key);
            assert !cache.isLockedByThread(key);
        }
        finally {
            if (locked.get())
                e.unlock();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinity() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            info("Grid " + i + ": " + grid(i).localNode().id());

        final Object affKey = new Object() {
            @Override public boolean equals(Object obj) {
                return obj == this;
            }

            @Override public int hashCode() {
                return 1;
            }
        };

        Object key = new Object() {
            /** */
            @SuppressWarnings("UnusedDeclaration")
            @GridCacheAffinityKeyMapped
            private final Object key0 = affKey;

            @Override public boolean equals(Object obj) {
                return obj == this;
            }

            @Override public int hashCode() {
                return 2;
            }
        };

        info("All affinity nodes: " + affinityNodes());

        GridCache<Object, Object> cache = grid(0).cache(null);

        info("Cache affinity nodes: " + cache.affinity().mapKeyToPrimaryAndBackups(key));

        GridCacheAffinity<Object> aff = cache.affinity();

        Collection<GridNode> nodes = aff.mapKeyToPrimaryAndBackups(key);

        info("Got nodes from affinity: " + nodes);

        assertEquals(cacheMode() == PARTITIONED ? 2 : affinityNodes().size(), nodes.size());

        GridNode primary = F.first(nodes);
        GridNode backup = F.last(nodes);

        assertNotSame(primary, backup);

        GridNode other = null;

        for (int i = 0; i < gridCount(); i++) {
            GridNode node = grid(i).localNode();

            if (!node.equals(primary) && !node.equals(backup)) {
                other = node;

                break;
            }
        }

        assertNotSame(other, primary);
        assertNotSame(other, backup);

        assertNotNull(primary);
        assertNotNull(backup);
        assertNotNull(other);

        assertTrue(cache.affinity().isPrimary(primary, key));
        assertFalse(cache.affinity().isBackup(primary, key));
        assertTrue(cache.affinity().isPrimaryOrBackup(primary, key));

        assertFalse(cache.affinity().isPrimary(backup, key));
        assertTrue(cache.affinity().isBackup(backup, key));
        assertTrue(cache.affinity().isPrimaryOrBackup(backup, key));

        assertFalse(cache.affinity().isPrimary(other, key));

        if (cacheMode() == PARTITIONED) {
            assertFalse(cache.affinity().isBackup(other, key));
            assertFalse(cache.affinity().isPrimaryOrBackup(other, key));
        }
    }
}
