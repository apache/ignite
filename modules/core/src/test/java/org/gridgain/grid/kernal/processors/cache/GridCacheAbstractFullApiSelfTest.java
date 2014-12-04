/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.spi.swapspace.inmemory.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.testframework.GridTestUtils.*;

/**
 * Full API cache test.
 */
public abstract class GridCacheAbstractFullApiSelfTest extends GridCacheAbstractSelfTest {
    /** Increment closure for transform operations. */
    public static final IgniteClosure<Integer,Integer> INCR_CLOS = new IgniteClosure<Integer, Integer>() {
        @Override public Integer apply(Integer old) {
            return old == null ? 1 : old + 1;
        }
    };

    /** Remove closure for transform operations. */
    public static final IgniteClosure<Integer,Integer> RMV_CLOS = new IgniteClosure<Integer, Integer>() {
        @Override public Integer apply(Integer e) {
            return null;
        }
    };

    /** Dflt grid. */
    protected Ignite dfltIgnite;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return true;
    }

    /**
     * @return {@code True} if values should be stored off-heap.
     */
    protected boolean offHeapValues() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (offHeapValues())
            cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        if (offHeapValues()) {
            ccfg.setQueryIndexEnabled(false);
            ccfg.setMemoryMode(GridCacheMemoryMode.OFFHEAP_VALUES);
            ccfg.setOffHeapMaxMemory(0);
        }

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();


        for (int i = 0; i < gridCount(); i++)
            info("Grid " + i + ": " + grid(i).localNode().id());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        assertEquals("Primary key set: " + cache().primaryKeySet(), 0, cache().primaryKeySet().size());
        assertEquals(0, cache().primarySize());
        assertEquals(0, cache().primaryKeySet().size());
        assertEquals(0, cache().size());
        assertEquals(0, cache().globalSize());
        assertEquals(0, cache().globalPrimarySize());

        super.beforeTest();

        assertEquals(0, cache().primarySize());
        assertEquals(0, cache().primaryKeySet().size());
        assertEquals(0, cache().size());
        assertEquals(0, cache().globalSize());
        assertEquals(0, cache().globalPrimarySize());

        dfltIgnite = grid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        assertEquals(0, cache().primarySize());
        assertEquals(0, cache().size());
        assertEquals(0, cache().globalSize());
        assertEquals(0, cache().globalPrimarySize());

        dfltIgnite = null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testSize() throws Exception {
        assert cache().isEmpty();

        int size = 10;

        Map<String, Integer> map = new HashMap<>(size);

        for (int i = 0; i < size; i++)
            map.put("key" + i, i);

        // Put in primary nodes to avoid near readers which will prevent entry from being cleared.
        Map<ClusterNode, Collection<String>> mapped = grid(0).mapKeysToNodes(null, map.keySet());

        for (int i = 0; i < gridCount(); i++) {
            Collection<String> keys = mapped.get(grid(i).localNode());

            if (!F.isEmpty(keys)) {
                for (String key : keys)
                    cache(i).put(key, map.get(key));
            }
        }

        map.remove("key0");

        mapped = grid(0).mapKeysToNodes(null, map.keySet());

        for (int i = 0; i < gridCount(); i++) {
            // Will actually delete entry from map.
            CU.invalidate(cache(i), "key0");

            assertNull("Failed check for grid: " + i, cache(i).peek("key0"));

            Collection<String> keysCol = mapped.get(grid(i).localNode());

            assert !cache(i).isEmpty() || F.isEmpty(keysCol);
        }

        for (int i = 0; i < gridCount(); i++) {
            GridCacheContext<String, Integer> ctx = context(i);

            int sum = 0;

            for (String key : map.keySet())
                if (ctx.affinity().localNode(key, ctx.discovery().topologyVersion()))
                    sum++;

            assertEquals("Incorrect key size on cache #" + i, sum, cache(i).keySet().size());
            assertEquals("Incorrect key size on cache #" + i, sum, cache(i).size());
        }

        for (int i = 0; i < gridCount(); i++) {
            Collection<String> keysCol = mapped.get(grid(i).localNode());

            assertEquals("Failed check for grid: " + i, !F.isEmpty(keysCol) ? keysCol.size() : 0,
                cache(i).primarySize());
        }

        int globalPrimarySize = map.size();

        for (int i = 0; i < gridCount(); i++)
            assertEquals(globalPrimarySize, cache(i).globalPrimarySize());

        int times = 1;

        if (cacheMode() == REPLICATED)
            times = gridCount();
        else if (cacheMode() == PARTITIONED)
            times = Math.min(gridCount(), cache().configuration().getBackups() + 1);

        int globalSize = globalPrimarySize * times;

        for (int i = 0; i < gridCount(); i++)
            assertEquals(globalSize, cache(i).globalSize());
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsKey() throws Exception {
        cache().put("testContainsKey", 1);

        checkContainsKey(true, "testContainsKey");
        checkContainsKey(false, "testContainsKeyWrongKey");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsKeyFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 100);

        checkProjectionContainsKey(true, "key1", F.<GridCacheEntry<String, Integer>>alwaysTrue());
        checkProjectionContainsKey(false, "key1", F.<GridCacheEntry<String, Integer>>alwaysFalse());
        checkProjectionContainsKey(false, "key1", gte100);
        checkProjectionContainsKey(true, "key2", gte100);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsValue() throws Exception {
        cache().put("key", 1);

        checkContainsValue(true, 1);
        checkContainsValue(false, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveInExplicitLocks() throws Exception {
        if (lockingEnabled()) {
            GridCache<String, Integer> cache = cache();

            cache.put("a", 1);

            cache.lockAll(F.asList("a", "b", "c", "d"), 0);

            try {
                cache.remove("a");

                // Make sure single-key operation did not remove lock.
                cache.putAll(F.asMap("b", 2, "c", 3, "d", 4));
            }
            finally {
                cache.unlockAll(F.asList("a", "b", "c", "d"));
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testContainsValueFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 100);

        checkProjectionContainsValue(true, 1, F.<GridCacheEntry<String, Integer>>alwaysTrue());
        checkProjectionContainsValue(false, 1, F.<GridCacheEntry<String, Integer>>alwaysFalse());
        checkProjectionContainsValue(false, 1, gte100);
        checkProjectionContainsValue(true, 100, gte100);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testForEach() throws Exception {
        Collection<Integer> vals1 = F.asList(123, 73);
        Collection<Integer> vals2 = F.asList(1567, 332);

        Collection<Integer> vals = new ArrayList<>();

        vals.addAll(vals1);
        vals.addAll(vals2);

        for (Integer val : vals)
            cache().put("key" + val, val);

        assert cache().tx() == null;

        AtomicInteger sum1 = new AtomicInteger(0);

        if (cacheMode() == PARTITIONED && !nearEnabled()) {
            for (int i = 0; i < gridCount(); i++)
                cache(i).forEach(new SumVisitor(sum1));

            // Multiply by 2 if more than one node. In this case backup values are also included.
            assertEquals(F.sumInt(vals) * (gridCount() == 1 ? 1 : 2), sum1.get());
        }
        else {
            cache().forEach(new SumVisitor(sum1));

            assertEquals(F.sumInt(vals), sum1.get());
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testForAll() throws Exception {
        assert cache().forAll(F.<GridCacheEntry<String, Integer>>alwaysTrue());
        assert cache().isEmpty() || !cache().forAll(F.<GridCacheEntry<String, Integer>>alwaysFalse());

        cache().put("key1", 100);
        cache().put("key2", 101);
        cache().put("key3", 200);
        cache().put("key4", 201);

        assert cache().forAll(gte100);
    }

    /**
     * @throws GridException If failed.
     */
    public void testAtomicOps() throws GridException {
        GridCacheProjectionEx<String, Integer> c = (GridCacheProjectionEx<String, Integer>)cache();

        final int cnt = 10;

        for (int i = 0; i < cnt; i++)
            assertNull(c.putIfAbsent("k" + i, i));

        for (int i = 0; i < cnt; i++) {
            boolean wrong = i % 2 == 0;

            String key = "k" + i;

            GridCacheReturn<Integer> res = c.replacex(key, wrong ? i + 1 : i, -1);

            assertTrue(wrong != res.success());

            if (wrong)
                assertEquals(c.get(key), res.value());
        }

        for (int i = 0; i < cnt; i++) {
            boolean success = i % 2 != 0;

            String key = "k" + i;

            GridCacheReturn<Integer> res = c.removex(key, -1);

            assertTrue(success == res.success());

            if (!success)
                assertEquals(c.get(key), res.value());
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGet() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);

        assert cache().get("key1") == 1;
        assert cache().get("key2") == 2;
        assert cache().get("wrongKey") == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 100);

        assert cache().projection(gte100).get("key1") == null;
        assert cache().projection(gte100).get("key2") == 100;
        assert cache().projection(gte100).get("key50") == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAsync() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);

        IgniteFuture<Integer> fut1 = cache().getAsync("key1");
        IgniteFuture<Integer> fut2 = cache().getAsync("key2");
        IgniteFuture<Integer> fut3 = cache().getAsync("wrongKey");

        assert fut1.get() == 1;
        assert fut2.get() == 2;
        assert fut3.get() == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAsyncFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 100);

        IgniteFuture<Integer> fut1 = cache().projection(gte100).getAsync("key1");
        IgniteFuture<Integer> fut2 = cache().projection(gte100).getAsync("key2");

        assert fut1.get() == null;
        assert fut2.get() == 100;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAll() throws Exception {
        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        cache().put("key1", 1);
        cache().put("key2", 2);

        if (tx != null)
            tx.commit();

        assert cache().getAll(null).isEmpty();
        assert cache().getAll(Collections.<String>emptyList()).isEmpty();

        Map<String, Integer> map1 = cache().getAll(F.asList("key1", "key2", "key9999"));

        info("Retrieved map1: " + map1);

        assert 2 == map1.size() : "Invalid map: " + map1;

        assertEquals(1, (int)map1.get("key1"));
        assertEquals(2, (int)map1.get("key2"));
        assertNull(map1.get("key9999"));

        Map<String, Integer> map2 = cache().getAll(F.asList("key1", "key2", "key9999"));

        info("Retrieved map2: " + map2);

        assert 2 == map2.size() : "Invalid map: " + map2;

        assertEquals(1, (int)map2.get("key1"));
        assertEquals(2, (int)map2.get("key2"));
        assertNull(map2.get("key9999"));

        // Now do the same checks but within transaction.
        if (txEnabled()) {
            tx = cache().txStart();

            assert cache().getAll(null).isEmpty();
            assert cache().getAll(Collections.<String>emptyList()).isEmpty();

            map1 = cache().getAll(F.asList("key1", "key2", "key9999"));

            info("Retrieved map1: " + map1);

            assert 2 == map1.size() : "Invalid map: " + map1;

            assertEquals(1, (int)map1.get("key1"));
            assertEquals(2, (int)map1.get("key2"));
            assertNull(map1.get("key9999"));

            map2 = cache().getAll(F.asList("key1", "key2", "key9999"));

            info("Retrieved map2: " + map2);

            assert 2 == map2.size() : "Invalid map: " + map2;

            assertEquals(1, (int)map2.get("key1"));
            assertEquals(2, (int)map2.get("key2"));
            assertNull(map2.get("key9999"));

            tx.commit();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAllWithNulls() throws Exception {
        GridCache<String, Integer> cache = cache();

        Collection<String> c = new LinkedList<>();

        c.add("key1");
        c.add(null);

        cache.getAll(c);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAllDuplicates() throws Exception {
        cache().getAll(F.asList("key1", "key1", "key1"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAllDuplicatesTx() throws Exception {
        if (txEnabled()) {
            try (GridCacheTx ignored = cache().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache().getAll(F.asList("key1", "key1", "key1"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTxNonExistingKey() throws Exception {
        if (txEnabled()) {
            try (GridCacheTx ignored = cache().txStart()) {
                cache().get("key999123");
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAllFilteredTx() throws Exception {
        if (txEnabled()) {
            GridCacheTx tx = cache().txStart();

            cache().put("key1", 100);
            cache().put("key2", 101);
            cache().put("key3", 200);
            cache().put("key4", 201);

            tx.commit();

            tx.close();

            tx = cache().txStart(PESSIMISTIC, REPEATABLE_READ);

            try {
                Map<String, Integer> map1 = cache().projection(gte200).getAll(
                    F.asList("key1", "key2", "key3", "key4", "key9999"));

                assertEquals("Invalid map size: " + map1, 2, map1.size());

                assert map1.get("key1") == null;
                assert map1.get("key2") == null;
                assert map1.get("key3") == 200;
                assert map1.get("key4") == 201;

                map1 = cache().projection(gte200).getAll(F.asList("key1", "key2"));

                assertEquals("Invalid map size: " + map1, 0, map1.size());

                assert map1.get("key1") == null;
                assert map1.get("key2") == null;
                assert map1.get("key3") == null;
                assert map1.get("key4") == null;

                map1 = cache().projection(gte200).getAll(F.asList("key1", "key2"));

                assertEquals("Invalid map size: " + map1, 0, map1.size());

                assert map1.get("key1") == null;
                assert map1.get("key2") == null;
                assert map1.get("key3") == null;
                assert map1.get("key4") == null;
            }
            finally {
                tx.close();
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAllFiltered() throws Exception {
        cache().put("key1", 100);
        cache().put("key2", 101);
        cache().put("key3", 200);
        cache().put("key4", 201);

        Map<String, Integer> map1 = cache().projection(gte100).getAll(
            F.asList("key1", "key2", "key3", "key4", "key9999"));

        assertEquals("Invalid map size: " + map1, 4, map1.size());

        assert map1.get("key1") == 100;
        assert map1.get("key2") == 101;
        assert map1.get("key3") == 200;
        assert map1.get("key4") == 201;

        Map<String, Integer> map2 = cache().projection(gte200).getAll(
            F.asList("key1", "key2", "key3", "key4", "key9999"));

        assertEquals("Invalid map size: " + map2, 2, map2.size());

        assert map2.get("key1") == null;
        assert map2.get("key2") == null;
        assert map2.get("key3") == 200;
        assert map2.get("key4") == 201;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAllAsync() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);

        IgniteFuture<Map<String, Integer>> fut1 = cache().getAllAsync(null);
        IgniteFuture<Map<String, Integer>> fut2 = cache().getAllAsync(Collections.<String>emptyList());
        IgniteFuture<Map<String, Integer>> fut3 = cache().getAllAsync(F.asList("key1", "key2"));

        assert fut1.get().isEmpty();
        assert fut2.get().isEmpty();
        assert fut3.get().size() == 2 : "Invalid map: " + fut3.get();
        assert fut3.get().get("key1") == 1;
        assert fut3.get().get("key2") == 2;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGetAllAsyncFiltered() throws Exception {
        cache().put("key1", 100);
        cache().put("key2", 101);
        cache().put("key3", 200);
        cache().put("key4", 201);

        List<String> keys = F.asList("key1", "key2", "key3", "key4");

        IgniteFuture<Map<String, Integer>> fut1 = cache().projection(gte100).getAllAsync(keys);
        IgniteFuture<Map<String, Integer>> fut2 = cache().projection(gte200).getAllAsync(keys);

        assert fut1.get().size() == 4 : "Invalid map: " + fut1.get();
        assert fut1.get().get("key1") == 100;
        assert fut1.get().get("key2") == 101;
        assert fut1.get().get("key3") == 200;
        assert fut1.get().get("key4") == 201;

        assert fut2.get().size() == 2 : "Invalid map: " + fut2.get();
        assert fut2.get().get("key1") == null;
        assert fut2.get().get("key2") == null;
        assert fut2.get().get("key3") == 200;
        assert fut2.get().get("key4") == 201;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPut() throws Exception {
        assert cache().put("key1", 1) == null;
        assert cache().put("key2", 2) == null;

        // Check inside transaction.
        assert cache().get("key1") == 1;
        assert cache().get("key2") == 2;

        // Put again to check returned values.
        assert cache().put("key1", 1) == 1;
        assert cache().put("key2", 2) == 2;

        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");

        assert cache().get("key1") != null;
        assert cache().get("key2") != null;
        assert cache().get("wrong") == null;

        // Check outside transaction.
        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");

        assert cache().get("key1") == 1;
        assert cache().get("key2") == 2;
        assert cache().get("wrong") == null;

        assertEquals((Integer)1, cache().put("key1", 10));
        assertEquals((Integer)2, cache().put("key2", 11));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutTx() throws Exception {
        if (txEnabled()) {
            GridCacheTx tx = cache().txStart();

            assert cache().put("key1", 1) == null;
            assert cache().put("key2", 2) == null;

            // Check inside transaction.
            assert cache().get("key1") == 1;
            assert cache().get("key2") == 2;

            // Put again to check returned values.
            assert cache().put("key1", 1) == 1;
            assert cache().put("key2", 2) == 2;

            checkContainsKey(true, "key1");
            checkContainsKey(true, "key2");

            assert cache().get("key1") != null;
            assert cache().get("key2") != null;
            assert cache().get("wrong") == null;

            tx.commit();

            // Check outside transaction.
            checkContainsKey(true, "key1");
            checkContainsKey(true, "key2");

            assert cache().get("key1") == 1;
            assert cache().get("key2") == 2;
            assert cache().get("wrong") == null;

            assertEquals((Integer)1, cache().put("key1", 10));
            assertEquals((Integer)2, cache().put("key2", 11));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticReadCommitted() throws Exception {
        checkTransform(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticRepeatableRead() throws Exception {
        checkTransform(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticReadCommitted() throws Exception {
        checkTransform(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticRepeatableRead() throws Exception {
        checkTransform(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkTransform(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
        GridCacheProjection<String, Integer> cache = cache();

        cache.put("key2", 1);
        cache.put("key3", 3);


        GridCacheTx tx = txEnabled() ? cache.txStart(concurrency, isolation) : null;

        try {
            cache.transform("key1", INCR_CLOS);
            cache.transform("key2", INCR_CLOS);
            cache.transform("key3", RMV_CLOS);

            if (tx != null)
                tx.commit();
        }
        catch (Exception e) {
            e.printStackTrace();

            throw e;
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull("Failed for cache: " + i, cache(i).peek("key3"));

        cache.remove("key1");
        cache.put("key2", 1);
        cache.put("key3", 3);

        cache.transform("key1", INCR_CLOS);
        cache.transform("key2", INCR_CLOS);
        cache.transform("key3", RMV_CLOS);

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(cache(i).peek("key3"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllOptimisticReadCommitted() throws Exception {
        checkTransformAll(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllOptimisticRepeatableRead() throws Exception {
        checkTransformAll(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllPessimisticReadCommitted() throws Exception {
        checkTransformAll(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllPessimisticRepeatableRead() throws Exception {
        checkTransformAll(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void checkTransformAll(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation)
        throws Exception {
        GridCacheProjection<String, Integer> cache = cache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        if (txEnabled()) {
            CU.inTx(cache, concurrency, isolation, new CIX1<GridCacheProjection<String, Integer>>() {
                @Override
                public void applyx(GridCacheProjection<String, Integer> c) throws GridException {
                    c.transformAll(F.asSet("key1", "key2", "key3"), INCR_CLOS);
                }
            });

            assertEquals((Integer)1, cache.get("key1"));
            assertEquals((Integer)2, cache.get("key2"));
            assertEquals((Integer)4, cache.get("key3"));
        }

        cache.transformAll(F.asSet("key1", "key2", "key3"), RMV_CLOS);

        for (int i = 0; i < gridCount(); i++) {
            assertNull(cache(i).peek("key1"));
            assertNull(cache(i).peek("key2"));
            assertNull(cache(i).peek("key3"));
        }

        cache.remove("key1");
        cache.put("key2", 1);
        cache.put("key3", 3);

        cache.transformAll(F.asMap("key1", INCR_CLOS, "key2", INCR_CLOS, "key3", INCR_CLOS));

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertEquals((Integer)4, cache.get("key3"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAllWithNulls() throws Exception {
        final GridCacheProjection<String, Integer> cache = cache();

        cache.transformAll(null); // This should be no-op.

        {
            Map<String, Integer> m = new HashMap<>(2);

            m.put("key1", 1);
            m.put(null, 2);

            // WARN: F.asMap() doesn't work here, because it will throw NPE.

            cache.putAll(m);
        }

        {
            Map<String, IgniteClosure<Integer, Integer>> tm = new HashMap<>(2);

            tm.put("key1", INCR_CLOS);
            tm.put(null, INCR_CLOS);

            // WARN: F.asMap() doesn't work here, because it will throw NPE.

            cache.transformAll(tm);
        }

        {
            Map<String, IgniteClosure<Integer, Integer>> tm = new HashMap<>(2);

            tm.put("key1", INCR_CLOS);
            tm.put("key2", null);

            // WARN: F.asMap() doesn't work here, because it will throw NPE.

            cache.transformAll(tm);
        }

        cache.transformAll(null, INCR_CLOS); // This should be no-op.

        {
            Set<String> ts = new HashSet<>(3);

            ts.add("key1");
            ts.add(null);

            // WARN: F.asSet() doesn't work here, because it will throw NPE.

            cache.transformAll(ts, INCR_CLOS);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformSequentialOptimisticNoStart() throws Exception {
        checkTransformSequential0(false, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformSequentialPessimisticNoStart() throws Exception {
        checkTransformSequential0(false, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformSequentialOptimisticWithStart() throws Exception {
        checkTransformSequential0(true, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformSequentialPessimisticWithStart() throws Exception {
        checkTransformSequential0(true, PESSIMISTIC);
    }

    /**
     * @param startVal Whether to put value.
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkTransformSequential0(boolean startVal, GridCacheTxConcurrency concurrency)
        throws Exception {
        GridCacheProjection<String, Integer> cache = cache();

        GridCacheTx tx = txEnabled() ? cache.txStart(concurrency, READ_COMMITTED) : null;

        try {
            if (startVal)
                cache.put("key", 2);

            cache.transform("key", INCR_CLOS);
            cache.transform("key", INCR_CLOS);
            cache.transform("key", INCR_CLOS);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        Integer exp = (startVal ? 2 : 0) + 3;

        assertEquals(exp, cache.get("key"));

        for (int i = 0; i < gridCount(); i++) {
            if (cache(i).affinity().isPrimaryOrBackup(grid(i).localNode(), "key"))
                assertEquals(exp, peek(cache(i), "key"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAfterRemoveOptimistic() throws Exception {
        checkTransformAfterRemove(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAfterRemovePessimistic() throws Exception {
        checkTransformAfterRemove(PESSIMISTIC);
    }

    /**
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkTransformAfterRemove(GridCacheTxConcurrency concurrency) throws Exception {
        GridCacheProjection<String, Integer> cache = cache();

        cache.put("key", 4);

        GridCacheTx tx = txEnabled() ? cache.txStart(concurrency, READ_COMMITTED) : null;

        try {
            cache.remove("key");

            cache.transform("key", INCR_CLOS);
            cache.transform("key", INCR_CLOS);
            cache.transform("key", INCR_CLOS);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)3, cache.get("key"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValueGetOptimisticReadCommitted() throws Exception {
        checkTransformReturnValue(false, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValueGetOptimisticRepeatableRead() throws Exception {
        checkTransformReturnValue(false, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValueGetPessimisticReadCommitted() throws Exception {
        checkTransformReturnValue(false, PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValueGetPessimisticRepeatableRead() throws Exception {
        checkTransformReturnValue(false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformReturnValuePutInTx() throws Exception {
        checkTransformReturnValue(true, OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @param put Whether to put value.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkTransformReturnValue(boolean put, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation) throws Exception {
        GridCacheProjection<String, Integer> cache = cache();

        if (!put)
            cache.put("key", 1);

        GridCacheTx tx = txEnabled() ? cache.txStart(concurrency, isolation) : null;

        try {
            if (put)
                cache.put("key", 1);

            cache.transform("key", INCR_CLOS);

            assertEquals((Integer)2, cache.get("key"));

            if (tx != null) {
                // Second get inside tx. Make sure read value is not transformed twice.
                assertEquals((Integer)2, cache.get("key"));

                tx.commit();
            }
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutFiltered() throws Exception {
        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        cache().put("key1", 1, F.<String, Integer>cacheNoPeekValue());
        cache().put("key2", 100, gte100);

        if (tx != null)
            tx.commit();

        checkSize(F.asSet("key1"));

        assert cache().get("key1") == 1;

        Integer i = cache().get("key2");

        assert i == null : "Why not null?: " + i;
    }


    /**
     * @throws Exception In case of error.
     */
    public void testPutAsync() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);

        IgniteFuture<Integer> fut1 = cache().putAsync("key1", 10);
        IgniteFuture<Integer> fut2 = cache().putAsync("key2", 11);

        assertEquals((Integer)1, fut1.get(5000));
        assertEquals((Integer)2, fut2.get(5000));

        assertEquals((Integer)10, cache().get("key1"));
        assertEquals((Integer)11, cache().get("key2"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutAsync0() throws Exception {
        IgniteFuture<Integer> fut1 = cache().putAsync("key1", 0);
        IgniteFuture<Integer> fut2 = cache().putAsync("key2", 1);

        assert fut1.get(5000) == null;
        assert fut2.get(5000) == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformEntry() throws Exception {
        GridCacheEntry<String, Integer> entry = cache().entry("test");

        entry.setValue(1);

        // Make user entry capture cache entry.
        entry.version();

        assertEquals((Integer)1, entry.getValue());

        entry.transform(INCR_CLOS);

        assertEquals((Integer)2, entry.getValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAsync() throws Exception {
        GridCacheProjection<String, Integer> cache = cache();

        cache.put("key2", 1);
        cache.put("key3", 3);

        IgniteFuture<?> fut0 = cache.transformAsync("key1", INCR_CLOS);
        IgniteFuture<?> fut1 = cache.transformAsync("key2", INCR_CLOS);
        IgniteFuture<?> fut2 = cache.transformAsync("key3", RMV_CLOS);

        fut0.get();
        fut1.get();
        fut2.get();

        assertEquals((Integer)1, cache.get("key1"));
        assertEquals((Integer)2, cache.get("key2"));
        assertNull(cache.get("key3"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(cache(i).peek("key3"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformCompute() throws Exception {
        GridCacheProjection<String, Integer> cache = cache();

        IgniteClosure<Integer, IgniteBiTuple<Integer, String>> c;

        c = new IgniteClosure<Integer, IgniteBiTuple<Integer, String>>() {
            @Override public IgniteBiTuple<Integer, String> apply(Integer val) {
                return val == null ? new IgniteBiTuple<>(0, "null") : new IgniteBiTuple<>(val + 1, String.valueOf(val));
            }
        };

        assertEquals("null", cache.transformAndCompute("k0", c));

        assertEquals((Integer)0, cache.get("k0"));

        assertEquals("0", cache.transformAndCompute("k0", c));

        assertEquals((Integer)1, cache.get("k0"));

        cache.put("k1", 1);

        assertEquals("1", cache.transformAndCompute("k1", c));

        assertEquals((Integer)2, cache.get("k1"));

        assertEquals("2", cache.transformAndCompute("k1", c));

        assertEquals((Integer)3, cache.get("k1"));

        c = new IgniteClosure<Integer, IgniteBiTuple<Integer, String>>() {
            @Override public IgniteBiTuple<Integer, String> apply(Integer integer) {
                return new IgniteBiTuple<>(null, null);
            }
        };

        assertNull(cache.transformAndCompute("k1", c));
        assertNull(cache.get("k1"));

        for (int i = 0; i < gridCount(); i++)
            assertNull(cache(i).peek("k1"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutAsyncFiltered() throws Exception {
        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        assert cache().putAsync("key1", 1, gte100).get() == null;
        assert cache().putAsync("key2", 101, F.<String, Integer>cacheNoPeekValue()).get() == null;

        if (tx != null)
            tx.commit();

        checkSize(F.asSet("key2"));

        assert cache().get("key1") == null;
        assert cache().get("key2") == 101;

        assert cache().putAsync("key2", 102, F.<String, Integer>cacheNoPeekValue()).get() == 101;
        assert cache().putAsync("key2", 103, F.<String, Integer>cacheHasPeekValue()).get() == 101;

        checkSize(F.asSet("key2"));

        assert cache().get("key1") == null;
        assert cache().get("key2") == 103;

        if (lockingEnabled()) {
            assert !cache().isLocked("key1");
            assert !cache().isLocked("key2");
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutx() throws Exception {
        if (txEnabled())
            checkPutx(true);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutxNoTx() throws Exception {
        checkPutx(false);
    }

    /**
     * @param inTx Whether to start transaction.
     * @throws Exception If failed.
     */
    private void checkPutx(boolean inTx) throws Exception {
        GridCacheTx tx = inTx ? cache().txStart() : null;

        assert cache().putx("key1", 1);
        assert cache().putx("key2", 2);
        assert !cache().putx("wrong", 3, gte100);

        // Check inside transaction.
        assert cache().get("key1") == 1;
        assert cache().get("key2") == 2;

        if (tx != null)
            tx.commit();

        checkSize(F.asSet("key1", "key2"));

        // Check outside transaction.
        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");
        checkContainsKey(false, "wrong");

        checkContainsValue(true, 1);
        checkContainsValue(true, 2);

        assert cache().get("key1") == 1;
        assert cache().get("key2") == 2;
        assert cache().get("wrong") == null;

        assert cache().putx("key1", 100, F.<String, Integer>cacheContainsPeek(1));
        assert cache().putx("key1", 101, gte100);
        assert !cache().putx("key1", 102, gte200);

        checkContainsValue(false, 1);
        checkContainsValue(true, 101);
        checkContainsValue(true, 2);

        checkSize(F.asSet("key1", "key2"));

        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");
        checkContainsKey(false, "wrong");

        assert cache().get("key1") == 101;
        assert cache().get("key2") == 2;
        assert cache().get("wrong") == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testFiltersOptimistic1() throws Exception {
        checkFilters1(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFiltersPessimistic1() throws Exception {
        checkFilters1(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFiltersOptimistic2() throws Exception {
        checkFilters2(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFiltersPessimistic2() throws Exception {
        checkFilters2(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFiltersOptimistic3() throws Exception {
        checkFilters3(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFiltersPessimistic3() throws Exception {
        checkFilters3(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * Check that empty filter is not overwritten with non-empty.
     *
     * @param concurrency Tx concurrency.
     * @param isolation Tx isolation.
     * @throws Exception If failed.
     */
    private void checkFilters1(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
        cache().putx("key1", 0);

        GridCacheTx tx = txEnabled() ? cache().txStart(concurrency, isolation) : null;

        try {
            assert cache().putx("key1", 100);
            assert cache().putx("key1", 101, gte100);
            assert cache().putx("key1", 1, gte100);

            // Check inside transaction.
            assert cache().get("key1") == 1;

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        // Check outside transaction.
        boolean passed = false;

        for (int i = 0; i < gridCount(); i++)
            passed |= containsKey(cache(i), "key1");

        assert passed;

        assertEquals((Integer)1, cache().get("key1"));
    }

    /**
     * Check that failed filter overwritten with passed.
     *
     * @param concurrency Tx concurrency.
     * @param isolation Tx isolation.
     * @throws Exception If failed.
     */
    private void checkFilters2(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
        cache().putx("key1", 100);

        GridCacheTx tx = txEnabled() ? cache().txStart(concurrency, isolation) : null;

        try {
            cache().put("key1", 101, F.<GridCacheEntry<String, Integer>>alwaysFalse());
            cache().put("key1", 101, F.<GridCacheEntry<String, Integer>>alwaysTrue());

            // Check inside transaction.
            assertEquals((Integer)101, cache().get("key1"));

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        // Check outside transaction.
        boolean passed = false;

        for (int i = 0; i < gridCount(); i++)
            passed |= containsKey(cache(i), "key1");

        assert passed;

        assertEquals((Integer)101, cache().get("key1"));
    }

    /**
     * Check that passed filter is not overwritten with failed.
     *
     * @param concurrency Tx concurrency.
     * @param isolation Tx isolation.
     * @throws Exception If failed.
     */
    private void checkFilters3(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
        cache().putx("key1", 100);

        GridCacheTx tx = txEnabled() ? cache().txStart(concurrency, isolation) : null;

        try {
            assertEquals((Integer)100, cache().put("key1", 101, F.<GridCacheEntry<String, Integer>>alwaysTrue()));

            assertEquals((Integer)101, cache().get("key1"));

            cache().put("key1", 102, F.<GridCacheEntry<String, Integer>>alwaysFalse());

            // Check inside transaction.
            assertEquals((Integer)101, cache().get("key1"));

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        // Check outside transaction.
        boolean passed = false;

        for (int i = 0; i < gridCount(); i++)
            passed |= containsKey(cache(i), "key1");

        assert passed;

        assertEquals((Integer)101, cache().get("key1"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxFiltered() throws Exception {
        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        try {
            cache().putx("key1", 1, F.<String, Integer>cacheHasPeekValue());
            cache().putx("key2", 100, F.<String, Integer>cacheNoPeekValue());

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        checkSize(F.asSet("key2"));

        assert cache().get("key1") == null;
        assert cache().get("key2") == 100;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxAsync() throws Exception {
        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        cache().put("key2", 1);

        IgniteFuture<Boolean> fut1 = cache().putxAsync("key1", 10);
        IgniteFuture<Boolean> fut2 = cache().putxAsync("key2", 11);

        IgniteFuture<GridCacheTx> f = tx == null ? null : tx.commitAsync();

        assert fut1.get();
        assert fut2.get();

        assert f == null || f.get().state() == COMMITTED;

        checkSize(F.asSet("key1", "key2"));

        assert cache().get("key1") == 10;
        assert cache().get("key2") == 11;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutxAsyncFiltered() throws Exception {
        IgniteFuture<Boolean> f1 = cache().putxAsync("key1", 1);
        IgniteFuture<Boolean> f2 = cache().putxAsync("key1", 101, F.<String, Integer>cacheHasPeekValue());
        IgniteFuture<Boolean> f3 = cache().putxAsync("key2", 2);
        IgniteFuture<Boolean> f4 = cache().putxAsync("key2", 202, F.<String, Integer>cacheHasPeekValue());
        IgniteFuture<Boolean> f5 = cache().putxAsync("key1", 1, F.<String, Integer>cacheNoPeekValue());
        IgniteFuture<Boolean> f6 = cache().putxAsync("key2", 2, F.<String, Integer>cacheNoPeekValue());

        assert f1.get() : "Invalid future1: " + f1;
        assert f2.get() : "Invalid future2: " + f2;
        assert f3.get() : "Invalid future3: " + f3;
        assert f4.get() : "Invalid future4: " + f4;

        assert !f5.get() : "Invalid future5: " + f5;
        assert !f6.get() : "Invalid future6: " + f6;

        checkSize(F.asSet("key1", "key2"));

        assertEquals((Integer)101, cache().get("key1"));
        assertEquals((Integer)202, cache().get("key2"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutAll() throws Exception {
        Map<String, Integer> map = F.asMap("key1", 1, "key2", 2);

        cache().putAll(map);

        checkSize(F.asSet("key1", "key2"));

        assert cache().get("key1") == 1;
        assert cache().get("key2") == 2;

        map.put("key1", 10);
        map.put("key2", 20);

        cache().putAll(map);

        checkSize(F.asSet("key1", "key2"));

        assert cache().get("key1") == 10;
        assert cache().get("key2") == 20;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutAllWithNulls() throws Exception {
        final GridCache<String, Integer> cache = cache();

        {
            Map<String, Integer> m = new HashMap<>(2);

            m.put("key1", 1);
            m.put(null, 2);

            // WARN: F.asMap() doesn't work here, because it will throw NPE.

            cache.putAll(m);

            assertNotNull(cache.get("key1"));
        }

        {
            Map<String, Integer> m = new HashMap<>(2);

            m.put("key3", 3);
            m.put("key4", null);

            cache.putAll(m);

            assertNotNull(cache.get("key3"));
            assertNull(cache.get("key4"));
        }

        assertThrows(log, new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                cache.put("key1", null);

                return null;
            }
        }, NullPointerException.class, A.NULL_MSG_PREFIX);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutAllFiltered() throws Exception {
        Map<String, Integer> map = F.asMap("key1", 1, "key2", 2);

        cache().putAll(map, F.<String, Integer>cacheNoPeekValue());

        checkSize(F.asSet("key1", "key2"));

        assert cache().get("key1") == 1;
        assert cache().get("key2") == 2;

        map.put("key1", 10);
        map.put("key2", 20);

        map.put("key3", 3);

        cache().putAll(map, F.<String, Integer>cacheNoPeekValue());

        checkSize(F.asSet("key1", "key2", "key3"));

        assert cache().get("key1") == 1;
        assert cache().get("key2") == 2;
        assert cache().get("key3") == 3;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutAllAsync() throws Exception {
        Map<String, Integer> map = F.asMap("key1", 1, "key2", 2);

        IgniteFuture<?> f1 = cache().putAllAsync(map);

        map.put("key1", 10);
        map.put("key2", 20);

        IgniteFuture<?> f2 = cache().putAllAsync(map);

        f2.get();
        f1.get();

        checkSize(F.asSet("key1", "key2"));

        assert cache().get("key1") == 10;
        assert cache().get("key2") == 20;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutAllAsyncFiltered() throws Exception {
        Map<String, Integer> map1 = F.asMap("key1", 1, "key2", 2);

        IgniteFuture<?> f1 = cache().putAllAsync(map1, F.<String, Integer>cacheNoPeekValue());

        Map<String, Integer> map2 = F.asMap("key1", 10, "key2", 20, "key3", 3);

        IgniteFuture<?> f2 = cache().putAllAsync(map2, F.<String, Integer>cacheNoPeekValue());

        f2.get();
        f1.get();

        checkSize(F.asSet("key1", "key2", "key3"));

        assert cache().get("key1") == 1;
        assert cache().get("key2") == 2;
        assert cache().get("key3") == 3;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutIfAbsent() throws Exception {
        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        try {
            assert cache().putIfAbsent("key", 1) == null;

            assert cache().get("key") != null;
            assert cache().get("key") == 1;

            assert cache().putIfAbsent("key", 2) != null;
            assert cache().putIfAbsent("key", 2) == 1;

            assert cache().get("key") != null;
            assert cache().get("key") == 1;

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache().putIfAbsent("key", 2) != null;

        for (int i = 0; i < gridCount(); i++) {
            info("Peek on node [i=" + i + ", id=" + grid(i).localNode().id() + ", val=" +
                grid(i).cache(null).peek("key") + ']');
        }

        assertEquals((Integer)1, cache().putIfAbsent("key", 2));

        assert cache().get("key") != null;
        assert cache().get("key") == 1;

        // Check swap.
        cache().put("key2", 1);

        assertTrue(cache().evict("key2"));

        assertEquals((Integer)1, cache().putIfAbsent("key2", 3));

        // Check db.
        putToStore("key3", 3);

        assertEquals((Integer)3, cache().putIfAbsent("key3", 4));

        assertEquals((Integer)1, cache().get("key2"));
        assertEquals((Integer)3, cache().get("key3"));

        cache().evict("key2");
        cache().clear("key3");

        // Same checks inside tx.
        tx = txEnabled() ? cache().txStart() : null;

        try {
            assertEquals((Integer)1, cache().putIfAbsent("key2", 3));
            assertEquals((Integer)3, cache().putIfAbsent("key3", 4));

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache().get("key2"));
            assertEquals((Integer)3, cache().get("key3"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutIfAbsentAsync() throws Exception {
        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        try {
            IgniteFuture<Integer> fut1 = cache().putIfAbsentAsync("key", 1);

            assert fut1.get() == null;
            assert cache().get("key") != null && cache().get("key") == 1;

            IgniteFuture<Integer> fut2 = cache().putIfAbsentAsync("key", 2);

            assert fut2.get() != null && fut2.get() == 1;
            assert cache().get("key") != null && cache().get("key") == 1;

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        // Check swap.
        cache().put("key2", 1);

        assertTrue(cache().evict("key2"));

        assertEquals((Integer)1, cache().putIfAbsentAsync("key2", 3).get());

        // Check db.
        putToStore("key3", 3);

        assertEquals((Integer)3, cache().putIfAbsentAsync("key3", 4).get());

        cache().evict("key2");
        cache().clear("key3");

        // Same checks inside tx.
        tx = txEnabled() ? cache().txStart() : null;

        try {
            assertEquals((Integer)1, cache().putIfAbsentAsync("key2", 3).get());
            assertEquals((Integer)3, cache().putIfAbsentAsync("key3", 4).get());

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache().get("key2"));
            assertEquals((Integer)3, cache().get("key3"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutxIfAbsent() throws Exception {
        assertNull(cache().get("key"));
        assert cache().putxIfAbsent("key", 1);
        assert cache().get("key") != null && cache().get("key") == 1;
        assert !cache().putxIfAbsent("key", 2);
        assert cache().get("key") != null && cache().get("key") == 1;

        // Check swap.
        cache().put("key2", 1);

        assertTrue(cache().evict("key2"));

        assertFalse(cache().putxIfAbsent("key2", 3));

        // Check db.
        putToStore("key3", 3);

        assertFalse(cache().putxIfAbsent("key3", 4));

        cache().evict("key2");
        cache().clear("key3");

        // Same checks inside tx.
        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        try {
            assertFalse(cache().putxIfAbsent("key2", 3));
            assertFalse(cache().putxIfAbsent("key3", 4));

            if (tx != null)
                tx.commit();

            assertEquals((Integer)1, cache().get("key2"));
            assertEquals((Integer)3, cache().get("key3"));
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutxIfAbsentAsync() throws Exception {
        if (txEnabled())
            checkPutxIfAbsentAsync(true);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutxIfAbsentAsyncNoTx() throws Exception {
        checkPutxIfAbsentAsync(false);
    }

    /**
     * @param  inTx In tx flag.
     * @throws Exception If failed.
     */
    private void checkPutxIfAbsentAsync(boolean inTx) throws Exception {
        IgniteFuture<Boolean> fut1 = cache().putxIfAbsentAsync("key", 1);

        assert fut1.get();
        assert cache().get("key") != null && cache().get("key") == 1;

        IgniteFuture<Boolean> fut2 = cache().putxIfAbsentAsync("key", 2);

        assert !fut2.get();
        assert cache().get("key") != null && cache().get("key") == 1;

        // Check swap.
        cache().put("key2", 1);

        assertTrue(cache().evict("key2"));

        assertFalse(cache().putxIfAbsentAsync("key2", 3).get());

        // Check db.
        putToStore("key3", 3);

        assertFalse(cache().putxIfAbsentAsync("key3", 4).get());

        cache().evict("key2");
        cache().clear("key3");

        // Same checks inside tx.
        GridCacheTx tx = inTx ? cache().txStart() : null;

        try {
            assertFalse(cache().putxIfAbsentAsync("key2", 3).get());
            assertFalse(cache().putxIfAbsentAsync("key3", 4).get());

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assertEquals((Integer)1, cache().get("key2"));
        assertEquals((Integer)3, cache().get("key3"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutxIfAbsentAsyncConcurrent() throws Exception {
        IgniteFuture<Boolean> fut1 = cache().putxIfAbsentAsync("key1", 1);
        IgniteFuture<Boolean> fut2 = cache().putxIfAbsentAsync("key2", 2);

        assert fut1.get();
        assert fut2.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplace() throws Exception {
        cache().put("key", 1);

        assert cache().get("key") == 1;

        info("key 1 -> 2");

        assert cache().replace("key", 2) == 1;

        assert cache().get("key") == 2;

        assert cache().replace("wrong", 0) == null;

        assert cache().get("wrong") == null;

        info("key 0 -> 3");

        assert !cache().replace("key", 0, 3);

        assert cache().get("key") == 2;

        info("key 0 -> 3");

        assert !cache().replace("key", 0, 3);

        assert cache().get("key") == 2;

        info("key 2 -> 3");

        assert cache().replace("key", 2, 3);

        assert cache().get("key") == 3;

        info("evict key");

        cache().evict("key");

        info("key 3 -> 4");

        assert cache().replace("key", 3, 4);

        assert cache().get("key") == 4;

        putToStore("key2", 5);

        info("key2 5 -> 6");

        assert cache().replace("key2", 5, 6);

        for (int i = 0; i < gridCount(); i++) {
            info("Peek key on grid [i=" + i + ", nodeId=" + grid(i).localNode().id() +
                ", peekVal=" + grid(i).cache(null).peek("key") + ']');

            info("Peek key2 on grid [i=" + i + ", nodeId=" + grid(i).localNode().id() +
                ", peekVal=" + grid(i).cache(null).peek("key2") + ']');
        }

        assertEquals((Integer)6, cache().get("key2"));

        cache().evict("key");
        cache().clear("key2");

        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        try {
            assert cache().replace("key", 4, 5);
            assert cache().replace("key2", 6, 7);

            if (tx != null)
                tx.commit();

            assert cache().get("key") == 5;
            assert cache().get("key2") == 7;
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplacex() throws Exception {
        cache().put("key", 1);

        assert cache().get("key") == 1;

        assert cache().replacex("key", 2);

        assert cache().get("key") == 2;

        assert !cache().replacex("wrong", 2);

        cache().evict("key");

        assert cache().replacex("key", 4);

        assert cache().get("key") == 4;

        putToStore("key2", 5);

        assert cache().replacex("key2", 6);

        assertEquals((Integer)6, cache().get("key2"));

        cache().evict("key");
        cache().clear("key2");

        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        try {
            assert cache().replacex("key", 5);
            assert cache().replacex("key2", 7);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache().get("key") == 5;
        assert cache().get("key2") == 7;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplaceAsync() throws Exception {
        cache().put("key", 1);

        assert cache().get("key") == 1;

        assert cache().replaceAsync("key", 2).get() == 1;

        assert cache().get("key") == 2;

        assert cache().replaceAsync("wrong", 0).get() == null;

        assert cache().get("wrong") == null;

        assert !cache().replaceAsync("key", 0, 3).get();

        assert cache().get("key") == 2;

        assert !cache().replaceAsync("key", 0, 3).get();

        assert cache().get("key") == 2;

        assert cache().replaceAsync("key", 2, 3).get();

        assert cache().get("key") == 3;

        cache().evict("key");

        assert cache().replaceAsync("key", 3, 4).get();

        assert cache().get("key") == 4;

        putToStore("key2", 5);

        assert cache().replaceAsync("key2", 5, 6).get();

        assertEquals((Integer)6, cache().get("key2"));

        cache().evict("key");
        cache().clear("key2");

        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        try {
            assert cache().replaceAsync("key", 4, 5).get();
            assert cache().replaceAsync("key2", 6, 7).get();

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache().get("key") == 5;
        assert cache().get("key2") == 7;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplacexAsync() throws Exception {
        cache().put("key", 1);

        assert cache().get("key") == 1;

        assert cache().replacexAsync("key", 2).get();

        U.debug(log, "Finished replace.");

        assertEquals((Integer)2, cache().get("key"));

        assert !cache().replacexAsync("wrong", 2).get();

        cache().evict("key");

        assert cache().replacexAsync("key", 4).get();

        assert cache().get("key") == 4;

        putToStore("key2", 5);

        assert cache().replacexAsync("key2", 6).get();

        assert cache().get("key2") == 6;

        cache().evict("key");
        cache().clear("key2");

        GridCacheTx tx = txEnabled() ? cache().txStart() : null;

        try {
            assert cache().replacexAsync("key", 5).get();
            assert cache().replacexAsync("key2", 7).get();

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        assert cache().get("key") == 5;
        assert cache().get("key2") == 7;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemove() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);

        assert !cache().remove("key1", 0);
        assert cache().get("key1") != null && cache().get("key1") == 1;
        assert cache().remove("key1", 1);
        assert cache().get("key1") == null;
        assert cache().remove("key2") == 2;
        assert cache().get("key2") == null;
        assert cache().remove("key2") == null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeletedEntriesFlag() throws Exception {
        if (cacheMode() != LOCAL && cacheMode() != REPLICATED) {
            int cnt = 3;

            for (int i = 0; i < cnt; i++)
                cache().put(String.valueOf(i), i);

            for (int i = 0; i < cnt; i++)
                cache().remove(String.valueOf(i));

            for (int g = 0; g < gridCount(); g++) {
                for (int i = 0; i < cnt; i++) {
                    String key = String.valueOf(i);

                    GridCacheContext<String, Integer> cctx = context(g);

                    GridCacheEntryEx<String, Integer> entry = cctx.isNear() ? cctx.near().dht().peekEx(key) :
                        cctx.cache().peekEx(key);

                    if (cache().affinity().mapKeyToPrimaryAndBackups(key).contains(grid(g).localNode())) {
                        assertNotNull(entry);
                        assertTrue(entry.deleted());
                    }
                    else
                        assertNull(entry);
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveLoad() throws Exception {
        int cnt = 10;

        Collection<String> keys = new ArrayList<>();

        for (int i = 0; i < cnt; i++)
            keys.add(String.valueOf(i));

        cache().removeAll(keys);

        for (String key : keys)
            putToStore(key, Integer.parseInt(key));

        for (int g = 0; g < gridCount(); g++)
            grid(g).cache(null).loadCache(null, 0);

        for (int g = 0; g < gridCount(); g++) {
            for (int i = 0; i < cnt; i++) {
                String key = String.valueOf(i);

                if (cache().affinity().mapKeyToPrimaryAndBackups(key).contains(grid(g).localNode()))
                    assertEquals((Integer)i, cache(g).peek(key));
                else
                    assertNull(cache(g).peek(key));
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 100);

        assert cache().remove("key1", gte100) == 1;
        assert cache().get("key1") != null && cache().get("key1") == 1;
        assert cache().remove("key2", gte100) == 100;
        assert cache().get("key2") == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAsync() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);

        assert !cache().removeAsync("key1", 0).get();
        assert cache().get("key1") != null && cache().get("key1") == 1;
        assert cache().removeAsync("key1", 1).get();
        assert cache().get("key1") == null;
        assert cache().removeAsync("key2").get() == 2;
        assert cache().get("key2") == null;
        assert cache().removeAsync("key2").get() == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAsyncFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 100);

        assert cache().removeAsync("key1", gte100).get() == 1;
        assert cache().get("key1") != null && cache().get("key1") == 1;
        assert cache().removeAsync("key2", gte100).get() == 100;
        assert cache().get("key2") == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemovex() throws Exception {
        cache().put("key1", 1);

        assert cache().removex("key1");
        assert cache().get("key1") == null;
        assert !cache().removex("key1");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemovexFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 100);

        assert !cache().removex("key1", gte100);
        assert cache().get("key1") != null && cache().get("key1") == 1;
        assert cache().removex("key2", gte100);
        assert cache().get("key2") == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemovexAsync() throws Exception {
        cache().put("key1", 1);

        assert cache().removexAsync("key1").get();
        assert cache().get("key1") == null;
        assert !cache().removexAsync("key1").get();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemovexAsyncFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 100);

        assert !cache().removexAsync("key1", gte100).get();
        assert cache().get("key1") != null && cache().get("key1") == 1;
        assert cache().removexAsync("key2", gte100).get();
        assertNull(cache().get("key2"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAll() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);
        cache().put("key3", 3);

        checkSize(F.asSet("key1", "key2", "key3"));

        cache().removeAll(F.asList("key1", "key2"));

        checkSize(F.asSet("key3"));

        checkContainsKey(false, "key1");
        checkContainsKey(false, "key2");
        checkContainsKey(true, "key3");

        // Put values again.
        cache().put("key1", 1);
        cache().put("key2", 2);
        cache().put("key3", 3);

        cache().removeAll();

        assert cache().isEmpty();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllWithNulls() throws Exception {
        GridCache<String, Integer> cache = cache();

        Collection<String> c = new LinkedList<>();

        c.add("key1");
        c.add(null);

        cache.removeAll(c);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllDuplicates() throws Exception {
        cache().removeAll(Arrays.asList("key1", "key1", "key1"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllDuplicatesTx() throws Exception {
        if (txEnabled()) {
            try (GridCacheTx tx = cache().txStart()) {
                cache().removeAll(Arrays.asList("key1", "key1", "key1"));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllEmpty() throws Exception {
        cache().removeAll();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);
        cache().put("key3", 100);
        cache().put("key4", 101);
        cache().put("key5", 102);

        checkSize(F.asSet("key1", "key2", "key3", "key4", "key5"));

        cache().removeAll(F.asList("key2", "key3", "key4"), gte100);

        checkSize(F.asSet("key1", "key2", "key5"));

        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");
        checkContainsKey(true, "key5");

        checkContainsKey(false, "key3");
        checkContainsKey(false, "key4");

        cache().put("key6", 200);
        cache().put("key7", 201);

        checkSize(F.asSet("key1", "key2", "key5", "key6", "key7"));

        for (int i = 0; i < gridCount(); i++)
            cache(i).removeAll(gte200);

        checkSize(F.asSet("key1", "key2", "key5"));

        checkContainsKey(false, "key6");
        checkContainsKey(false, "key7");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllAsync() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);
        cache().put("key3", 3);

        checkSize(F.asSet("key1", "key2", "key3"));

        cache().removeAllAsync(F.asList("key1", "key2")).get();

        checkSize(F.asSet("key3"));

        checkContainsKey(false, "key1");
        checkContainsKey(false, "key2");
        checkContainsKey(true, "key3");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testRemoveAllAsyncFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);
        cache().put("key3", 100);
        cache().put("key4", 101);
        cache().put("key5", 102);

        checkSize(F.asSet("key1", "key2", "key3", "key4", "key5"));

        cache().removeAllAsync(F.asList("key2", "key3", "key4"), gte100).get();

        checkSize(F.asSet("key1", "key2", "key5"));

        checkContainsKey(true, "key1");
        checkContainsKey(true, "key2");
        checkContainsKey(false, "key3");
        checkContainsKey(false, "key4");
        checkContainsKey(true, "key5");

        cache().put("key6", 200);
        cache().put("key7", 201);

        checkSize(F.asSet("key1", "key2", "key5", "key6", "key7"));

        for (int i = 0; i < gridCount(); i++)
            cache(i).removeAllAsync(gte200).get();

        checkSize(F.asSet("key1", "key2", "key5"));

        checkContainsKey(false, "key6");
        checkContainsKey(false, "key7");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testKeySet() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);
        cache().put("key3", 3);

        Collection<String> keys = new HashSet<>();

        for (int i = 0; i < gridCount(); i++)
            keys.addAll(cache(i).keySet());

        assert keys.size() == 3;
        assert keys.contains("key1");
        assert keys.contains("key2");
        assert keys.contains("key3");
        assert !keys.contains("wrongKey");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testKeySetFiltered() throws Exception {
        if (offheapTiered(cache()))
            return;

        cache().put("key1", 1);
        cache().put("key2", 100);
        cache().put("key3", 101);

        Collection<String> keys = new HashSet<>();

        for (int i = 0; i < gridCount(); i++)
            keys.addAll(cache(i).projection(gte100).keySet());

        assert keys.size() == 2;
        assert !keys.contains("key1");
        assert keys.contains("key2");
        assert keys.contains("key3");
    }

    /**
     * @throws Exception In case of error.
     */
    public void testValues() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 2);
        cache().put("key3", 3);

        Collection<Integer> vals = new HashSet<>();

        for (int i = 0; i < gridCount(); i++)
            vals.addAll(cache(i).values());

        assert vals.size() == 3;
        assert vals.contains(1);
        assert vals.contains(2);
        assert vals.contains(3);
        assert !vals.contains(0);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testValuesFiltered() throws Exception {
        cache().put("key1", 1);
        cache().put("key2", 100);
        cache().put("key3", 101);

        Collection<Integer> vals = new HashSet<>();

        for (int i = 0; i < gridCount(); i++)
            vals.addAll(cache(i).projection(gte100).values());

        assert vals.size() == 2;
        assert !vals.contains(1);
        assert vals.contains(100);
        assert vals.contains(101);
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReload() throws Exception {
        String key = "testReload";

        GridCache<String, Integer> cache = primaryCache(key);

        assertNull(cache.peek(key));

        cache.put(key, 1);

        assertEquals((Integer)1, cache.peek(key));

        cache.clear(key);

        assertNull(cache.peek(key));

        assertEquals((Integer)1, cache.reload(key));
        assertEquals((Integer)1, cache.peek(key));
    }

    /**
     *
     * @throws Exception In case of error.
     */
    public void testReloadAsync() throws Exception {
        String key = "testReloadAsync";

        GridCache<String, Integer> cache = primaryCache(key);

        assertNull(cache.get(key));

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        cache.clearAll();

        assertNull(cache.peek(key));

        assertEquals((Integer)1, cache.reloadAsync(key).get());

        assertEquals((Integer)1, cache.peek(key));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReloadFiltered() throws Exception {
        GridCache<String, Integer> cache = primaryCache("key");

        assertNull(cache.get("key"));

        cache.put("key", 1);

        assertEquals((Integer)1, cache.get("key"));

        cache.clearAll();

        assertNull(cache.projection(entryKeyFilterInv).reload("key"));
        assertEquals((Integer)1, cache.projection(entryKeyFilter).reload("key"));

        assertEquals((Integer)1, peek(cache, "key"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReloadAsyncFiltered() throws Exception {
        GridCache<String, Integer> cache = primaryCache("key");

        assertNull(cache.get("key"));

        cache.put("key", 1);

        assertEquals((Integer)1, cache.get("key"));

        cache.clearAll();

        assertNull(cache.projection(entryKeyFilterInv).reloadAsync("key").get());
        assertEquals((Integer) 1, cache.projection(entryKeyFilter).reloadAsync("key").get());

        assertEquals((Integer)1, cache.peek("key"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReloadAll() throws Exception {
        GridCache<String, Integer> cache = cache();

        Collection<String> keys = primaryKeysForCache(cache, 2);

        for (String key : keys)
            assertNull(cache.peek(key));

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), cache.peek(key));

        cache.clearAll();

        for (String key : keys)
            assertNull(cache.peek(key));

        cache.reloadAll(keys);

        for (String key : keys)
            assertEquals(vals.get(key), cache.peek(key));

        cache.clearAll();

        for (String key : keys)
            assertNull(cache.peek(key));

        String[] keysArr = new String[keys.size()];
        keys.toArray(keysArr);

        cache.reloadAll(F.asList(keysArr));

        for (String key : keys) {
            assertEquals(vals.get(key), cache.peek(key));

            cache.clear(key);
        }

        cache.reloadAll(keys);

        for (String key : keys)
            assertEquals(vals.get(key), cache.peek(key));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReloadAllAsync() throws Exception {
        GridCache<String, Integer> cache = cache();

        Collection<String> keys = primaryKeysForCache(cache, 2);

        for (String key : keys)
            assertNull(cache.peek(key));

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), cache.peek(key));

        cache.clearAll();

        for (String key : keys)
            assertNull(cache.peek(key));

        cache.reloadAllAsync(keys).get();

        for (String key : keys)
            assertEquals(vals.get(key), cache.peek(key));

        cache.clearAll();

        for (String key : keys)
            assertNull(cache.peek(key));

        String[] keysArr = new String[keys.size()];
        keys.toArray(keysArr);

        cache.reloadAllAsync(F.asList(keysArr)).get();

        for (String key : keys)
            assertEquals(vals.get(key), cache.peek(key));

        for (String key : keys) {
            assertEquals(vals.get(key), cache.peek(key));

            cache.clear(key);
        }

        cache.reloadAllAsync(keys).get();

        for (String key : keys)
            assertEquals(vals.get(key), cache.peek(key));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReloadAllFiltered() throws Exception {
        GridCache<String, Integer> cache = cache();

        Collection<String> keys = primaryKeysForCache(cache, 3);

        for (String key : keys)
            assertNull(cache.get(key));

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), cache.peek(key));

        cache.clearAll();

        for (String key : keys)
            assertNull(cache.peek(key));

        String first = F.first(keys);

        cache.put(first, 0);

        assertEquals((Integer)0, cache.peek(first));

        cache.projection(F.<String, Integer>cacheHasPeekValue()).reloadAll(keys);

        assertEquals((Integer)0, cache.peek(first));

        for (String key : keys) {
            if (!first.equals(key)) // Should not have peek value.
                assertNull(cache.peek(key));
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReloadAllAsyncFiltered() throws Exception {
        GridCache<String, Integer> cache = cache();

        Collection<String> keys = primaryKeysForCache(cache, 3);

        for (String key : keys)
            assertNull(cache.get(key));

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), cache.peek(key));

        cache.clearAll();

        for (String key : keys)
            assertNull(cache.peek(key));

        String first = F.first(keys);

        cache.put(first, 0);

        assertEquals((Integer)0, cache.peek(first));

        cache.projection(F.<String, Integer>cacheHasPeekValue()).reloadAllAsync(keys).get();

        assertEquals((Integer)0, cache.peek(first));

        for (String key : keys) {
            if (!first.equals(key)) // Should not have peek value.
                assertNull(cache.peek(key));
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testClear() throws Exception {
        GridCache<String, Integer> cache = cache();

        Collection<String> keys = primaryKeysForCache(cache, 3);

        for (String key : keys)
            assertNull(cache.get(key));

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));

        cache.clearAll();

        for (String key : keys)
            assertNull(peek(cache, key));

        for (i = 0; i < gridCount(); i++)
            cache(i).clearAll();

        for (i = 0; i < gridCount(); i++)
            assert cache(i).isEmpty();

        for (Map.Entry<String, Integer> entry : vals.entrySet())
            cache.put(entry.getKey(), entry.getValue());

        for (String key : keys)
            assertEquals(vals.get(key), peek(cache, key));

        String first = F.first(keys);

        if (lockingEnabled()) {
            assertTrue(cache.lock(first, 0));

            cache.clearAll();

            assertEquals(vals.get(first), peek(cache, first));

            cache.unlock(first);
        }
        else {
            cache.clearAll();

            cache.put(first, vals.get(first));
        }

        cache.projection(gte100).clear(first);

        assertNotNull(peek(cache, first));

        cache.put(first, 101);

        cache.projection(gte100).clear(first);

        assert cache.isEmpty() : "Values after clear: " + cache.values();

        i = 0;

        for (String key : keys) {
            cache.put(key, i);

            vals.put(key, i);

            i++;
        }

        for (String key : keys) {
            if (!first.equals(key))
                assertEquals(vals.get(key), peek(cache, key));
        }

        cache().put("key1", 1);
        cache().put("key2", 2);

        cache().evictAll();

        assert cache().isEmpty();

        cache().clearAll();

        assert cache().promote("key1") == null;
        assert cache().promote("key2") == null;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testClearKeys() throws Exception {
        GridCache<String, Integer> cache = cache();

        Collection<String> keys = primaryKeysForCache(cache, 3);

        for (String key : keys)
            assertNull(cache.get(key));

        String lastKey = F.last(keys);

        Collection<String> subKeys = new ArrayList<>(keys);

        subKeys.remove(lastKey);

        Map<String, Integer> vals = new HashMap<>(keys.size());

        int i = 0;

        for (String key : keys)
            vals.put(key, i++);

        cache.putAll(vals);

        for (String subKey : subKeys)
            cache.clear(subKey);

        for (String key : subKeys)
            assertNull(cache.peek(key));

        assertEquals(vals.get(lastKey), cache.peek(lastKey));

        cache.clearAll();

        vals.put(lastKey, 102);

        cache.putAll(vals);

        for (String key : keys)
            cache.projection(gte100).clear(key);

        assertNull(cache.peek(lastKey));

        for (String key : subKeys)
            assertEquals(vals.get(key), cache.peek(key));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testGlobalClearAll() throws Exception {
        // Save entries only on their primary nodes. If we didn't do so, clearAll() will not remove all entries
        // because some of them were blocked due to having readers.
        for (int i = 0; i < gridCount(); i++) {
            for (String key : primaryKeysForCache(cache(i), 3, 100_000))
                cache(i).put(key, 1);
        }

        cache().globalClearAll();

        for (int i = 0; i < gridCount(); i++)
            assert cache(i).isEmpty();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testEntrySet() throws Exception {
        if (offheapTiered(cache()))
            return;

        cache().put("key1", 1);
        cache().put("key2", 2);
        cache().put("key3", 3);

        Collection<GridCacheEntry<String, Integer>> entries = new HashSet<>();

        for (int i = 0; i < gridCount(); i++)
            entries.addAll(cache(i).entrySet());

        assertEquals(3, entries.size());

        for (GridCacheEntry<String, Integer> entry : entries)
            assert "key1".equals(entry.getKey()) || "key2".equals(entry.getKey()) ||
                "key3".equals(entry.getKey());
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockUnlock() throws Exception {
        if (lockingEnabled()) {
            final CountDownLatch lockCnt = new CountDownLatch(1);
            final CountDownLatch unlockCnt = new CountDownLatch(1);

            grid(0).events().localListen(new IgnitePredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    switch (evt.type()) {
                        case EVT_CACHE_OBJECT_LOCKED:
                            lockCnt.countDown();

                            break;
                        case EVT_CACHE_OBJECT_UNLOCKED:
                            unlockCnt.countDown();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_UNLOCKED);

            GridCache<String, Integer> cache = cache();

            String key = primaryKeysForCache(cache, 1).get(0);

            cache.put(key, 1);

            assert !cache.isLocked(key);

            cache.lock(key, 0);

            lockCnt.await();

            assert cache.isLocked(key);

            cache.unlock(key);

            unlockCnt.await();

            for (int i = 0; i < 100; i++)
                if (cache.isLocked(key))
                    Thread.sleep(10);
                else
                    break;

            assert !cache.isLocked(key);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockAsync() throws Exception {
        if (lockingEnabled()) {
            cache().put("key", 1);

            assert !cache().isLocked("key");

            cache().lockAsync("key", 0).get();

            assert cache().isLocked("key");

            cache().unlock("key");

            for (int i = 0; i < 100; i++)
                if (cache().isLocked("key"))
                    Thread.sleep(10);
                else
                    break;

            assert !cache().isLocked("key");
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockAsyncEntry() throws Exception {
        if (lockingEnabled()) {
            cache().put("key", 1);

            GridCacheEntry<String, Integer> e = cache().entry("key");

            assert e != null;

            assert !e.isLocked();

            e.lockAsync(0).get();

            assert e.isLocked();

            e.unlock();

            for (int i = 0; i < 100; i++)
                if (e.isLocked())
                    Thread.sleep(10);
                else
                    break;

            assert !e.isLocked();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testLockWithTimeout() throws Exception {
        if (lockingEnabled()) {
            cache().put("key", 1);

            assert !cache().isLocked("key");

            cache().lock("key", 2000);

            assert cache().isLocked("key");
            assert cache().isLockedByThread("key");

            assert !forLocal(dfltIgnite).call(new Callable<Boolean>() {
                @Override public Boolean call() throws GridException {
                    return cache().lock("key", 100);
                }
            });

            cache().unlock("key");
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockWithTimeoutEntry() throws Exception {
        if (lockingEnabled()) {
            cache().put("key", 1);

            final GridCacheEntry<String, Integer> e = cache().entry("key");

            assert e != null;

            assert !e.isLocked();

            e.lock(2000);

            assert e.isLocked();

            assert !forLocal(dfltIgnite).call(new Callable<Boolean>() {
                @Override public Boolean call() throws GridException {
                    return e.lock(100);
                }
            });

            e.unlock();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockAsyncWithTimeout() throws Exception {
        if (lockingEnabled()) {
            cache().put("key", 1);

            assert !cache().isLocked("key");

            cache().lockAsync("key", 1000).get();

            assert cache().isLocked("key");
            assert cache().isLockedByThread("key");

            final CountDownLatch latch = new CountDownLatch(1);

            IgniteCompute comp = forLocal(dfltIgnite).enableAsync();

            comp.call(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    IgniteFuture<Boolean> f = cache().lockAsync("key", 1000);

                    try {
                        f.get(100);

                        fail();
                    } catch (GridFutureTimeoutException ex) {
                        info("Caught expected exception: " + ex);
                    }

                    latch.countDown();

                    try {
                        assert f.get();
                    } finally {
                        cache().unlock("key");
                    }

                    return true;
                }
            });

            IgniteFuture<Boolean> f = comp.future();

                // Let another thread start.
            latch.await();

            assert cache().isLocked("key");
            assert cache().isLockedByThread("key");

            cache().unlock("key");

            assert f.get();

            for (int i = 0; i < 100; i++)
                if (cache().isLocked("key") || cache().isLockedByThread("key"))
                    Thread.sleep(10);
                else
                    break;

            assert !cache().isLocked("key");
            assert !cache().isLockedByThread("key");
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockAsyncWithTimeoutEntry() throws Exception {
        if (lockingEnabled()) {
            // Put only to primary.
            ClusterNode node = F.first(cache().affinity().mapKeyToPrimaryAndBackups("key"));

            if (node == null)
                throw new GridException("Failed to map key.");

            GridCache<String, Integer> cache = G.grid(node.id()).cache(null);

            final GridCacheEntry<String, Integer> e = cache.entry("key");

            info("Entry [e=" + e + ", primary=" + e.primary() + ", backup=" + e.backup() + ']');

            assert e != null;

            assert !e.isLocked();

            e.lockAsync(2000).get();

            assert e.isLocked();

            final CountDownLatch syncLatch = new CountDownLatch(1);

            IgniteCompute comp = forLocal(dfltIgnite).enableAsync();

            comp.call(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    syncLatch.countDown();

                    IgniteFuture<Boolean> f = e.lockAsync(1000);

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
                    }

                    return true;
                }
            });

            IgniteFuture<Boolean> f = comp.future();

            syncLatch.await();

            // Make 1st future in closure fail.
            Thread.sleep(300);

            assert e.isLocked();
            assert e.isLockedByThread();

            cache.unlock("key");

            assert f.get();

            for (int i = 0; i < 100; i++)
                if (cache.isLocked("key") || cache.isLockedByThread("key"))
                    Thread.sleep(10);
                else
                    break;

            assert !cache.isLocked("key");
            assert !cache.isLockedByThread("key");
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockFiltered() throws Exception {
        if (lockingEnabled()) {
            cache().put("key1", 1);
            cache().put("key2", 100);

            for (int i = 0; i < gridCount(); i++) {
                assert !cache(i).isLocked("key1");
                assert !cache(i).isLocked("key2");
            }

            cache().projection(F.<GridCacheEntry<String, Integer>>alwaysFalse()).lock("key1", 0);
            cache().projection(F.<GridCacheEntry<String, Integer>>alwaysTrue()).lock("key2", 0);

            boolean passed = false;

            for (int i = 0; i < gridCount(); i++) {
                assertFalse("key1 is locked for cache: " + i, cache(i).isLocked("key1"));

                if (cache(i).isLocked("key2"))
                    passed = true;
            }

            assert passed;

            // Must pass keys as lock() was called with keys.
            cache().unlockAll(F.asList("key2"), F.<GridCacheEntry<String, Integer>>alwaysTrue());

            for (int i = 0; i < 100; i++) {
                boolean sleep = false;

                for (int j = 0; j < gridCount(); j++) {
                    if (cache(j).isLocked("key1") || cache(j).isLocked("key2")) {
                        sleep = true;

                        break;
                    }
                }

                if (sleep)
                    Thread.sleep(10);
                else
                    break;
            }

            for (int i = 0; i < gridCount(); i++) {
                assert !cache(i).isLocked("key1");
                assert !cache(i).isLocked("key2");
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockFilteredEntry() throws Exception {
        if (lockingEnabled()) {
            cache().put("key1", 1);
            cache().put("key2", 100);

            for (int i = 0; i < gridCount(); i++) {
                assert !cache(i).entry("key1").isLocked();
                assert !cache(i).entry("key2").isLocked();
            }

            cache().projection(F.<GridCacheEntry<String, Integer>>alwaysFalse()).entry("key1").lock(0);
            cache().projection(F.<GridCacheEntry<String, Integer>>alwaysTrue()).entry("key2").lock(0);

            boolean passed = false;

            for (int i = 0; i < gridCount(); i++) {
                assert !cache(i).entry("key1").isLocked();

                if (cache(i).entry("key2").isLocked())
                    passed = true;
            }

            assert passed;

            cache().unlockAll(F.asList("key1", "key2"), F.<GridCacheEntry<String, Integer>>alwaysTrue());

            for (int i = 0; i < 100; i++) {
                boolean sleep = false;

                for (int j = 0; j < gridCount(); j++) {
                    if (cache(j).entry("key1").isLocked() || cache(j).entry("key2").isLocked()) {
                        sleep = true;

                        break;
                    }
                }

                if (sleep)
                    Thread.sleep(10);
                else
                    break;
            }

            for (int i = 0; i < gridCount(); i++) {
                assert !cache(i).entry("key1").isLocked();
                assert !cache(i).entry("key2").isLocked();
            }
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testUnlockFiltered() throws Exception {
        if (lockingEnabled()) {
            List<String> keys = primaryKeysForCache(cache(), 2);

            info("Keys: " + keys);

            final String key1 = keys.get(0);
            final String key2 = keys.get(1);

            cache().put(key1, 1);
            cache().put(key2, 100);

            assert !cache().isLocked(key1);
            assert !cache().isLocked(key2);

            cache().lock(key1, 0);
            cache().lock(key2, 0);

            assert cache().isLocked(key1);
            assert cache().isLocked(key2);

            cache().unlock(key1, gte100);
            cache().unlock(key2, gte100);

            GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    for (int g = 0; g < gridCount(); g++) {
                        if (cache(g).isLocked(key2)) {
                            info(key2 + " is locked on grid: " + g);

                            return false;
                        }
                    }

                    return true;
                }
            }, 2000);

            assert cache().isLocked(key1);
            assert !cache().isLocked(key2);

            cache().unlockAll(F.asList(key1, key2));

            GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    for (int g = 0; g < gridCount(); g++) {
                        if (cache(g).isLocked(key1))
                            info(key1 + " is locked on grid: " + g);

                        if (cache(g).isLocked(key2))
                            info(key2 + " is locked on grid: " + g);
                    }

                    return true;
                }
            }, 2000);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testUnlockFilteredEntry() throws Exception {
        if (lockingEnabled()) {
            cache().put("key1", 1);
            cache().put("key2", 100);

            GridCacheEntry<String, Integer> e1 = cache().entry("key1");
            GridCacheEntry<String, Integer> e2 = cache().entry("key2");

            assert e1 != null;
            assert e2 != null;

            assert !e1.isLocked();
            assert !e2.isLocked();

            e1.lock(0);
            e2.lock(0);

            assert e1.isLocked();
            assert e2.isLocked();

            e1.unlock(F.<GridCacheEntry<String, Integer>>alwaysFalse());
            e2.unlock(F.<GridCacheEntry<String, Integer>>alwaysTrue());

            for (int i = 0; i < 100; i++)
                if (e2.isLocked())
                    Thread.sleep(10);
                else
                    break;

            assert e1.isLocked();
            assert !e2.isLocked();

            cache().unlockAll(F.asList("key1", "key2"), F.<GridCacheEntry<String, Integer>>alwaysTrue());
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockUnlockAll() throws Exception {
        if (lockingEnabled()) {
            cache().put("key1", 1);
            cache().put("key2", 2);

            assert !cache().isLocked("key1");
            assert !cache().isLocked("key2");

            cache().lockAll(F.asList("key1", "key2"), 0);

            assert cache().isLocked("key1");
            assert cache().isLocked("key2");

            cache().unlockAll(F.asList("key1", "key2"));

            for (int i = 0; i < 100; i++)
                if (cache().isLocked("key1") || cache().isLocked("key2"))
                    Thread.sleep(10);
                else
                    break;

            assert !cache().isLocked("key1");
            assert !cache().isLocked("key2");

            cache().lockAll(F.asList("key1", "key2"), 0);

            assert cache().isLocked("key1");
            assert cache().isLocked("key2");

            cache().unlockAll(F.asList("key1", "key2"));

            for (int i = 0; i < 100; i++)
                if (cache().isLocked("key1") || cache().isLocked("key2"))
                    Thread.sleep(10);
                else
                    break;

            assert !cache().isLocked("key1");
            assert !cache().isLocked("key2");
        }
    }

    /**
     * @throws Exception In case of error.
     */
    @SuppressWarnings("BusyWait")
    public void testLockAllFiltered() throws Exception {
        if (lockingEnabled()) {
            cache().put("key1", 1);
            cache().put("key2", 2);
            cache().put("key3", 100);
            cache().put("key4", 101);

            assert !cache().isLocked("key1");
            assert !cache().isLocked("key2");
            assert !cache().isLocked("key3");
            assert !cache().isLocked("key4");

            assert !cache().isLockedByThread("key1");
            assert !cache().isLockedByThread("key2");
            assert !cache().isLockedByThread("key3");
            assert !cache().isLockedByThread("key4");

            assert !cache().projection(gte100).lockAll(F.asList("key2", "key3"), 0);

            assert !cache().isLocked("key1");
            assert !cache().isLocked("key2");
            assert !cache().isLocked("key3");
            assert !cache().isLocked("key4");

            assert !cache().isLockedByThread("key1");
            assert !cache().isLockedByThread("key2");
            assert !cache().isLockedByThread("key3");
            assert !cache().isLockedByThread("key4");

            assert cache().projection(F.<GridCacheEntry<String, Integer>>alwaysTrue()).lockAll(
                F.asList("key1", "key2", "key3", "key4"), 0);

            assert cache().isLocked("key1");
            assert cache().isLocked("key2");
            assert cache().isLocked("key3");
            assert cache().isLocked("key4");

            assert cache().isLockedByThread("key1");
            assert cache().isLockedByThread("key2");
            assert cache().isLockedByThread("key3");
            assert cache().isLockedByThread("key4");

            cache().unlockAll(F.asList("key1", "key2", "key3", "key4"),
                F.<GridCacheEntry<String, Integer>>alwaysTrue());

            for (String key : cache().primaryKeySet()) {
                for (int i = 0; i < 100; i++)
                    if (cache().isLocked(key))
                        Thread.sleep(10);
                    else
                        break;

                assert !cache().isLocked(key);
            }

            assert !cache().isLockedByThread("key1");
            assert !cache().isLockedByThread("key2");
            assert !cache().isLockedByThread("key3");
            assert !cache().isLockedByThread("key4");
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPeek() throws Exception {
        GridCache<String, Integer> cache = primaryCache("key");

        assert cache.peek("key") == null;

        cache.put("key", 1);

        GridCacheTx tx = txEnabled() ? cache.txStart() : null;

        try {
            cache.replace("key", 2);

            assert cache.peek("key") == 2;
        }
        finally {
            if (tx != null)
                tx.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekTxRemoveOptimistic() throws Exception {
        checkPeekTxRemove(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekTxRemovePessimistic() throws Exception {
        checkPeekTxRemove(PESSIMISTIC);
    }

    /**
     * @param concurrency Concurrency.
     * @throws Exception If failed.
     */
    private void checkPeekTxRemove(GridCacheTxConcurrency concurrency) throws Exception {
        if (txEnabled()) {
            GridCache<String, Integer> cache = primaryCache("key");

            cache.put("key", 1);

            try (GridCacheTx tx = cache.txStart(concurrency, READ_COMMITTED)) {
                cache.remove("key");

                assertNull(cache.peek("key"));

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeekRemove() throws Exception {
        GridCache<String, Integer> cache = primaryCache("key");

        cache.put("key", 1);
        cache.remove("key");

        assertNull(cache.peek("key"));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPeekMode() throws Exception {
        String key = "testPeekMode";
        GridCache<String, Integer> cache = primaryCache(key);

        cache.put(key, 1);

        GridCacheEntry<String, Integer> entry = cache.entry(key);

        assert entry.primary();

        assert cache.peek(key, F.asList(TX)) == null;
        assert cache.peek(key, F.asList(SWAP)) == null;
        assert cache.peek(key, F.asList(DB)) == 1;
        assert cache.peek(key, F.asList(TX, GLOBAL)) == 1;

        if (!partitionedMode()) {
            assert cache.peek(key, F.asList(TX, NEAR_ONLY)) == 1;
            assert cache.peek(key, F.asList(TX, PARTITIONED_ONLY)) == 1;
        }

        assert cache.peek(key, F.asList(SMART)) == 1;

        assert entry.peek(F.asList(TX)) == null;
        assert entry.peek(F.asList(SWAP)) == null;
        assert entry.peek(F.asList(DB)) == 1;
        assert entry.peek(F.asList(TX, GLOBAL)) == 1;

        if (!partitionedMode()) {
            assert entry.peek(F.asList(TX, NEAR_ONLY)) == 1;
            assert entry.peek(F.asList(TX, PARTITIONED_ONLY)) == 1;
        }

        assert entry.peek(F.asList(SMART)) == 1;

        GridCacheEntry<String, Integer> ew = cache.entry("wrongKey");

        assert cache.peek("wrongKey", F.asList(TX, GLOBAL, SWAP, DB)) == null;

        if (!partitionedMode()) {
            assert cache.peek("wrongKey", F.asList(TX, NEAR_ONLY, SWAP, DB)) == null;
            assert cache.peek("wrongKey", F.asList(TX, PARTITIONED_ONLY, SWAP, DB)) == null;
        }

        assert ew.peek(F.asList(TX, GLOBAL, SWAP, DB)) == null;

        if (cacheMode() != PARTITIONED) {
            assert ew.peek(F.asList(TX, NEAR_ONLY, SWAP, DB)) == null;
            assert ew.peek(F.asList(TX, PARTITIONED_ONLY, SWAP, DB)) == null;
        }

        if (txEnabled()) {
            GridCacheTx tx = cache.txStart();

            cache.replace(key, 2);

            assert cache.peek(key, F.asList(GLOBAL)) == 1;

            if (!partitionedMode()) {
                assert cache.peek(key, F.asList(NEAR_ONLY)) == 1;
                assert cache.peek(key, F.asList(PARTITIONED_ONLY)) == 1;
            }

            assert cache.peek(key, F.asList(TX)) == 2;
            assert cache.peek(key, F.asList(SMART)) == 2;
            assert cache.peek(key, F.asList(SWAP)) == null;
            assert cache.peek(key, F.asList(DB)) == 1;

            assertEquals((Integer)1, entry.peek(F.asList(GLOBAL)));

            if (!partitionedMode()) {
                assertEquals((Integer)1, entry.peek(F.asList(NEAR_ONLY)));
                assertEquals((Integer)1, entry.peek(F.asList(PARTITIONED_ONLY)));
            }

            assertEquals((Integer)2, entry.peek(F.asList(TX)));
            assertEquals((Integer)2, entry.peek(F.asList(SMART)));
            assertNull(entry.peek(F.asList(SWAP)));
            assertEquals((Integer)1, entry.peek(F.asList(DB)));

            tx.commit();
        }
        else
            cache.replace(key, 2);

        assertEquals((Integer)2, cache.peek(key, F.asList(GLOBAL)));

        if (!partitionedMode()) {
            assertEquals((Integer)2, cache.peek(key, F.asList(NEAR_ONLY)));
            assertEquals((Integer)2, cache.peek(key, F.asList(PARTITIONED_ONLY)));
        }

        assertNull(cache.peek(key, F.asList(TX)));
        assertNull(cache.peek(key, F.asList(SWAP)));
        assertEquals((Integer)2, cache.peek(key, F.asList(DB)));

        assertEquals((Integer)2, entry.peek(F.asList(GLOBAL)));

        if (!partitionedMode()) {
            assertEquals((Integer)2, entry.peek(F.asList(NEAR_ONLY)));
            assertEquals((Integer)2, entry.peek(F.asList(PARTITIONED_ONLY)));
        }

        assertNull(entry.peek(F.asList(TX)));
        assertNull(entry.peek(F.asList(SWAP)));
        assertEquals((Integer)2, entry.peek(F.asList(DB)));

        assertTrue(cache.evict(key));

        assertNull(cache.peek(key, F.asList(SMART)));
        assertNull(cache.peek(key, F.asList(TX, GLOBAL)));

        if (!partitionedMode()) {
            assertNull(cache.peek(key, F.asList(TX, NEAR_ONLY)));
            assertNull(cache.peek(key, F.asList(TX, PARTITIONED_ONLY)));
        }

        assertEquals((Integer)2, cache.peek(key, F.asList(SWAP)));
        assertEquals((Integer)2, cache.peek(key, F.asList(DB)));
        assertEquals((Integer)2, cache.peek(key, F.asList(SMART, SWAP, DB)));

        assertNull(entry.peek(F.asList(SMART)));
        assertNull(entry.peek(F.asList(TX, GLOBAL)));

        if (!partitionedMode()) {
            assertNull(entry.peek(F.asList(TX, NEAR_ONLY)));
            assertNull(entry.peek(F.asList(TX, PARTITIONED_ONLY)));
        }

        assertEquals((Integer)2, cache.peek(key, F.asList(SWAP)));

        assertEquals((Integer)2, entry.peek(F.asList(DB)));
        assertEquals((Integer)2, entry.peek(F.asList(SMART, SWAP, DB)));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPeekFiltered() throws Exception {
        GridCache<String, Integer> cache1 = primaryCache("key1");
        GridCache<String, Integer> cache2 = primaryCache("key2");

        cache1.put("key1", 1);
        cache2.put("key2", 100);

        assertNull(peek(cache1.projection(gte100), "key1"));
        assertEquals((Integer)100, peek(cache2.projection(gte100), "key2"));

        if (txEnabled()) {
            GridCacheTx tx = cache().txStart();

            assertEquals((Integer)1, cache1.replace("key1", 101));
            assertEquals((Integer)100, cache2.replace("key2", 2));

            assertEquals((Integer)101, peek(cache1.projection(gte100), "key1"));
            assertNull(peek(cache2.projection(gte100), "key2"));

            tx.close();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testEvict() throws Exception {
        GridCache<String, Integer> cache = cache();

        List<String> keys = primaryKeysForCache(cache, 2);

        String key = keys.get(0);
        String key2 = keys.get(1);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        assertTrue(cache.evict(key));

        assertNull(cache.peek(key));

        cache.reload(key);

        assertEquals((Integer)1, cache.peek(key));

        cache.remove(key);

        cache.put(key, 1);
        cache.put(key2, 102);

        assertFalse(cache.projection(gte100).evict(key));

        assertEquals((Integer)1, cache.get(key));

        assertTrue(cache.projection(gte100).evict(key2));

        assertNull(cache.peek(key2));

        assertTrue(cache.evict(key));

        assertNull(cache.peek(key));
    }

    /**
     * @throws Exception In case of error.
     */
    public void testEvictExpired() throws Exception {
        GridCache<String, Integer> cache = cache();

        String key = primaryKeysForCache(cache, 1).get(0);

        cache.put(key, 1);

        assertEquals((Integer)1, cache.get(key));

        GridCacheEntry<String, Integer> entry = cache.entry(key);

        assert entry != null;

        long ttl = 500;

        entry.timeToLive(ttl);

        // Update is required for TTL to have effect.
        entry.set(1);

        Thread.sleep(ttl + 100);

        // Expired entry should not be swapped.
        assertTrue(cache.evict(key));

        assertNull(cache.peek(key));

        assertNull(cache.promote(key));

        assertNull(cache.peek(key));

        assertTrue(cache.isEmpty());

        // Force reload on primary node.
        for (int i = 0; i < gridCount(); i++) {
            if (cache(i).entry(key).primary())
                cache(i).reload(key);
        }

        // Will do near get request.
        cache.reload(key);

        assertEquals((Integer)1, peek(cache, key));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPeekExpired() throws Exception {
        GridCache<String, Integer> c = cache();

        String key = primaryKeysForCache(c, 1).get(0);

        info("Using key: " + key);

        c.put(key, 1);

        assertEquals(Integer.valueOf(1), c.peek(key));

        int ttl = 500;

        GridCacheEntry<String, Integer> entry = c.entry(key);

        entry.timeToLive(ttl);

        // Update is required for TTL to have effect.
        entry.set(1);

        Thread.sleep(ttl + 100);

        assert c.peek(key) == null;

        assert c.isEmpty() : "Cache is not empty: " + c.values();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPeekExpiredTx() throws Exception {
        if (txEnabled()) {
            GridCache<String, Integer> c = cache();

            String key = "1";
            int ttl = 500;

            try (GridCacheTx tx = c.txStart()) {
                GridCacheEntry<String, Integer> entry = c.entry(key);

                entry.timeToLive(ttl);

                entry.set(1);

                tx.commit();
            }

            Thread.sleep(ttl + 100);

            assertNull(c.peek(key));

            assert c.isEmpty();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTtlTx() throws Exception {
        if (txEnabled())
            checkTtl(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTtlNoTx() throws Exception {
        checkTtl(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTtlNoTxOldEntry() throws Exception {
        checkTtl(false, true);
    }

    /**
     * @param inTx In tx flag.
     * @param oldEntry {@code True} to check TTL on old entry, {@code false} on new.
     * @throws Exception If failed.
     */
    private void checkTtl(boolean inTx, boolean oldEntry) throws Exception {
        final GridCache<String, Integer> c = cache();

        final String key = primaryKeysForCache(c, 1).get(0);

        if (oldEntry)
            c.put(key, 1);

        GridCacheEntry<String, Integer> entry = c.entry(key);

        assert entry != null;

        assertEquals(0, entry.timeToLive());
        assertEquals(0, entry.expirationTime());

        int ttl = 1000;

        long startTime = System.currentTimeMillis();

        if (inTx) {
            // Rollback transaction for the first time.
            GridCacheTx tx = c.txStart();

            try {
                entry.timeToLive(ttl);

                entry.set(1);

                assertEquals(ttl, entry.timeToLive());
                assertTrue(entry.expirationTime() > 0);
            }
            finally {
                tx.rollback();
            }

            assertEquals(ttl, entry.timeToLive());
            assertTrue(entry.expirationTime() > 0);
        }

        // Now commit transaction and check that ttl and expire time have been saved.
        GridCacheTx tx = inTx ? c.txStart() : null;

        try {
            entry.timeToLive(ttl);

            entry.set(1);

            assertEquals(ttl, entry.timeToLive());
            assertTrue(entry.expirationTime() > 0);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        long[] expireTimes = new long[gridCount()];

        for (int i = 0; i < gridCount(); i++) {
            GridCacheEntry<String, Integer> curEntry = cache(i).entry(key);

            if (curEntry.primary() || curEntry.backup()) {
                assertEquals(ttl, curEntry.timeToLive());

                assert curEntry.expirationTime() > startTime;

                expireTimes[i] = curEntry.expirationTime();
            }
        }

        // One more update from the same cache entry to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? c.txStart() : null;

        try {
            c.entry(key).set(2);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            GridCacheEntry<String, Integer> curEntry = cache(i).entry(key);

            if (curEntry.primary() || curEntry.backup()) {
                assertEquals(ttl, curEntry.timeToLive());

                assert curEntry.expirationTime() > expireTimes[i];

                expireTimes[i] = curEntry.expirationTime();
            }
        }

        // And one more direct update to ensure that expire time is shifted forward.
        U.sleep(100);

        tx = inTx ? c.txStart() : null;

        try {
            c.putx(key, 3);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            GridCacheEntry<String, Integer> curEntry = cache(i).entry(key);

            if (curEntry.primary() || curEntry.backup()) {
                assertEquals(ttl, curEntry.timeToLive());

                assert curEntry.expirationTime() > expireTimes[i];
            }
        }

        // Avoid reloading from store.
        map.remove(key);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @SuppressWarnings("unchecked")
            @Override public boolean applyx() throws GridException {
                try {
                    if (c.get(key) != null)
                        return false;

                    // Get "cache" field from GridCacheProxyImpl.
                    GridCacheAdapter c0 = GridTestUtils.getFieldValue(c, "cache");

                    if (!c0.context().deferredDelete()) {
                        GridCacheEntryEx e0 = c0.peekEx(key);

                        return e0 == null || (e0.rawGet() == null && e0.valueBytes() == null);
                    }
                    else
                        return true;
                }
                catch (GridCacheEntryRemovedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, Math.min(ttl * 10, getTestTimeout())));

        // Ensure that old TTL and expire time are not longer "visible".
        entry = c.entry(key);

        assert entry.get() == null;

        assertEquals(0, entry.timeToLive());
        assertEquals(0, entry.expirationTime());

        // Ensure that next update will not pick old expire time.

        tx = inTx ? c.txStart() : null;

        try {
            entry.set(10);
        }
        finally {
            if (tx != null)
                tx.commit();
        }

        U.sleep(2000);

        entry = c.entry(key);

        assertEquals((Integer)10, entry.get());

        assertEquals(0, entry.timeToLive());
        assertEquals(0, entry.expirationTime());
    }

    /**
     * @throws Exception In case of error.
     */
    public void testEvictAll() throws Exception {
        List<String> keys = primaryKeysForCache(cache(), 3);

        String key1 = keys.get(0);
        String key2 = keys.get(1);
        String key3 = keys.get(2);

        cache().put(key1, 1);
        cache().put(key2, 2);
        cache().put(key3, 3);

        assert cache().peek(key1) == 1;
        assert cache().peek(key2) == 2;
        assert cache().peek(key3) == 3;

        cache().evictAll(F.asList(key1, key2));

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == 3;

        cache().reloadAll(F.asList(key1, key2));

        assert cache().peek(key1) == 1;
        assert cache().peek(key2) == 2;
        assert cache().peek(key3) == 3;

        cache().evictAll(F.asList(key1, key2));

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == 3;

        cache().reloadAll(F.asList(key1, key2));

        assert cache().peek(key1) == 1;
        assert cache().peek(key2) == 2;
        assert cache().peek(key3) == 3;

        cache().evictAll();

        assert cache().peek(key1) == null;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == null;

        cache().put(key1, 1);
        cache().put(key2, 102);
        cache().put(key3, 3);

        U.debug(log, "Before evictAll");

        cache().projection(gte100).evictAll();

        U.debug(log, "After evictAll");

        assertEquals((Integer)1, cache().peek(key1));
        assertNull(cache().peek(key2));
        assertEquals((Integer)3, cache().peek(key3));

        cache().put(key1, 1);
        cache().put(key2, 102);
        cache().put(key3, 3);

        cache().projection(gte100).evictAll(F.asList(key1, key2, key3));

        assert cache().peek(key1) == 1;
        assert cache().peek(key2) == null;
        assert cache().peek(key3) == 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnswap() throws Exception {
        List<String> keys = primaryKeysForCache(cache(), 3);

        String k1 = keys.get(0);
        String k2 = keys.get(1);
        String k3 = keys.get(2);

        cache().put(k1, 1);
        cache().put(k2, 2);
        cache().put(k3, 3);

        final AtomicInteger swapEvts = new AtomicInteger(0);
        final AtomicInteger unswapEvts = new AtomicInteger(0);

        Collection<String> locKeys = new HashSet<>();

        if (CU.isAffinityNode(cache().configuration())) {
            locKeys.addAll(cache().projection(F.<String, Integer>cachePrimary()).keySet());

            info("Local keys (primary): " + locKeys);

            locKeys.addAll(cache().projection(F.<String, Integer>cacheBackup()).keySet());

            info("Local keys (primary + backup): " + locKeys);
        }

        for (int i = 0; i < gridCount(); i++) {
            grid(i).events().localListen(new IgnitePredicate<GridEvent>() {
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

        assert cache().evict(k2);
        assert cache().evict(k3);

        assert cache().containsKey(k1);
        assert !cache().containsKey(k2);
        assert !cache().containsKey(k3);

        int cnt = 0;

        if (locKeys.contains(k2)) {
            assertEquals((Integer)2, cache().promote(k2));

            cnt++;
        }
        else
            assertNull(cache().promote(k2));

        if (locKeys.contains(k3)) {
            assertEquals((Integer)3, cache().promote(k3));

            cnt++;
        }
        else
            assertNull(cache().promote(k3));

        assertEquals(cnt, swapEvts.get());
        assertEquals(cnt, unswapEvts.get());

        assert cache().evict(k1);

        assertEquals((Integer)1, cache().get(k1));

        if (locKeys.contains(k1))
            cnt++;

        assertEquals(cnt, swapEvts.get());
        assertEquals(cnt, unswapEvts.get());

        cache().clearAll();

        // Check with multiple arguments.
        cache().put(k1, 1);
        cache().put(k2, 2);
        cache().put(k3, 3);

        swapEvts.set(0);
        unswapEvts.set(0);

        cache().evict(k2);
        cache().evict(k3);

        assert cache().containsKey(k1);
        assert !cache().containsKey(k2);
        assert !cache().containsKey(k3);

        cache().promoteAll(F.asList(k2, k3));

        cnt = 0;

        if (locKeys.contains(k2))
            cnt++;

        if (locKeys.contains(k3))
            cnt++;

        assertEquals(cnt, swapEvts.get());
        assertEquals(cnt, unswapEvts.get());
    }

    /**
     * JUnit.
     */
    public void testCacheProxy() {
        GridCache<String, Integer> cache = cache();

        assert cache instanceof GridCacheProxy;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCompactExpired() throws Exception {
        GridCache<String, Integer> cache = cache();

        String key = F.first(primaryKeysForCache(cache, 1));

        cache.put(key, 1);

        GridCacheEntry<String, Integer> entry = cache.entry(key);

        assert entry != null;

        long ttl = 500;

        entry.timeToLive(ttl);

        // Update is required for TTL to have effect.
        entry.set(1);

        Thread.sleep(ttl + 100);

        // Peek will actually remove entry from cache.
        assert cache.peek(key) == null;

        assert cache.isEmpty();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticTxMissingKey() throws Exception {
        if (txEnabled()) {

            try (GridCacheTx tx = cache().txStart(OPTIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertTrue(cache().removex(UUID.randomUUID().toString()));

                tx.commit();
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticTxMissingKeyNoCommit() throws Exception {
        if (txEnabled()) {

            try (GridCacheTx tx = cache().txStart(OPTIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertTrue(cache().removex(UUID.randomUUID().toString()));

                tx.setRollbackOnly();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTxReadCommittedInTx() throws Exception {
        checkRemovexInTx(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTxRepeatableReadInTx() throws Exception {
        checkRemovexInTx(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxReadCommittedInTx() throws Exception {
        checkRemovexInTx(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxRepeatableReadInTx() throws Exception {
        checkRemovexInTx(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkRemovexInTx(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) throws Exception {
        if (txEnabled()) {
            final int cnt = 10;

            CU.inTx(cache(), concurrency, isolation, new CIX1<GridCacheProjection<String, Integer>>() {
                @Override
                public void applyx(GridCacheProjection<String, Integer> cache) throws GridException {
                    for (int i = 0; i < cnt; i++)
                        assertTrue(cache.putx("key" + i, i));
                }
            });

            CU.inTx(cache(), concurrency, isolation, new CIX1<GridCacheProjection<String, Integer>>() {
                @Override
                public void applyx(GridCacheProjection<String, Integer> cache) throws GridException {
                    for (int i = 0; i < cnt; i++)
                        assertEquals(new Integer(i), cache.get("key" + i));
                }
            });

            CU.inTx(cache(), concurrency, isolation, new CIX1<GridCacheProjection<String, Integer>>() {
                @Override
                public void applyx(GridCacheProjection<String, Integer> cache) throws GridException {
                    for (int i = 0; i < cnt; i++)
                        assertTrue(cache.removex("key" + i));
                }
            });
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticTxMissingKey() throws Exception {
        if (txEnabled()) {
            try (GridCacheTx tx = cache().txStart(PESSIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(cache().removex(UUID.randomUUID().toString()));

                tx.commit();
            }
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticTxMissingKeyNoCommit() throws Exception {
        if (txEnabled()) {
            try (GridCacheTx tx = cache().txStart(PESSIMISTIC, READ_COMMITTED)) {
                // Remove missing key.
                assertFalse(cache().removex(UUID.randomUUID().toString()));

                tx.setRollbackOnly();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxRepeatableRead() throws Exception {
        if (txEnabled()) {
            try (GridCacheTx ignored = cache().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache().putx("key", 1);

                assert cache().get("key") == 1;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxRepeatableReadOnUpdate() throws Exception {
        if (txEnabled()) {
            try (GridCacheTx ignored = cache().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache().put("key", 1);

                assert cache().put("key", 2) == 1;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryData() throws Exception {
        if (offheapTiered(cache(0)))
            return;

        final List<String> keys = new ArrayList<>(3);

        for (int i = 0; i < 3; i++) {
            while (true) {
                String key = UUID.randomUUID().toString();

                if (grid(0).mapKeyToNode(null, key).equals(grid(0).localNode())) {
                    assertTrue(cache(0).putx(key, i));

                    keys.add(key);

                    break;
                }
            }
        }

        if (cacheMode() == PARTITIONED && gridCount() > 1) {
            for (int i = 0; i < 10; i++) {
                while (true) {
                    String key = UUID.randomUUID().toString();

                    if (!grid(0).mapKeyToNode(null, key).equals(grid(0).localNode())) {
                        assertTrue(cache(1).putx(key, i));

                        break;
                    }
                }
            }
        }

        List<String> subList = keys.subList(1, keys.size());

        // ---------------
        // Key set checks.
        // ---------------

        info("Key set: " + cache(0).keySet());
        info("Entry set: " + cache(0).entrySet());
        info("Primary entry set: " + cache(0).primaryEntrySet());

        Set<String> primKeys = cache(0).primaryKeySet();

        assertEquals(3, primKeys.size());
        assertTrue(primKeys.containsAll(keys));

        primKeys = cache(0).projection(new P1<GridCacheEntry<String, Integer>>() {
            @Override public boolean apply(GridCacheEntry<String, Integer> e) {
                return !e.getKey().equals(keys.get(0));
            }
        }).primaryKeySet();

        assertEquals(2, primKeys.size());
        assertTrue(primKeys.containsAll(subList));

        // --------------
        // Values checks.
        // --------------

        Collection<Integer> primVals = cache(0).primaryValues();

        assertEquals(3, primVals.size());
        assertTrue(primVals.containsAll(F.asList(0, 1, 2)));

        primVals = cache(0).projection(new P1<GridCacheEntry<String, Integer>>() {
            @Override public boolean apply(GridCacheEntry<String, Integer> e) {
                return !e.getKey().equals(keys.get(0));
            }
        }).primaryValues();

        assertEquals(2, primVals.size());
        assertTrue(primVals.containsAll(F.asList(1, 2)));

        // -----------------
        // Entry set checks.
        // -----------------

        Set<GridCacheEntry<String, Integer>> primEntries = cache(0).primaryEntrySet();

        assertEquals(3, primEntries.size());

        primEntries = cache(0).projection(new P1<GridCacheEntry<String, Integer>>() {
            @Override public boolean apply(GridCacheEntry<String, Integer> e) {
                return !e.getKey().equals(keys.get(0));
            }
        }).primaryEntrySet();

        assertEquals(2, primEntries.size());
    }

    /**
     * @throws Exception In case of error.
     */
    public void testToMap() throws Exception {
        if (offheapTiered(cache()))
            return;

        cache().put("key1", 1);
        cache().put("key2", 2);

        Map<String, Integer> map = new HashMap<>();

        for (int i = 0; i < gridCount(); i++)
            map.putAll(cache(i).toMap());

        assert map != null;
        assert map.size() == 2;
        assert map.get("key1") == 1;
        assert map.get("key2") == 2;
    }

    /**
     * @param keys Expected keys.
     * @throws Exception If failed.
     */
    protected void checkSize(Collection<String> keys) throws Exception {
        if (nearEnabled())
            assertEquals(keys.size(), cache().size());
        else {
            for (int i = 0; i < gridCount(); i++) {
                GridCacheContext<String, Integer> ctx = context(i);

                if (offheapTiered(ctx.cache()))
                    continue;

                int size = 0;

                for (String key : keys) {
                    if (ctx.affinity().localNode(key, ctx.discovery().topologyVersion())) {
                        GridCacheEntryEx<String, Integer> e =
                            ctx.isNear() ? ctx.near().dht().peekEx(key) : ctx.cache().peekEx(key);

                        assert e != null : "Entry is null [idx=" + i + ", key=" + key + ", ctx=" + ctx + ']';
                        assert !e.deleted() : "Entry is deleted: " + e;

                        size++;
                    }
                }

                assertEquals("Incorrect size on cache #" + i, size, cache(i).size());
            }
        }
    }

    /**
     * @param keys Expected keys.
     * @throws Exception If failed.
     */
    protected void checkKeySize(Collection<String> keys) throws Exception {
        if (nearEnabled())
            assertEquals("Invalid key size: " + cache().keySet(), keys.size(), cache().size());
        else {
            for (int i = 0; i < gridCount(); i++) {
                GridCacheContext<String, Integer> ctx = context(i);

                int size = 0;

                for (String key : keys)
                    if (ctx.affinity().localNode(key, ctx.discovery().topologyVersion()))
                        size++;

                assertEquals("Incorrect key size on cache #" + i, size, cache(i).size());
            }
        }
    }

    /**
     * Construct cache projectnio for provided filters.
     *
     * @param cache Cache.
     * @param filters Filters.
     * @return Projection.
     */
    private GridCacheProjection<String, Integer> projection(GridCacheProjection<String, Integer> cache,
        @Nullable IgnitePredicate<GridCacheEntry<String, Integer>>... filters) {
        GridCacheProjection<String, Integer> res = cache;

        if (filters != null) {
            for (IgnitePredicate<GridCacheEntry<String, Integer>> filter : filters)
                res = res.projection(filter);
        }

        return res;
    }

    /**
     * @param exp Expected value.
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkContainsKey(boolean exp, String key) throws Exception {
        if (nearEnabled())
            assertEquals(exp, cache().containsKey(key));
        else {
            boolean contains = false;

            for (int i = 0; i < gridCount(); i++)
                if (containsKey(cache(i), key)) {
                    contains = true;

                    break;
                }

            assertEquals("Key: " + key, exp, contains);
        }
    }

    /**
     * @param exp Expected value.
     * @param key Key.
     * @param f Filter.
     * @throws Exception If failed.
     */
    private void checkProjectionContainsKey(boolean exp, String key,
        IgnitePredicate<GridCacheEntry<String, Integer>>... f) throws Exception {
        if (nearEnabled())
            assertEquals(exp, projection(cache(), f).containsKey(key));
        else {
            boolean contains = false;

            for (int i = 0; i < gridCount(); i++) {
                if (offheapTiered(cache(i)))
                    return;

                if (projection(cache(i), f).containsKey(key)) {
                    contains = true;

                    break;
                }
            }

            assertEquals("Key: " + key, exp, contains);
        }
    }

    /**
     * @param exp Expected value.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void checkContainsValue(boolean exp, Integer val) throws Exception {
        if (nearEnabled())
            assertEquals(exp, cache().containsValue(val));
        else {
            boolean contains = false;

            for (int i = 0; i < gridCount(); i++)
                if (containsValue(cache(i), val)) {
                    contains = true;

                    break;
                }

            assertEquals("Value: " + val, exp, contains);
        }
    }

    /**
     * @param exp Expected value.
     * @param val Value.
     * @param f Filter.
     * @throws Exception If failed.
     */
    private void checkProjectionContainsValue(boolean exp, Integer val,
        IgnitePredicate<GridCacheEntry<String, Integer>>... f) throws Exception {
        if (nearEnabled())
            assertEquals(exp, projection(cache(), f).containsValue(val));
        else {
            boolean contains = false;

            for (int i = 0; i < gridCount(); i++) {
                if (offheapTiered(cache(i)))
                    return;

                if (projection(cache(i), f).containsValue(val)) {
                    contains = true;

                    break;
                }
            }

            assertEquals("Value: " + val, exp, contains);
        }
    }

    /**
     * @param key Key.
     * @return Cache.
     * @throws Exception If failed.
     */
    protected GridCache<String, Integer> primaryCache(String key) throws Exception {
        ClusterNode node = cache().affinity().mapKeyToNode(key);

        if (node == null)
            throw new GridException("Failed to find primary node.");

        UUID nodeId = node.id();

        GridCache<String, Integer> cache = null;

        for (int i = 0; i < gridCount(); i++) {
            if (context(i).localNodeId().equals(nodeId)) {
                cache = cache(i);

                break;
            }
        }

        assert cache != null;

        return cache;
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     * @throws GridException If failed.
     */
    protected List<String> primaryKeysForCache(GridCacheProjection<String, Integer> cache, int cnt)
        throws GridException {
        return primaryKeysForCache(cache, cnt, 1);
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is primary.
     * @throws GridException If failed.
     */
    protected List<String> primaryKeysForCache(GridCacheProjection<String, Integer> cache, int cnt, int startFrom)
        throws GridException {
        List<String> found = new ArrayList<>(cnt);

        for (int i = startFrom; i < startFrom + 100_000; i++) {
            String key = "key" + i;

            if (cache.entry(key).primary()) {
                found.add(key);

                if (found.size() == cnt)
                    return found;
            }
        }

        throw new GridException("Unable to find " + cnt + " keys as primary for cache.");
    }
}
