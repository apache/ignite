/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Tests events.
 */
public abstract class GridCacheEventAbstractTest extends GridCacheAbstractSelfTest {
    /** */
    private static final boolean TEST_INFO = true;

    /** Wait timeout. */
    private static final long WAIT_TIMEOUT = 5000;

    /** Key. */
    private static final String KEY = "key";

    /** */
    private static volatile int gridCnt;

    /**
     * @return {@code True} if partitioned.
     */
    protected boolean partitioned() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        gridCnt = gridCount();

        for (int i = 0; i < gridCnt; i++)
            grid(i).events().localListen(new TestEventListener(partitioned()), EVTS_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (TEST_INFO)
            info("Called beforeTest() callback.");

        TestEventListener.reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (TEST_INFO)
            info("Called afterTest() callback.");

        TestEventListener.stopListen();

        try {
            super.afterTest();
        }
        finally {
            TestEventListener.listen();
        }
    }

    /**
     * Waits for event count on all nodes.
     *
     * @param gridIdx Grid index.
     * @param evtCnts Array of tuples with values: V1 - event type, V2 - expected event count on one node.
     * @throws InterruptedException If thread has been interrupted while waiting.
     */
    private void waitForEvents(int gridIdx, IgniteBiTuple<Integer, Integer>... evtCnts) throws Exception {
        if (!F.isEmpty(evtCnts))
            try {
                TestEventListener.waitForEventCount(((GridKernal)grid(0)).context(), evtCnts);
            }
            catch (GridException e) {
                printEventCounters(gridIdx, evtCnts);

                throw e;
            }
    }

    /**
     * @param gridIdx Grid index.
     * @param expCnts Expected counters
     */
    private void printEventCounters(int gridIdx, IgniteBiTuple<Integer, Integer>[] expCnts) {
        info("Printing counters [gridIdx=" + gridIdx + ']');

        for (IgniteBiTuple<Integer, Integer> t : expCnts) {
            Integer evtType = t.get1();

            int actCnt = TestEventListener.eventCount(evtType);

            info("Event [evtType=" + evtType + ", expCnt=" + t.get2() + ", actCnt=" + actCnt + ']');
        }
    }

    /**
     * Clear caches without generating events.
     *
     * @throws GridException If failed to clear caches.
     */
    private void clearCaches() throws GridException {
        for (int i = 0; i < gridCnt; i++) {
            GridCache<String, Integer> cache = cache(i);

            cache.removeAll();

            assert cache.isEmpty();
        }
    }

    /**
     * Runs provided {@link TestCacheRunnable} instance on all caches.
     *
     * @param run {@link TestCacheRunnable} instance.
     * @param evtCnts Expected event counts for each iteration.
     * @throws Exception In failed.
     */
    @SuppressWarnings({"CaughtExceptionImmediatelyRethrown"})
    private void runTest(TestCacheRunnable run, IgniteBiTuple<Integer, Integer>... evtCnts) throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            info(">>> Running test for grid [idx=" + i + ", grid=" + grid(i).name() +
                ", id=" + grid(i).localNode().id() + ']');

            try {
                run.run(cache(i));

                waitForEvents(i, evtCnts);
            }
            catch (Exception e) { // Leave this catch to be able to set breakpoint.
                throw e;
            }
            finally {
                // This call is mainly required to correctly clear event futures.
                TestEventListener.reset();

                clearCaches();

                // This call is required for the second time to reset counters for
                // the previous call.
                TestEventListener.reset();
            }
        }
    }

    /**
     * Get key-value pairs.
     *
     * @param size Pairs count.
     * @return Key-value pairs.
     */
    private Map<String, Integer> pairs(int size) {
        Map<String, Integer> pairs = new HashMap<>(size);

        for (int i = 1; i <= size; i++)
            pairs.put(KEY + i, i);

        return pairs;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testFilteredPut() throws Exception {
        GridCache<String, Integer> cache = grid(0).cache(null);

        String key = "1";
        int val = 1;

        assert !cache.putx(key, val, F.<String, Integer>cacheHasPeekValue());

        assert !cache.containsKey(key);

        assertEquals(0, TestEventListener.eventCount(EVT_CACHE_OBJECT_PUT));

        assert cache.putx(key, val);

        assert cache.containsKey(key);

        waitForEvents(0, F.t(EVT_CACHE_OBJECT_PUT, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testGetPutRemove() throws Exception {
        // TODO: GG-7578.
        if (cache(0).configuration().getCacheMode() == GridCacheMode.REPLICATED)
            return;

        runTest(
            new TestCacheRunnable() {
                @Override public void run(GridCache<String, Integer> cache) throws GridException {
                    String key = "key";
                    Integer val = 1;

                    assert cache.put(key, val) == null;

                    assert cache.containsKey(key);

                    assertEquals(val, cache.get(key));

                    assertEquals(val, cache.remove(key));

                    assert !cache.containsKey(key);
                }
            },
            F.t(EVT_CACHE_OBJECT_PUT, gridCnt),
            F.t(EVT_CACHE_OBJECT_READ, 3),
            F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt)
        );
    }

    /**
     * @throws Exception If test failed.
     */
    public void testGetPutRemoveTx1() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                GridCacheTx tx = cache.txStart();

                assert cache.put(key, val) == null;

                assert cache.containsKey(key);

                assert val.equals(cache.get(key));

                assert val.equals(cache.remove(key));

                assert !cache.containsKey(key);

                tx.commit();

                assert !cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testGetPutRemoveTx2() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                GridCacheTx tx = cache.txStart();

                assert cache.put(key, val) == null;

                assert cache.containsKey(key);

                assert val.equals(cache.get(key));

                assert val.equals(cache.remove(key));

                assert !cache.containsKey(key);

                assert cache.put(key, val) == null;

                assert cache.containsKey(key);

                tx.commit();

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testGetPutRemoveAsync() throws Exception {
        // TODO: GG-7578.
        if (cache(0).configuration().getCacheMode() == GridCacheMode.REPLICATED)
            return;

        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                assert cache.putAsync(key, val).get() == null;

                assert cache.containsKey(key);

                assert val.equals(cache.getAsync(key).get());

                assert val.equals(cache.removeAsync(key).get());

                assert !cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt), F.t(EVT_CACHE_OBJECT_READ, 3), F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testGetPutRemoveAsyncTx1() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                GridCacheTx tx = cache.txStart();

                assert cache.putAsync(key, val).get() == null;

                assert cache.containsKey(key);

                assert val.equals(cache.getAsync(key).get());

                assert val.equals(cache.removeAsync(key).get());

                assert !cache.containsKey(key);

                tx.commit();

                assert !cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testGetPutRemoveAsyncTx2() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                GridCacheTx tx = cache.txStart();

                assert cache.putAsync(key, val).get() == null;

                assert cache.containsKey(key);

                assert val.equals(cache.getAsync(key).get());

                assert val.equals(cache.removeAsync(key).get());

                assert !cache.containsKey(key);

                assert cache.putAsync(key, val).get() == null;

                assert cache.containsKey(key);

                tx.commit();

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPutRemovex() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                assert cache.putx(key, val);

                assert cache.containsKey(key);

                assert cache.removex(key);

                assert !cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt), F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPutRemovexTx1() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                GridCacheTx tx = cache.txStart();

                assert cache.putx(key, val);

                assert cache.containsKey(key);

                assert cache.removex(key);

                assert !cache.containsKey(key);

                tx.commit();

                assert !cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPutRemovexTx2() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                GridCacheTx tx = cache.txStart();

                assert cache.putx(key, val);

                assert cache.containsKey(key);

                assert cache.removex(key);

                assert !cache.containsKey(key);

                assert cache.putx(key, val);

                assert cache.containsKey(key);

                tx.commit();

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPutIfAbsent() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Iterator<Map.Entry<String, Integer>> iter = pairs(2).entrySet().iterator();

                Map.Entry<String, Integer> e = iter.next();

                String key = e.getKey();
                Integer val = e.getValue();

                assert cache.putIfAbsent(key, val) == null;
                assert val.equals(cache.putIfAbsent(key, val));

                assert cache.containsKey(key);

                e = iter.next();

                key = e.getKey();
                val = e.getValue();

                assert cache.putxIfAbsent(key, val);
                assert !cache.putxIfAbsent(key, val);

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, 2 * gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPutIfAbsentTx() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Iterator<Map.Entry<String, Integer>> iter = pairs(2).entrySet().iterator();

                GridCacheTx tx = cache.txStart();

                Map.Entry<String, Integer> e = iter.next();

                String key = e.getKey();
                Integer val = e.getValue();

                assert cache.putIfAbsent(key, val) == null;

                assertEquals(val, cache.putIfAbsent(key, val));

                assert cache.containsKey(key);

                e = iter.next();

                key = e.getKey();
                val = e.getValue();

                assert cache.putxIfAbsent(key, val);
                assert !cache.putxIfAbsent(key, val);

                assert cache.containsKey(key);

                tx.commit();

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, 2 * gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPutIfAbsentAsync() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Iterator<Map.Entry<String, Integer>> iter = pairs(2).entrySet().iterator();

                Map.Entry<String, Integer> e = iter.next();

                String key = e.getKey();
                Integer val = e.getValue();

                assert cache.putIfAbsentAsync(key, val).get() == null;
                assert val.equals(cache.putIfAbsentAsync(key, val).get());

                assert cache.containsKey(key);

                e = iter.next();

                key = e.getKey();
                val = e.getValue();

                assert cache.putxIfAbsentAsync(key, val).get();
                assert !cache.putxIfAbsentAsync(key, val).get();

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, 2 * gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    public void testPutIfAbsentAsyncTx() throws Exception {
        IgniteBiTuple[] evts = new IgniteBiTuple[] {F.t(EVT_CACHE_OBJECT_PUT, 2 * gridCnt), F.t(EVT_CACHE_OBJECT_READ, 1)};

        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Iterator<Map.Entry<String, Integer>> iter = pairs(2).entrySet().iterator();

                // Optimistic transaction.
                GridCacheTx tx = cache.txStart(OPTIMISTIC, REPEATABLE_READ);

                Map.Entry<String, Integer> e = iter.next();

                String key = e.getKey();
                Integer val = e.getValue();

                assert cache.putIfAbsentAsync(key, val).get() == null;
                assert val.equals(cache.putIfAbsentAsync(key, val).get());

                assert cache.containsKey(key);

                e = iter.next();

                key = e.getKey();
                val = e.getValue();

                assert cache.putxIfAbsentAsync(key, val).get();
                assert !cache.putxIfAbsentAsync(key, val).get();

                assert cache.containsKey(key);

                tx.commit();

                assert cache.containsKey(key);
            }
        }, evts);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testFilteredPutRemovex() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                GridPredicate<GridCacheEntry<String, Integer>> noPeekVal = F.cacheNoPeekValue();
                GridPredicate<GridCacheEntry<String, Integer>> hasPeekVal = F.cacheHasPeekValue();

                String key = e.getKey();
                Integer val = e.getValue();

                assert !cache.putx(key, val, hasPeekVal);
                assert cache.putx(key, val, noPeekVal);

                assert cache.containsKey(key);

                assert !cache.removex(key, noPeekVal);
                assert cache.removex(key, hasPeekVal);

                assert !cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt), F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testFilteredPutRemovexTx1() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                assert cache.keySet().isEmpty() : "Key set is not empty: " + cache().keySet();

                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                GridPredicate<GridCacheEntry<String, Integer>> noPeekVal = F.cacheNoPeekValue();
                GridPredicate<GridCacheEntry<String, Integer>> hasPeekVal = F.cacheHasPeekValue();

                String key = e.getKey();
                Integer val = e.getValue();

                // Optimistic.
                GridCacheTx tx = cache.txStart();

                assert !cache.putx(key, val, hasPeekVal);
                assert cache.putx(key, val, noPeekVal);

                assert cache.containsKey(key);

                assert !cache.removex(key, noPeekVal);
                assert cache.removex(key);

                assert !cache.containsKey(key);

                tx.commit();

                assert !cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testFilteredPutRemovexTx2() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(GridCache<String, Integer> cache) throws GridException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                GridPredicate<GridCacheEntry<String, Integer>> noPeekVal = F.cacheNoPeekValue();
                GridPredicate<GridCacheEntry<String, Integer>> hasPeekVal = F.cacheHasPeekValue();

                String key = e.getKey();
                Integer val = e.getValue();

                GridCacheTx tx = cache.txStart();

                assert !cache.putx(key, val, hasPeekVal);
                assert cache.putx(key, val, noPeekVal);

                assert cache.containsKey(key);

                assert !cache.removex(key, noPeekVal);
                assert cache.removex(key, hasPeekVal);

                assert !cache.containsKey(key);

                assert !cache.putx(key, val, hasPeekVal);
                assert cache.putx(key, val, noPeekVal);

                assert cache.containsKey(key);

                tx.commit();

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt));
    }

    /**
     *
     */
    private static interface TestCacheRunnable {
        /**
         * @param cache Cache.
         * @throws GridException If any exception occurs.
         */
        void run(GridCache<String, Integer> cache) throws GridException;
    }

    /**
     * Local event listener.
     */
    private static class TestEventListener implements GridPredicate<GridEvent> {
        /** Events count map. */
        private static ConcurrentMap<Integer, AtomicInteger> cntrs = new ConcurrentHashMap<>();

        /** Event futures. */
        private static Collection<EventTypeFuture> futs = new GridConcurrentHashSet<>();

        /** */
        private static volatile boolean listen = true;

        /** */
        private static boolean partitioned;

        /**
         * @param p Partitioned flag.
         */
        private TestEventListener(boolean p) {
            partitioned = p;
        }

        /**
         *
         */
        private static void listen() {
            listen = true;
        }

        /**
         *
         */
        private static void stopListen() {
            listen = false;
        }

        /**
         * @param type Event type.
         * @return Count.
         */
        static int eventCount(int type) {
            assert type > 0;

            AtomicInteger cntr = cntrs.get(type);

            return cntr != null ? cntr.get() : 0;
        }

        /**
         * Reset listener.
         */
        static void reset() {
            cntrs.clear();

            futs.clear();
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridEvent evt) {
            assert evt instanceof GridCacheEvent;

            if (!listen)
                return true;

            if (TEST_INFO)
                X.println("Cache event: " + evt.shortDisplay());

            AtomicInteger cntr = F.addIfAbsent(cntrs, evt.type(), F.newAtomicInt());

            assert cntr != null;

            int cnt = cntr.incrementAndGet();

            for (EventTypeFuture f : futs)
                f.onEvent(evt.type(), cnt);

            return true;
        }

        /**
         * Waits for event count.
         *
         * @param ctx Kernal context.
         * @param evtCnts Array of tuples with values: V1 - event type, V2 - expected event count.
         * @throws GridException If failed to wait.
         */
        private static void waitForEventCount(GridKernalContext ctx,
            IgniteBiTuple<Integer, Integer>... evtCnts) throws GridException {
            if (F.isEmpty(evtCnts))
                return;

            // Create future that aggregates all required event types.
            GridCompoundIdentityFuture<Object> cf = new GridCompoundIdentityFuture<>(ctx);

            for (IgniteBiTuple<Integer, Integer> t : evtCnts) {
                Integer evtType = t.get1();
                Integer expCnt = t.get2();

                assert expCnt != null && expCnt > 0;

                EventTypeFuture fut = new EventTypeFuture(ctx, evtType, expCnt, partitioned);

                futs.add(fut);

                // We need to account the window.
                AtomicInteger cntr = cntrs.get(evtType);

                if (!fut.isDone())
                    fut.onEvent(evtType, cntr != null ? cntr.get() : 0);

                cf.add(fut);
            }

            cf.markInitialized();

            try {
                cf.get(WAIT_TIMEOUT);
            }
            catch (GridFutureTimeoutException e) {
                throw new RuntimeException("Timed out waiting for events: " + cf, e);
            }
        }
    }

    /**
     *
     */
    private static class EventTypeFuture extends GridFutureAdapter<Object> {
        /** */
        private int evtType;

        /** */
        private int expCnt;

        /** */
        private int cnt;

        /** Partitioned flag. */
        private boolean partitioned;

        /**
         * For {@link Externalizable}.
         */
        public EventTypeFuture() {
            // No-op.
        }

        /**
         * @param ctx Kernal context.
         * @param evtType Event type.
         * @param expCnt Expected count.
         * @param partitioned Partitioned flag.
         */
        EventTypeFuture(GridKernalContext ctx, int evtType, int expCnt, boolean partitioned) {
            super(ctx);

            assert expCnt > 0;

            this.evtType = evtType;
            this.expCnt = expCnt;
            this.partitioned = partitioned;
        }

        /**
         * @return Count.
         */
        int count() {
            return cnt;
        }

        /**
         * @param evtType Event type.
         * @param cnt Count.
         */
        void onEvent(int evtType, int cnt) {
            if (isDone() || this.evtType != evtType)
                return;

            if (TEST_INFO)
                X.println("EventTypeFuture.onEvent() [evtName=" + U.gridEventName(evtType) + ", evtType=" + evtType +
                    ", cnt=" + cnt + ", expCnt=" + expCnt + ']');

            this.cnt = cnt;


            // For partitioned caches we allow extra event for reads.
            if (expCnt < cnt && (!partitioned || evtType != EVT_CACHE_OBJECT_READ || expCnt + 1 < cnt))
                onDone(new GridException("Wrong event count [evtName=" + U.gridEventName(evtType) + ", evtType=" +
                    evtType + ", expCnt=" + expCnt + ", actCnt=" + cnt + ", partitioned=" + partitioned + "]"));

            if (expCnt == cnt || (partitioned && expCnt + 1 == cnt))
                onDone();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EventTypeFuture.class, this, "evtName", U.gridEventName(evtType));
        }
    }
}
