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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.events.EventType.EVTS_CACHE;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

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
                TestEventListener.waitForEventCount(evtCnts);
            }
            catch (IgniteCheckedException e) {
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
     */
    private void clearCaches() {
        for (int i = 0; i < gridCnt; i++) {
            IgniteCache<String, Integer> cache = jcache(i);

            cache.removeAll();

            assert cache.localSize() == 0;
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
                run.run(jcache(i));

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
     *
     * Note: test was disabled for REPPLICATED cache case because IGNITE-607.
     * This comment should be removed if test passed stably.
     */
    public void testGetPutRemove() throws Exception {
        runTest(
            new TestCacheRunnable() {
                @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                    String key = "key";
                    Integer val = 1;

                    assert cache.getAndPut(key, val) == null;

                    assert cache.containsKey(key);

                    assertEquals(val, cache.get(key));

                    assertEquals(val, cache.getAndRemove(key));

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
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                Transaction tx = cache.unwrap(Ignite.class).transactions().txStart();

                assert cache.getAndPut(key, val) == null;

                assert cache.containsKey(key);

                assert val.equals(cache.get(key));

                assert val.equals(cache.getAndRemove(key));

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
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                Transaction tx = cache.unwrap(Ignite.class).transactions().txStart();

                assert cache.getAndPut(key, val) == null;

                assert cache.containsKey(key);

                assert val.equals(cache.get(key));

                assert val.equals(cache.getAndRemove(key));

                assert !cache.containsKey(key);

                cache.put(key, val);

                assert cache.containsKey(key);

                tx.commit();

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     *
     * Note: test was disabled for REPPLICATED cache case because IGNITE-607.
     * This comment should be removed if test passed stably.
     */
    public void testGetPutRemoveAsync() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                IgniteCache<String, Integer> asyncCache = cache.withAsync();

                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                asyncCache.getAndPut(key, val);

                assert asyncCache.future().get() == null;

                assert cache.containsKey(key);

                asyncCache.get(key);

                assert val.equals(asyncCache.future().get());

                asyncCache.getAndRemove(key);

                assert val.equals(asyncCache.future().get());

                assert !cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt), F.t(EVT_CACHE_OBJECT_READ, 3), F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testGetPutRemoveAsyncTx1() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                IgniteCache<String, Integer> asyncCache = cache.withAsync();

                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                Transaction tx = cache.unwrap(Ignite.class).transactions().txStart();

                asyncCache.getAndPut(key, val);

                assert asyncCache.future().get() == null;

                assert cache.containsKey(key);

                asyncCache.get(key);

                assert val.equals(asyncCache.future().get());

                asyncCache.getAndRemove(key);

                assert val.equals(asyncCache.future().get());

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
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                IgniteCache<String, Integer> asyncCache = cache.withAsync();

                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                Transaction tx = cache.unwrap(Ignite.class).transactions().txStart();

                asyncCache.getAndPut(key, val);

                assert asyncCache.future().get() == null;

                assert cache.containsKey(key);

                asyncCache.get(key);

                assert val.equals(asyncCache.future().get());

                asyncCache.getAndRemove(key);

                assert val.equals(asyncCache.future().get());

                assert !cache.containsKey(key);

                asyncCache.getAndPut(key, val);

                assert asyncCache.future().get() == null;

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
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                cache.put(key, val);

                assert cache.containsKey(key);

                assert cache.remove(key);

                assert !cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, gridCnt), F.t(EVT_CACHE_OBJECT_REMOVED, gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPutRemovexTx1() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                Transaction tx = cache.unwrap(Ignite.class).transactions().txStart();

                cache.put(key, val);

                assert cache.containsKey(key);

                assert cache.remove(key);

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
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                Map.Entry<String, Integer> e = F.first(pairs(1).entrySet());

                assert e != null;

                String key = e.getKey();
                Integer val = e.getValue();

                Transaction tx = cache.unwrap(Ignite.class).transactions().txStart();

                cache.put(key, val);

                assert cache.containsKey(key);

                assert cache.remove(key);

                assert !cache.containsKey(key);

                cache.put(key, val);

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
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                Iterator<Map.Entry<String, Integer>> iter = pairs(2).entrySet().iterator();

                Map.Entry<String, Integer> e = iter.next();

                String key = e.getKey();
                Integer val = e.getValue();

                assert cache.getAndPutIfAbsent(key, val) == null;
                assert val.equals(cache.getAndPutIfAbsent(key, val));

                assert cache.containsKey(key);

                e = iter.next();

                key = e.getKey();
                val = e.getValue();

                assert cache.putIfAbsent(key, val);
                assert !cache.putIfAbsent(key, val);

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, 2 * gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPutIfAbsentTx() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                Iterator<Map.Entry<String, Integer>> iter = pairs(2).entrySet().iterator();

                Map.Entry<String, Integer> e = iter.next();

                String key = e.getKey();
                Integer val = e.getValue();

                try (Transaction tx = grid(0).transactions().txStart();) {
                    assert cache.getAndPutIfAbsent(key, val) == null;

                    assertEquals(val, cache.getAndPutIfAbsent(key, val));

                    assert cache.containsKey(key);

                    e = iter.next();

                    key = e.getKey();
                    val = e.getValue();

                    assert cache.putIfAbsent(key, val);
                    assert !cache.putIfAbsent(key, val);

                    assert cache.containsKey(key);

                    tx.commit();
                }

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, 2 * gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPutIfAbsentAsync() throws Exception {
        runTest(new TestCacheRunnable() {
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                IgniteCache<String, Integer> asyncCache = cache.withAsync();

                Iterator<Map.Entry<String, Integer>> iter = pairs(2).entrySet().iterator();

                Map.Entry<String, Integer> e = iter.next();

                String key = e.getKey();
                Integer val = e.getValue();

                asyncCache.getAndPutIfAbsent(key, val);

                assert asyncCache.future().get() == null;

                asyncCache.getAndPutIfAbsent(key, val);

                assert val.equals(asyncCache.future().get());

                assert cache.containsKey(key);

                e = iter.next();

                key = e.getKey();
                val = e.getValue();

                asyncCache.putIfAbsent(key, val);

                assert ((Boolean)asyncCache.future().get()).booleanValue();

                asyncCache.putIfAbsent(key, val);

                assert !((Boolean)asyncCache.future().get()).booleanValue();

                assert cache.containsKey(key);
            }
        }, F.t(EVT_CACHE_OBJECT_PUT, 2 * gridCnt));
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    public void testPutIfAbsentAsyncTx() throws Exception {
        IgniteBiTuple[] evts = new IgniteBiTuple[] {F.t(EVT_CACHE_OBJECT_PUT, 2 * gridCnt)};

        runTest(new TestCacheRunnable() {
            @Override public void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException {
                IgniteCache<String, Integer> asyncCache = cache.withAsync();

                Iterator<Map.Entry<String, Integer>> iter = pairs(2).entrySet().iterator();

                // Optimistic transaction.
                try (Transaction tx = cache.unwrap(Ignite.class).transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                    Map.Entry<String, Integer> e = iter.next();

                    String key = e.getKey();
                    Integer val = e.getValue();

                    asyncCache.getAndPutIfAbsent(key, val);

                    assert asyncCache.future().get() == null;

                    asyncCache.getAndPutIfAbsent(key, val);

                    assert val.equals(asyncCache.future().get());

                    assert cache.containsKey(key);

                    e = iter.next();

                    key = e.getKey();
                    val = e.getValue();

                    asyncCache.putIfAbsent(key, val);

                    assert ((Boolean)asyncCache.future().get()).booleanValue();

                    asyncCache.putIfAbsent(key, val);

                    assert !((Boolean)asyncCache.future().get()).booleanValue();

                    assert cache.containsKey(key);

                    tx.commit();

                    assert cache.containsKey(key);
                }
            }
        }, evts);
    }

    /**
     *
     */
    private static interface TestCacheRunnable {
        /**
         * @param cache Cache.
         * @throws IgniteCheckedException If any exception occurs.
         */
        void run(IgniteCache<String, Integer> cache) throws IgniteCheckedException;
    }

    /**
     * Local event listener.
     */
    private static class TestEventListener implements IgnitePredicate<Event> {
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
        @Override public boolean apply(Event evt) {
            assert evt instanceof CacheEvent;

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
         * @param evtCnts Array of tuples with values: V1 - event type, V2 - expected event count.
         * @throws IgniteCheckedException If failed to wait.
         */
        private static void waitForEventCount(IgniteBiTuple<Integer, Integer>... evtCnts)
            throws IgniteCheckedException {
            if (F.isEmpty(evtCnts))
                return;

            // Create future that aggregates all required event types.
            GridCompoundIdentityFuture<Object> cf = new GridCompoundIdentityFuture<>();

            for (IgniteBiTuple<Integer, Integer> t : evtCnts) {
                Integer evtType = t.get1();
                Integer expCnt = t.get2();

                assert expCnt != null && expCnt > 0;

                EventTypeFuture fut = new EventTypeFuture(evtType, expCnt, partitioned);

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
            catch (IgniteFutureTimeoutCheckedException e) {
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
         * @param evtType Event type.
         * @param expCnt Expected count.
         * @param partitioned Partitioned flag.
         */
        EventTypeFuture(int evtType, int expCnt, boolean partitioned) {
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
                onDone(new IgniteCheckedException("Wrong event count [evtName=" + U.gridEventName(evtType) + ", evtType=" +
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