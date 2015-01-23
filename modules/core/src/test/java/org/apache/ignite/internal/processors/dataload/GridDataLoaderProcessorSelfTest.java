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

package org.apache.ignite.internal.processors.dataload;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.GridCache;
import org.apache.ignite.cache.eviction.fifo.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.cache.configuration.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 *
 */
public class GridDataLoaderProcessorSelfTest extends GridCommonAbstractTest {
    /** */
    private static ConcurrentHashMap<Object, Object> storeMap;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private CacheMode mode = PARTITIONED;

    /** */
    private boolean nearEnabled = true;

    /** */
    private boolean useCache;

    /** */
    private boolean useGrpLock;

    /** */
    private TestStore store;

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        useCache = false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"IfMayBeConditional", "unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        cfg.setIncludeProperties();

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        if (useCache) {
            CacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(mode);
            cc.setAtomicityMode(TRANSACTIONAL);
            cc.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
            cc.setWriteSynchronizationMode(FULL_SYNC);

            cc.setEvictionPolicy(new CacheFifoEvictionPolicy(10000));

            cc.setEvictSynchronized(false);
            cc.setEvictNearSynchronized(false);

            if (store != null) {
                cc.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(store));
                cc.setReadThrough(true);
                cc.setWriteThrough(true);
            }

            cfg.setCacheConfiguration(cc);
        }
        else
            cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitioned() throws Exception {
        mode = PARTITIONED;

        checkDataLoader();
    }

    /**
     * @throws Exception If failed.
     */
    public void testColocated() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;

        checkDataLoader();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedGroupLock() throws Exception {
        mode = PARTITIONED;
        useGrpLock = true;

        checkDataLoader();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicated() throws Exception {
        mode = REPLICATED;

        checkDataLoader();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedGroupLock() throws Exception {
        mode = REPLICATED;
        useGrpLock = true;

        checkDataLoader();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocal() throws Exception {
        mode = LOCAL;

        try {
            checkDataLoader();

            assert false;
        }
        catch (IgniteCheckedException e) {
            // Cannot load local cache configured remotely.
            info("Caught expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private void checkDataLoader() throws Exception {
        try {
            Ignite g1 = startGrid(1);

            useCache = true;

            Ignite g2 = startGrid(2);
            Ignite g3 = startGrid(3);

            final IgniteDataLoader<Integer, Integer> ldr = g1.dataLoader(null);

            ldr.updater(useGrpLock ? GridDataLoadCacheUpdaters.<Integer, Integer>groupLocked() :
                GridDataLoadCacheUpdaters.<Integer, Integer>batchedSorted());

            final AtomicInteger idxGen = new AtomicInteger();
            final int cnt = 400;
            final int threads = 10;

            final CountDownLatch l1 = new CountDownLatch(threads);

            IgniteFuture<?> f1 = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Collection<IgniteFuture<?>> futs = new ArrayList<>(cnt);

                    for (int i = 0; i < cnt; i++) {
                        int idx = idxGen.getAndIncrement();

                        futs.add(ldr.addData(idx, idx));
                    }

                    l1.countDown();

                    for (IgniteFuture<?> fut : futs)
                        fut.get();

                    return null;
                }
            }, threads);

            l1.await();

            // This will wait until data loader finishes loading.
            stopGrid(getTestGridName(1), false);

            f1.get();

            int s2 = g2.cache(null).primaryKeySet().size();
            int s3 = g3.cache(null).primaryKeySet().size();
            int total = threads * cnt;

            assertEquals(total, s2 + s3);

            final IgniteDataLoader<Integer, Integer> rmvLdr = g2.dataLoader(null);

            rmvLdr.updater(useGrpLock ? GridDataLoadCacheUpdaters.<Integer, Integer>groupLocked() :
                GridDataLoadCacheUpdaters.<Integer, Integer>batchedSorted());

            final CountDownLatch l2 = new CountDownLatch(threads);

            IgniteFuture<?> f2 = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Collection<IgniteFuture<?>> futs = new ArrayList<>(cnt);

                    for (int i = 0; i < cnt; i++) {
                        final int key = idxGen.decrementAndGet();

                        futs.add(rmvLdr.removeData(key));
                    }

                    l2.countDown();

                    for (IgniteFuture<?> fut : futs)
                        fut.get();

                    return null;
                }
            }, threads);

            l2.await();

            rmvLdr.close(false);

            f2.get();

            s2 = g2.cache(null).primaryKeySet().size();
            s3 = g3.cache(null).primaryKeySet().size();

            assert s2 == 0 && s3 == 0 : "Incorrect entries count [s2=" + s2 + ", s3=" + s3 + ']';
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test primitive arrays can be passed into data loader.
     *
     * @throws Exception If failed.
     */
    public void testPrimitiveArrays() throws Exception {
        try {
            useCache = true;
            mode = PARTITIONED;

            Ignite g1 = startGrid(1);
            startGrid(2); // Reproduced only for several nodes in topology (if marshalling is used).

            List<Object> arrays = Arrays.<Object>asList(
                new byte[] {1}, new boolean[] {true, false}, new char[] {2, 3}, new short[] {3, 4},
                new int[] {4, 5}, new long[] {5, 6}, new float[] {6, 7}, new double[] {7, 8});

            IgniteDataLoader<Object, Object> dataLdr = g1.dataLoader(null);

            for (int i = 0, size = arrays.size(); i < 1000; i++) {
                Object arr = arrays.get(i % size);

                dataLdr.addData(i, arr);
                dataLdr.addData(i, fixedClosure(arr));
            }

            dataLdr.close(false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedMultiThreaded() throws Exception {
        mode = REPLICATED;

        checkLoaderMultithreaded(1, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedMultiThreadedGroupLock() throws Exception {
        mode = REPLICATED;
        useGrpLock = true;

        checkLoaderMultithreaded(1, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedMultiThreaded() throws Exception {
        mode = PARTITIONED;

        checkLoaderMultithreaded(1, 3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedMultiThreadedGroupLock() throws Exception {
        mode = PARTITIONED;
        useGrpLock = true;

        checkLoaderMultithreaded(1, 3);
    }

    /**
     * Tests loader in multithreaded environment with various count of grids started.
     *
     * @param nodesCntNoCache How many nodes should be started without cache.
     * @param nodesCntCache How many nodes should be started with cache.
     * @throws Exception If failed.
     */
    protected void checkLoaderMultithreaded(int nodesCntNoCache, int nodesCntCache)
        throws Exception {
        try {
            // Start all required nodes.
            int idx = 1;

            for (int i = 0; i < nodesCntNoCache; i++)
                startGrid(idx++);

            useCache = true;

            for (int i = 0; i < nodesCntCache; i++)
                startGrid(idx++);

            Ignite g1 = grid(1);

            // Get and configure loader.
            final IgniteDataLoader<Integer, Integer> ldr = g1.dataLoader(null);

            ldr.updater(useGrpLock ? GridDataLoadCacheUpdaters.<Integer, Integer>groupLocked() :
                GridDataLoadCacheUpdaters.<Integer, Integer>individual());
            ldr.perNodeBufferSize(2);

            // Define count of puts.
            final AtomicInteger idxGen = new AtomicInteger();

            final AtomicBoolean done = new AtomicBoolean();

            try {
                final int totalPutCnt = 50000;

                IgniteFuture<?> fut1 = multithreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        Collection<IgniteFuture<?>> futs = new ArrayList<>();

                        while (!done.get()) {
                            int idx = idxGen.getAndIncrement();

                            if (idx >= totalPutCnt) {
                                info(">>> Stopping producer thread since maximum count of puts reached.");

                                break;
                            }

                            futs.add(ldr.addData(idx, idx));
                        }

                        ldr.flush();

                        for (IgniteFuture<?> fut : futs)
                            fut.get();

                        return null;
                    }
                }, 5, "producer");

                IgniteFuture<?> fut2 = multithreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        while (!done.get()) {
                            ldr.flush();

                            U.sleep(100);
                        }

                        return null;
                    }
                }, 1, "flusher");

                // Define index of node being restarted.
                final int restartNodeIdx = nodesCntCache + nodesCntNoCache + 1;

                IgniteFuture<?> fut3 = multithreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try {
                            for (int i = 0; i < 5; i++) {
                                Ignite g = startGrid(restartNodeIdx);

                                UUID id = g.cluster().localNode().id();

                                info(">>>>>>> Started node: " + id);

                                U.sleep(1000);

                                stopGrid(getTestGridName(restartNodeIdx), true);

                                info(">>>>>>> Stopped node: " + id);
                            }
                        }
                        finally {
                            done.set(true);

                            info("Start stop thread finished.");
                        }

                        return null;
                    }
                }, 1, "start-stop-thread");

                fut1.get();
                fut2.get();
                fut3.get();
            }
            finally {
                ldr.close(false);
            }

            info("Cache size on second grid: " + grid(nodesCntNoCache + 1).cache(null).primaryKeySet().size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoaderApi() throws Exception {
        useCache = true;

        try {
            Ignite g1 = startGrid(1);

            IgniteDataLoader<Object, Object> ldr = g1.dataLoader(null);

            ldr.close(false);

            try {
                ldr.addData(0, 0);

                assert false;
            }
            catch (IllegalStateException e) {
                info("Caught expected exception: " + e);
            }

            assert ldr.future().isDone();

            ldr.future().get();

            try {
                // Create another loader.
                ldr = g1.dataLoader("UNKNOWN_CACHE");

                assert false;
            }
            catch (IllegalStateException e) {
                info("Caught expected exception: " + e);
            }

            ldr.close(true);

            assert ldr.future().isDone();

            ldr.future().get();

            // Create another loader.
            ldr = g1.dataLoader(null);

            // Cancel with future.
            ldr.future().cancel();

            try {
                ldr.addData(0, 0);

                assert false;
            }
            catch (IllegalStateException e) {
                info("Caught expected exception: " + e);
            }

            assert ldr.future().isDone();

            try {
                ldr.future().get();

                assert false;
            }
            catch (IgniteFutureCancelledException e) {
                info("Caught expected exception: " + e);
            }

            // Create another loader.
            ldr = g1.dataLoader(null);

            // This will close loader.
            stopGrid(getTestGridName(1), false);

            try {
                ldr.addData(0, 0);

                assert false;
            }
            catch (IllegalStateException e) {
                info("Caught expected exception: " + e);
            }

            assert ldr.future().isDone();

            ldr.future().get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Wraps integer to closure returning it.
     *
     * @param i Value to wrap.
     * @return Callable.
     */
    private static Callable<Integer> callable(@Nullable final Integer i) {
        return new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                return i;
            }
        };
    }

    /**
     * Wraps integer to closure returning it.
     *
     * @param i Value to wrap.
     * @return Closure.
     */
    private static IgniteClosure<Integer, Integer> closure(@Nullable final Integer i) {
        return new IgniteClosure<Integer, Integer>() {
            @Override public Integer apply(Integer e) {
                return e == null ? i : e + i;
            }
        };
    }

    /**
     * Wraps object to closure returning it.
     *
     * @param obj Value to wrap.
     * @return Closure.
     */
    private static <T> IgniteClosure<T, T> fixedClosure(@Nullable final T obj) {
        return new IgniteClosure<T, T>() {
            @Override public T apply(T e) {
                assert e == null || obj == null || e.getClass() == obj.getClass() :
                    "Expects the same types [e=" + e + ", obj=" + obj + ']';

                return obj;
            }
        };
    }

    /**
     * Wraps integer to closure expecting it and returning {@code null}.
     *
     * @param exp Expected closure value.
     * @return Remove expected cache value closure.
     */
    private static <T> IgniteClosure<T, T> removeClosure(@Nullable final T exp) {
        return new IgniteClosure<T, T>() {
            @Override public T apply(T act) {
                if (exp == null ? act == null : exp.equals(act))
                    return null;

                throw new AssertionError("Unexpected value [exp=" + exp + ", act=" + act + ']');
            }
        };
    }

    /**
     * @throws Exception If failed.
     */
    public void testFlush() throws Exception {
        mode = LOCAL;

        useCache = true;

        try {
            Ignite g = startGrid();

            final GridCache<Integer, Integer> c = g.cache(null);

            final IgniteDataLoader<Integer, Integer> ldr = g.dataLoader(null);

            ldr.perNodeBufferSize(10);

            for (int i = 0; i < 9; i++)
                ldr.addData(i, i);

            assertTrue(c.isEmpty());

            multithreaded(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    ldr.flush();

                    assertEquals(9, c.size());

                    return null;
                }
            }, 5, "flush-checker");

            ldr.addData(100, 100);

            ldr.flush();

            assertEquals(10, c.size());

            ldr.addData(200, 200);

            ldr.close(false);

            ldr.future().get();

            assertEquals(11, c.size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTryFlush() throws Exception {
        mode = LOCAL;

        useCache = true;

        try {
            Ignite g = startGrid();

            GridCache<Integer, Integer> c = g.cache(null);

            IgniteDataLoader<Integer, Integer> ldr = g.dataLoader(null);

            ldr.perNodeBufferSize(10);

            for (int i = 0; i < 9; i++)
                ldr.addData(i, i);

            assertTrue(c.isEmpty());

            ldr.tryFlush();

            Thread.sleep(100);

            assertEquals(9, c.size());

            ldr.close(false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFlushTimeout() throws Exception {
        mode = LOCAL;

        useCache = true;

        try {
            Ignite g = startGrid();

            final CountDownLatch latch = new CountDownLatch(9);

            g.events().localListen(new IgnitePredicate<IgniteEvent>() {
                @Override public boolean apply(IgniteEvent evt) {
                    latch.countDown();

                    return true;
                }
            }, EVT_CACHE_OBJECT_PUT);

            GridCache<Integer, Integer> c = g.cache(null);

            assertTrue(c.isEmpty());

            IgniteDataLoader<Integer, Integer> ldr = g.dataLoader(null);

            ldr.perNodeBufferSize(10);
            ldr.autoFlushFrequency(3000);

            for (int i = 0; i < 9; i++)
                ldr.addData(i, i);

            assertTrue(c.isEmpty());

            assertFalse(latch.await(1000, MILLISECONDS));

            assertTrue(c.isEmpty());

            assertTrue(latch.await(3000, MILLISECONDS));

            assertEquals(9, c.size());

            ldr.close(false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateStore() throws Exception {
        storeMap = new ConcurrentHashMap<>();

        try {
            store = new TestStore();

            useCache = true;

            Ignite ignite = startGrid(1);

            startGrid(2);
            startGrid(3);

            for (int i = 0; i < 1000; i++)
                storeMap.put(i, i);

            try (IgniteDataLoader<Object, Object> ldr = ignite.dataLoader(null)) {
                assertFalse(ldr.skipStore());

                for (int i = 0; i < 1000; i++)
                    ldr.removeData(i);

                for (int i = 1000; i < 2000; i++)
                    ldr.addData(i, i);
            }

            for (int i = 0; i < 1000; i++)
                assertNull(storeMap.get(i));

            for (int i = 1000; i < 2000; i++)
                assertEquals(i, storeMap.get(i));

            try (IgniteDataLoader<Object, Object> ldr = ignite.dataLoader(null)) {
                ldr.skipStore(true);

                for (int i = 0; i < 1000; i++)
                    ldr.addData(i, i);

                for (int i = 1000; i < 2000; i++)
                    ldr.removeData(i);
            }

            IgniteCache<Object, Object> cache = ignite.jcache(null);

            for (int i = 0; i < 1000; i++) {
                assertNull(storeMap.get(i));

                assertEquals(i, cache.get(i));
            }

            for (int i = 1000; i < 2000; i++) {
                assertEquals(i, storeMap.get(i));

                assertNull(cache.localPeek(i));
            }
        }
        finally {
            storeMap = null;
        }
    }

    /**
     *
     */
    private static class TestObject {
        /** Value. */
        private final int val;

        /**
         * @param val Value.
         */
        private TestObject(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject obj = (TestObject)o;

            return val == obj.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     *
     */
    private class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Nullable @Override public Object load(Object key) {
            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<?, ?> entry) {
            storeMap.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            storeMap.remove(key);
        }
    }
}
