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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.IgniteReflectionFactory;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 *
 */
public class DataStreamProcessorSelfTest extends GridCommonAbstractTest {
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

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setForceServerMode(true);
        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        cfg.setIncludeProperties();

        if (useCache) {
            CacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(mode);
            cc.setAtomicityMode(TRANSACTIONAL);

            if (nearEnabled) {
                NearCacheConfiguration nearCfg = new NearCacheConfiguration();

                cc.setNearConfiguration(nearCfg);
            }

            cc.setWriteSynchronizationMode(FULL_SYNC);

            cc.setEvictSynchronized(false);

            if (store != null) {
                cc.setCacheStoreFactory(new IgniteReflectionFactory<CacheStore>(TestStore.class));
                cc.setReadThrough(true);
                cc.setWriteThrough(true);
            }

            cfg.setCacheConfiguration(cc);
        }
        else {
            cfg.setCacheConfiguration();

            cfg.setClientMode(true);
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitioned() throws Exception {
        mode = PARTITIONED;

        checkDataStreamer();
    }

    /**
     * @throws Exception If failed.
     */
    public void testColocated() throws Exception {
        mode = PARTITIONED;
        nearEnabled = false;

        checkDataStreamer();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicated() throws Exception {
        mode = REPLICATED;

        checkDataStreamer();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocal() throws Exception {
        mode = LOCAL;

        try {
            checkDataStreamer();

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
    private void checkDataStreamer() throws Exception {
        try {
            Ignite g1 = startGrid(1);

            useCache = true;

            Ignite g2 = startGrid(2);
            startGrid(3);

            final IgniteDataStreamer<Integer, Integer> ldr = g1.dataStreamer(null);

            ldr.receiver(DataStreamerCacheUpdaters.<Integer, Integer>batchedSorted());

            final AtomicInteger idxGen = new AtomicInteger();
            final int cnt = 400;
            final int threads = 10;

            final CountDownLatch l1 = new CountDownLatch(threads);

            IgniteInternalFuture<?> f1 = multithreadedAsync(new Callable<Object>() {
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

            // This will wait until data streamer finishes loading.
            stopGrid(getTestGridName(1), false);

            f1.get();

            int s2 = grid(2).cache(null).localSize(CachePeekMode.PRIMARY);
            int s3 = grid(3).cache(null).localSize(CachePeekMode.PRIMARY);
            int total = threads * cnt;

            assertEquals(total, s2 + s3);

            final IgniteDataStreamer<Integer, Integer> rmvLdr = g2.dataStreamer(null);

            rmvLdr.receiver(DataStreamerCacheUpdaters.<Integer, Integer>batchedSorted());

            final CountDownLatch l2 = new CountDownLatch(threads);

            IgniteInternalFuture<?> f2 = multithreadedAsync(new Callable<Object>() {
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

            s2 = grid(2).cache(null).localSize(CachePeekMode.PRIMARY);
            s3 = grid(3).cache(null).localSize(CachePeekMode.PRIMARY);

            assert s2 == 0 && s3 == 0 : "Incorrect entries count [s2=" + s2 + ", s3=" + s3 + ']';
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedIsolated() throws Exception {
        mode = PARTITIONED;

        checkIsolatedDataStreamer();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedIsolated() throws Exception {
        mode = REPLICATED;

        checkIsolatedDataStreamer();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkIsolatedDataStreamer() throws Exception {
        try {
            useCache = true;

            Ignite g1 = startGrid(0);
            startGrid(1);
            startGrid(2);

            awaitPartitionMapExchange();

            IgniteCache<Integer, Integer> cache = grid(0).cache(null);

            for (int i = 0; i < 100; i++)
                cache.put(i, -1);

            final int cnt = 40_000;
            final int threads = 10;

            try (final IgniteDataStreamer<Integer, Integer> ldr = g1.dataStreamer(null)) {
                final AtomicInteger idxGen = new AtomicInteger();

                IgniteInternalFuture<?> f1 = multithreadedAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        for (int i = 0; i < cnt; i++) {
                            int idx = idxGen.getAndIncrement();

                            ldr.addData(idx, idx);
                        }

                        return null;
                    }
                }, threads);

                f1.get();
            }

            for (int g = 0; g < 3; g++) {
                ClusterNode locNode = grid(g).localNode();

                GridCacheAdapter<Integer, Integer> cache0 = ((IgniteKernal)grid(g)).internalCache(null);

                if (cache0.isNear())
                    cache0 = ((GridNearCacheAdapter<Integer, Integer>)cache0).dht();

                Affinity<Integer> aff = cache0.affinity();

                for (int key = 0; key < cnt * threads; key++) {
                    if (aff.isPrimary(locNode, key) || aff.isBackup(locNode, key)) {
                        GridCacheEntryEx entry = cache0.peekEx(key);

                        assertNotNull("Missing entry for key: " + key, entry);
                        assertEquals(new Integer((key < 100 ? -1 : key)),
                            CU.value(entry.rawGetOrUnmarshal(false), cache0.context(), false));
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test primitive arrays can be passed into data streamer.
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

            IgniteDataStreamer<Object, Object> dataLdr = g1.dataStreamer(null);

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
    public void testPartitionedMultiThreaded() throws Exception {
        mode = PARTITIONED;

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
            final IgniteDataStreamer<Integer, Integer> ldr = g1.dataStreamer(null);

            ldr.receiver(DataStreamerCacheUpdaters.<Integer, Integer>individual());
            ldr.perNodeBufferSize(2);

            // Define count of puts.
            final AtomicInteger idxGen = new AtomicInteger();

            final AtomicBoolean done = new AtomicBoolean();

            try {
                final int totalPutCnt = 50000;

                IgniteInternalFuture<?> fut1 = multithreadedAsync(new Callable<Object>() {
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

                IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable<Object>() {
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

                IgniteInternalFuture<?> fut3 = multithreadedAsync(new Callable<Object>() {
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

            IgniteDataStreamer<Object, Object> ldr = g1.dataStreamer(null);

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
                ldr = g1.dataStreamer("UNKNOWN_CACHE");

                assert false;
            }
            catch (IllegalStateException e) {
                info("Caught expected exception: " + e);
            }

            ldr.close(true);

            assert ldr.future().isDone();

            ldr.future().get();

            // Create another loader.
            ldr = g1.dataStreamer(null);

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
            ldr = g1.dataStreamer(null);

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

            final IgniteCache<Integer, Integer> c = g.cache(null);

            final IgniteDataStreamer<Integer, Integer> ldr = g.dataStreamer(null);

            ldr.perNodeBufferSize(10);

            for (int i = 0; i < 9; i++)
                ldr.addData(i, i);

            assertTrue(c.localSize() == 0);

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

            IgniteCache<Integer, Integer> c = g.cache(null);

            IgniteDataStreamer<Integer, Integer> ldr = g.dataStreamer(null);

            ldr.perNodeBufferSize(10);

            for (int i = 0; i < 9; i++)
                ldr.addData(i, i);

            assertTrue(c.localSize() == 0);

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

            g.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    latch.countDown();

                    return true;
                }
            }, EVT_CACHE_OBJECT_PUT);

            IgniteCache<Integer, Integer> c = g.cache(null);

            assertTrue(c.localSize() == 0);

            IgniteDataStreamer<Integer, Integer> ldr = g.dataStreamer(null);

            ldr.perNodeBufferSize(10);
            ldr.autoFlushFrequency(3000);
            ldr.allowOverwrite(true);

            for (int i = 0; i < 9; i++)
                ldr.addData(i, i);

            assertTrue(c.localSize() == 0);

            assertFalse(latch.await(1000, MILLISECONDS));

            assertTrue(c.localSize() == 0);

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

            try (IgniteDataStreamer<Object, Object> ldr = ignite.dataStreamer(null)) {
                ldr.allowOverwrite(true);

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

            try (IgniteDataStreamer<Object, Object> ldr = ignite.dataStreamer(null)) {
                ldr.allowOverwrite(true);

                ldr.skipStore(true);

                for (int i = 0; i < 1000; i++)
                    ldr.addData(i, i);

                for (int i = 1000; i < 2000; i++)
                    ldr.removeData(i);
            }

            IgniteCache<Object, Object> cache = ignite.cache(null);

            for (int i = 0; i < 1000; i++) {
                assertNull(storeMap.get(i));

                assertEquals(i, cache.get(i));
            }

            for (int i = 1000; i < 2000; i++) {
                assertEquals(i, storeMap.get(i));

                assertNull(cache.localPeek(i, CachePeekMode.ONHEAP));
            }
        }
        finally {
            storeMap = null;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomUserUpdater() throws Exception {
        useCache = true;

        try {
            Ignite ignite = startGrid(1);

            startGrid(2);
            startGrid(3);

            try (IgniteDataStreamer<String, TestObject> ldr = ignite.dataStreamer(null)) {
                ldr.allowOverwrite(true);

                ldr.receiver(getStreamReceiver());

                for (int i = 0; i < 100; i++)
                    ldr.addData(String.valueOf(i), new TestObject(i));
            }

            IgniteCache<String, TestObject> cache = ignite.cache(null);

            for (int i = 0; i < 100; i++) {
                TestObject val = cache.get(String.valueOf(i));

                assertNotNull(val);
                assertEquals(i + 1, val.val);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public static class TestObject {
        /** Value. */
        public final int val;

        /**
         * @param val Value.
         */
        public TestObject(int val) {
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
     * @return Stream receiver.
     */
    protected StreamReceiver<String, TestObject> getStreamReceiver() {
        return new TestDataReceiver();
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Nullable @Override public Object load(Object key) {
            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) {
            storeMap.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            storeMap.remove(key);
        }
    }

    /**
     *
     */
    private static class TestDataReceiver implements StreamReceiver<String, TestObject> {
        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<String, TestObject> cache,
            Collection<Map.Entry<String, TestObject>> entries) {
            for (Map.Entry<String, TestObject> e : entries) {
                assertTrue(e.getKey() instanceof String);
                assertTrue(e.getValue() instanceof TestObject);

                cache.put(e.getKey(), new TestObject(e.getValue().val + 1));
            }
        }
    }
}