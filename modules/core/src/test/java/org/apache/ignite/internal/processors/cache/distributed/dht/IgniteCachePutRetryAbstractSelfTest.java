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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public abstract class IgniteCachePutRetryAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    protected static final long DURATION = GridTestUtils.SF.applyLB(30_000, 7_000);

    /** */
    protected static final int GRID_CNT = 4;

    /**
     * @return Keys count for the test.
     */
    protected int keysCount() {
        return 2_000;
    }

    /**
     * @param evict If {@code true} adds eviction policy.
     * @param store If {@code true} adds cache store.
     * @return Cache configuration.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration cacheConfiguration(boolean evict, boolean store) throws Exception {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cfg.setAtomicityMode(atomicityMode());
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setBackups(1);
        cfg.setRebalanceMode(SYNC);

        if (evict) {
            LruEvictionPolicy plc = new LruEvictionPolicy();

            plc.setMaxSize(100);

            cfg.setEvictionPolicy(plc);

            cfg.setOnheapCacheEnabled(true);
        }

        if (store) {
            cfg.setCacheStoreFactory(new TestStoreFactory());
            cfg.setWriteThrough(true);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes();

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        AtomicConfiguration acfg = new AtomicConfiguration();

        acfg.setBackups(1);

        cfg.setAtomicConfiguration(acfg);

        cfg.setIncludeEventTypes(new int[0]);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        try {
            checkInternalCleanup();
        }
        finally {
            ignite(0).destroyCache(DEFAULT_CACHE_NAME);
        }
    }

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testPut() throws Exception {
        checkRetry(Test.PUT, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testGetAndPut() throws Exception {
        checkRetry(Test.GET_AND_PUT, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testPutStoreEnabled() throws Exception {
        checkRetry(Test.PUT, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testPutAll() throws Exception {
        checkRetry(Test.PUT_ALL, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testPutAsync() throws Exception {
        checkRetry(Test.PUT_ASYNC, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testPutAsyncStoreEnabled() throws Exception {
        checkRetry(Test.PUT_ASYNC, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testInvoke() throws Exception {
        checkRetry(Test.INVOKE, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testInvokeAll() throws Exception {
        checkRetry(Test.INVOKE_ALL, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testInvokeAllEvict() throws Exception {
        checkRetry(Test.INVOKE_ALL, true, false);
    }

    /**
     * @param test Test type.
     * @param evict If {@code true} uses eviction policy
     * @param store If {@code true} uses cache with store.
     * @throws Exception If failed.
     */
    protected final void checkRetry(Test test, boolean evict, boolean store) throws Exception {
        ignite(0).createCache(cacheConfiguration(evict, store));

        final AtomicBoolean finished = new AtomicBoolean();

        int keysCnt = keysCount();

        IgniteInternalFuture<Object> fut = runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                while (!finished.get()) {
                    stopGrid(3);

                    U.sleep(300);

                    startGrid(3);

                    if (rnd.nextBoolean()) // OPC possible only when there is no migration from one backup to another.
                        awaitPartitionMapExchange();
                }

                return null;
            }
        });

        final IgniteCache<Integer, Integer> cache = ignite(0).cache(DEFAULT_CACHE_NAME);

        int iter = 0;

        try {
            long stopTime = System.currentTimeMillis() + DURATION;

            switch (test) {
                case PUT: {
                    while (System.currentTimeMillis() < stopTime) {
                        Integer val = ++iter;

                        for (int i = 0; i < keysCnt; i++)
                            cache.put(i, val);

                        for (int i = 0; i < keysCnt; i++)
                            assertEquals(val, cache.get(i));
                    }

                    break;
                }

                case GET_AND_PUT: {
                    for (int i = 0; i < keysCnt; i++)
                        cache.put(i, 0);

                    while (System.currentTimeMillis() < stopTime) {
                        Integer expOld = iter;

                        Integer val = ++iter;

                        for (int i = 0; i < keysCnt; i++) {
                            Integer old = cache.getAndPut(i, val);

                            assertTrue("Unexpected old value [old=" + old + ", exp=" + expOld + ']',
                                expOld.equals(old) || val.equals(old));
                        }

                        for (int i = 0; i < keysCnt; i++)
                            assertEquals(val, cache.get(i));
                    }

                    break;
                }

                case TX_PUT: {
                    while (System.currentTimeMillis() < stopTime) {
                        final Integer val = ++iter;

                        Ignite ignite = ignite(0);

                        for (int i = 0; i < keysCnt; i++) {
                            final Integer key = i;

                            doInTransaction(ignite, new Callable<Void>() {
                                @Override public Void call() throws Exception {
                                    cache.put(key, val);

                                    return null;
                                }
                            });
                        }

                        for (int i = 0; i < keysCnt; i++)
                            assertEquals(val, cache.get(i));
                    }

                    break;
                }

                case PUT_ALL: {
                    while (System.currentTimeMillis() < stopTime) {
                        Integer val = ++iter;

                        Map<Integer, Integer> map = new LinkedHashMap<>();

                        for (int i = 0; i < keysCnt; i++) {
                            map.put(i, val);

                            if (map.size() == 100 || i == keysCnt - 1) {
                                cache.putAll(map);

                                map.clear();
                            }
                        }

                        for (int i = 0; i < keysCnt; i++)
                            assertEquals(val, cache.get(i));
                    }
                }

                case PUT_ASYNC: {
                    while (System.currentTimeMillis() < stopTime) {
                        Integer val = ++iter;

                        for (int i = 0; i < keysCnt; i++)
                            cache.putAsync(i, val).get();

                        for (int i = 0; i < keysCnt; i++)
                            assertEquals(val, cache.getAsync(i).get());
                    }

                    break;
                }

                case INVOKE: {
                    while (System.currentTimeMillis() < stopTime) {
                        Integer val = ++iter;

                        Integer expOld = iter - 1;

                        for (int i = 0; i < keysCnt; i++) {
                            Integer old = cache.invoke(i, new SetEntryProcessor(val));

                            assertNotNull(old);
                            assertTrue(old.equals(expOld) || old.equals(val));
                        }

                        for (int i = 0; i < keysCnt; i++)
                            assertEquals(val, cache.get(i));
                    }

                    break;
                }

                case INVOKE_ALL: {
                    while (System.currentTimeMillis() < stopTime) {
                        Integer val = ++iter;

                        Integer expOld = iter - 1;

                        Set<Integer> keys = new LinkedHashSet<>();

                        for (int i = 0; i < keysCnt; i++) {
                            keys.add(i);

                            if (keys.size() == 100 || i == keysCnt - 1) {
                                Map<Integer, EntryProcessorResult<Integer>> resMap =
                                    cache.invokeAll(keys, new SetEntryProcessor(val));

                                for (Integer key : keys) {
                                    EntryProcessorResult<Integer> res = resMap.get(key);

                                    assertNotNull(res);

                                    Integer old = res.get();

                                    assertTrue(old.equals(expOld) || old.equals(val));
                                }

                                assertEquals(keys.size(), resMap.size());

                                keys.clear();
                            }
                        }

                        for (int i = 0; i < keysCnt; i++)
                            assertEquals(val, cache.get(i));
                    }

                    break;
                }

                default:
                    assert false : test;
            }
        }
        finally {
            finished.set(true);
            fut.get();
        }

        for (int i = 0; i < keysCnt; i++)
            assertEquals((Integer)iter, cache.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkInternalCleanup() throws Exception {
        checkNoAtomicFutures();

        checkOnePhaseCommitReturnValuesCleaned(GRID_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNoAtomicFutures() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            final IgniteKernal ignite = (IgniteKernal)grid(i);

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return ignite.context().cache().context().mvcc().atomicFuturesCount() == 0;
                }
            }, 5_000);

            Collection<?> futs = ignite.context().cache().context().mvcc().atomicFutures();

            assertTrue("Unexpected atomic futures: " + futs, futs.isEmpty());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testFailsWithNoRetries() throws Exception {
        checkFailsWithNoRetries(false);
    }

    /**
     * @throws Exception If failed.
     */
    @org.junit.Test
    public void testFailsWithNoRetriesAsync() throws Exception {
        checkFailsWithNoRetries(true);
    }

    /**
     * @param async If {@code true} tests asynchronous put.
     * @throws Exception If failed.
     */
    private void checkFailsWithNoRetries(boolean async) throws Exception {
        ignite(0).createCache(cacheConfiguration(false, false));

        final AtomicBoolean finished = new AtomicBoolean();

        IgniteInternalFuture<Object> fut = runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    stopGrid(3);

                    startGrid(3);

                    awaitPartitionMapExchange();
                }

                return null;
            }
        });

        try {
            int keysCnt = keysCount();

            boolean eThrown = false;

            IgniteCache<Object, Object> cache = ignite(0).cache(DEFAULT_CACHE_NAME).withNoRetries();

            long stopTime = System.currentTimeMillis() + 60_000;

            while (System.currentTimeMillis() < stopTime) {
                for (int i = 0; i < keysCnt; i++) {
                    try {
                        if (async)
                            cache.putAsync(i, i).get();
                        else
                            cache.put(i, i);
                    }
                    catch (Exception e) {
                        assertTrue("Invalid exception: " + e,
                            X.hasCause(e, ClusterTopologyCheckedException.class, CachePartialUpdateException.class));

                        eThrown = true;

                        break;
                    }
                }

                if (eThrown)
                    break;
            }

            assertTrue(eThrown);

            finished.set(true);

            fut.get();
        }
        finally {
            finished.set(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60 * 1000;
    }

    /**
     *
     */
    enum Test {
        /** */
        PUT,

        /** */
        GET_AND_PUT,

        /** */
        PUT_ALL,

        /** */
        PUT_ASYNC,

        /** */
        INVOKE,

        /** */
        INVOKE_ALL,

        /** */
        TX_PUT
    }

    /**
     *
     */
    class SetEntryProcessor implements CacheEntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer val;

        /**
         * @param val Value.
         */
        public SetEntryProcessor(Integer val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args) {
            Integer old = e.getValue();

            e.setValue(val);

            return old == null ? 0 : old;
        }
    }

    private static class TestStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestCacheStore();
        }
    }

    /**
     *
     */
    private static class TestCacheStore extends CacheStoreAdapter {
        /** Store map. */
        private static Map STORE_MAP = new ConcurrentHashMap();

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return STORE_MAP.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
            STORE_MAP.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            STORE_MAP.remove(key);
        }
    }
}
