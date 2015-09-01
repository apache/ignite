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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 *
 */
public abstract class IgniteCachePutRetryAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /**
     * @return Keys count for the test.
     */
    private int keysCount() {
        return 10_000;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setAtomicWriteOrderMode(writeOrderMode());
        cfg.setBackups(1);
        cfg.setRebalanceMode(SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        AtomicConfiguration acfg = new AtomicConfiguration();

        acfg.setBackups(1);

        cfg.setAtomicConfiguration(acfg);

        return cfg;
    }

    /**
     * @return Write order mode.
     */
    protected CacheAtomicWriteOrderMode writeOrderMode() {
        return CLOCK;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        checkRetry(Test.PUT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        checkRetry(Test.PUT_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAsync() throws Exception {
        checkRetry(Test.PUT_ASYNC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvoke() throws Exception {
        checkRetry(Test.INVOKE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAll() throws Exception {
        checkRetry(Test.INVOKE_ALL);
    }

    /**
     * @param test Test type.
     * @throws Exception If failed.
     */
    private void checkRetry(Test test) throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();

        int keysCnt = keysCount();

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    stopGrid(3);

                    U.sleep(300);

                    startGrid(3);
                }

                return null;
            }
        });

        IgniteCache<Integer, Integer> cache = ignite(0).cache(null);

        int iter = 0;

        try {
            if (atomicityMode() == ATOMIC)
                assertEquals(writeOrderMode(), cache.getConfiguration(CacheConfiguration.class).getAtomicWriteOrderMode());

            long stopTime = System.currentTimeMillis() + 60_000;

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
                    IgniteCache<Integer, Integer> cache0 = cache.withAsync();

                    while (System.currentTimeMillis() < stopTime) {
                        Integer val = ++iter;

                        for (int i = 0; i < keysCnt; i++) {
                            cache0.put(i, val);

                            cache0.future().get();
                        }

                        for (int i = 0; i < keysCnt; i++) {
                            cache0.get(i);

                            assertEquals(val, cache0.future().get());
                        }
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

        for (int i = 0; i < gridCount(); i++) {
            IgniteKernal ignite = (IgniteKernal)grid(i);

            Collection<?> futs = ignite.context().cache().context().mvcc().atomicFutures();

            assertTrue("Unexpected atomic futures: " + futs, futs.isEmpty());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailsWithNoRetries() throws Exception {
        checkFailsWithNoRetries(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailsWithNoRetriesAsync() throws Exception {
        checkFailsWithNoRetries(true);
    }

    /**
     * @param async If {@code true} tests asynchronous put.
     * @throws Exception If failed.
     */
    private void checkFailsWithNoRetries(boolean async) throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    stopGrid(3);

                    U.sleep(300);

                    startGrid(3);
                }

                return null;
            }
        });

        try {
            int keysCnt = keysCount();

            boolean eThrown = false;

            IgniteCache<Object, Object> cache = ignite(0).cache(null).withNoRetries();

            if (async)
                cache = cache.withAsync();

            long stopTime = System.currentTimeMillis() + 60_000;

            while (System.currentTimeMillis() < stopTime) {
                for (int i = 0; i < keysCnt; i++) {
                    try {
                        if (async) {
                            cache.put(i, i);

                            cache.future().get();
                        }
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
        PUT_ALL,

        /** */
        PUT_ASYNC,

        /** */
        INVOKE,

        /** */
        INVOKE_ALL
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
}