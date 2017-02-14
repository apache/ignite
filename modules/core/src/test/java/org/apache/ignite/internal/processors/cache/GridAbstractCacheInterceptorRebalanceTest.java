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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public abstract class GridAbstractCacheInterceptorRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "test_cache";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static final int TEST_ITERATIONS = 5;

    /** */
    private static final int NODES = 5;

    /** */
    private static volatile boolean failed;

    /** */
    private static CacheInterceptor<Integer, Integer> interceptor;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        final CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setInterceptor(interceptor);
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Cache memory mode;
     */
    protected abstract CacheMemoryMode memoryMode();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If fail.
     */
    public void testRebalanceUpdate() throws Exception {
        interceptor = new RebalanceUpdateInterceptor();

        testRebalance(new Operation() {
            @Override public void run(final IgniteCache<Integer, Integer> cache, final Integer key, final Integer val) {
                cache.put(key, val);
            }
        });
    }

    /**
     * @throws Exception If fail.
     */
    public void testRebalanceUpdateInvoke() throws Exception {
        interceptor = new RebalanceUpdateInterceptor();

        final UpdateEntryProcessor proc = new UpdateEntryProcessor();

        testRebalance(new Operation() {
            @Override public void run(final IgniteCache<Integer, Integer> cache, final Integer key, final Integer val) {
                cache.invoke(key, proc, val);
            }
        });
    }

    /**
     * @throws Exception If fail.
     */
    public void testRebalanceRemoveInvoke() throws Exception {
        interceptor = new RebalanceUpdateInterceptor();

        final RemoveEntryProcessor proc = new RemoveEntryProcessor();

        testRebalance(new Operation() {
            @Override public void run(final IgniteCache<Integer, Integer> cache, final Integer key, final Integer val) {
                cache.invoke(key, proc, val);
            }
        });
    }

    /**
     * @throws Exception If fail.
     */
    public void testRebalanceRemove() throws Exception {
        interceptor = new RebalanceRemoveInterceptor();

        testRebalance(new Operation() {
            @Override public void run(final IgniteCache<Integer, Integer> cache, final Integer key, final Integer val) {
                cache.remove(key);
            }
        });
    }

    /**
     * @throws Exception If fail.
     */
    public void testPutIfAbsent() throws Exception {
        interceptor = new RebalanceUpdateInterceptor();

        testRebalance(new Operation() {
            @Override public void run(final IgniteCache<Integer, Integer> cache, final Integer key, final Integer val) {
                cache.putIfAbsent(key, val);
            }
        });
    }

    /**
     * @throws Exception If fail.
     */
    public void testGetAndPut() throws Exception {
        interceptor = new RebalanceUpdateInterceptor();

        testRebalance(new Operation() {
            @Override public void run(final IgniteCache<Integer, Integer> cache, final Integer key, final Integer val) {
                final Integer old = cache.getAndPut(key, val);

                assert val == old + 1 : "Unexpected old value: " + old;
            }
        });
    }

    /**
     * @param operation Operation to be tested.
     * @throws Exception If fail.
     */
    private void testRebalance(final Operation operation) throws Exception {
        interceptor = new RebalanceUpdateInterceptor();

        long stopTime = System.currentTimeMillis() + 2 * 60_000;

        for (int iter = 0; iter < TEST_ITERATIONS && System.currentTimeMillis() < stopTime; iter++) {
            log.info("Iteration: " + iter);

            failed = false;

            final IgniteEx ignite = startGrid(1);

            final IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < CNT; i++)
                cache.put(i, i);

            final CountDownLatch latch = new CountDownLatch(1);

            final IgniteInternalFuture<Object> updFut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    latch.await();

                    for (int j = 1; j <= 3; j++) {
                        for (int i = 0; i < CNT; i++) {
                            if (i % 2 == 0) {
                                try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                    operation.run(cache, i, i + j);

                                    tx.commit();
                                }
                            }
                            else
                                operation.run(cache, i, i + j);
                        }
                    }

                    return null;
                }
            });

            final IgniteInternalFuture<Object> rebFut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    latch.await();

                    for (int i = 2; i < NODES; i++)
                        startGrid(i);

                    return null;
                }
            });

            latch.countDown();

            updFut.get();
            rebFut.get();

            stopAllGrids();

            assertFalse(failed);
        }
    }

    /**
     *
     */
    private interface Operation {
        /**
         * @param cache Cache.
         * @param key Key.
         * @param val Value.
         */
        void run(IgniteCache<Integer, Integer> cache, Integer key, Integer val);
    }

    /**
     *
     */
    private static class UpdateEntryProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(final MutableEntry<Integer, Integer> entry,
            final Object... arguments) throws EntryProcessorException {
            entry.setValue((Integer) arguments[0]);

            return null;
        }
    }

    /**
     *
     */
    private static class RemoveEntryProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(final MutableEntry<Integer, Integer> entry,
            final Object... arguments) throws EntryProcessorException {
            entry.remove();

            return null;
        }
    }

    /**
     *
     */
    private static class RebalanceUpdateInterceptor extends CacheInterceptorAdapter<Integer, Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Nullable @Override public Integer onBeforePut(final Cache.Entry entry, final Integer newVal) {
            try {
                boolean first = entry.getKey().equals(newVal);

                if (first)
                    assertNull("Expected null old value: " + entry, entry.getValue());
                else {
                    Integer old = (Integer)entry.getValue();

                    assertNotNull("Null old value: " + entry, old);
                    assertEquals("Unexpected old value: " + entry, newVal.intValue(), old + 1);
                }
            }
            catch (Throwable e) {
                failed = true;

                System.out.println("Unexpected error: " + e);
                e.printStackTrace(System.out);
            }

            return newVal;
        }
    }

    /**
     *
     */
    private static class RebalanceRemoveInterceptor extends CacheInterceptorAdapter<Integer, Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, Integer> onBeforeRemove(
            final Cache.Entry<Integer, Integer> entry) {
            try {
                assertNotNull("Null old value: " + entry, entry.getValue());
                assertEquals("Unexpected old value: " + entry, entry.getKey(), entry.getValue());
            }
            catch (Throwable t) {
                failed = true;

                System.out.println("Unexpected error: " + t);
                t.printStackTrace(System.out);
            }

            return new T2<>(true, null);
        }
    }

}
