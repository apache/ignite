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

import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheAsyncOperationsTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static volatile CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (gridName.equals(getTestGridName(1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncOperationsTx() throws Exception {
        asyncOperations(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncOperationsAtomic() throws Exception {
        asyncOperations(ATOMIC);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        CountDownLatch latch0 = latch;

        if (latch0 != null)
            latch0.countDown();

        latch = null;
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    public void asyncOperations(CacheAtomicityMode atomicityMode) throws Exception {
        try (IgniteCache<Integer, Integer> cache = ignite(1).getOrCreateCache(cacheConfiguration(atomicityMode))) {
            async1(cache);

            async2(cache);
        }
    }

    /**
     * @param cache Cache.
     */
    private void async1(IgniteCache<Integer, Integer> cache) {
        cache.put(1, 1);

        latch = new CountDownLatch(1);

        IgniteCache<Integer, Integer> asyncCache = cache.withAsync();

        asyncCache.put(0, 0);

        IgniteFuture<?> fut1 = asyncCache.future();

        asyncCache.getAndPut(1, 2);

        IgniteFuture<?> fut2 = asyncCache.future();

        asyncCache.getAndPut(1, 3);

        IgniteFuture<?> fut3 = asyncCache.future();

        assertFalse(fut1.isDone());
        assertFalse(fut2.isDone());
        assertFalse(fut3.isDone());

        latch.countDown();

        try {
            fut1.get();

            fail();
        }
        catch (CacheException e) {
            log.info("Expected error: " + e);
        }

        assertEquals(1, fut2.get());
        assertEquals(2, fut3.get());

        assertNull(cache.get(0));
        assertEquals((Integer)3, cache.get(1));
    }
    /**
     *
     * @param cache Cache.
     */
    private void async2(IgniteCache<Integer, Integer> cache) {
        cache.put(1, 1);

        latch = new CountDownLatch(1);

        IgniteCache<Integer, Integer> asyncCache = cache.withAsync();

        asyncCache.put(0, 0);

        IgniteFuture<?> fut1 = asyncCache.future();

        asyncCache.put(0, 0);

        IgniteFuture<?> fut2 = asyncCache.future();

        asyncCache.getAndPut(1, 2);

        IgniteFuture<?> fut3 = asyncCache.future();

        asyncCache.put(0, 0);

        IgniteFuture<?> fut4 = asyncCache.future();

        assertFalse(fut1.isDone());
        assertFalse(fut2.isDone());
        assertFalse(fut3.isDone());
        assertFalse(fut4.isDone());

        latch.countDown();

        try {
            fut1.get();

            fail();
        }
        catch (CacheException e) {
            log.info("Expected error: " + e);
        }

        try {
            fut2.get();

            fail();
        }
        catch (CacheException e) {
            log.info("Expected error: " + e);
        }

        assertEquals(1, fut3.get());

        try {
            fut4.get();

            fail();
        }
        catch (CacheException e) {
            log.info("Expected error: " + e);
        }

        assertNull(cache.get(0));
        assertEquals((Integer)2, cache.get(1));
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setWriteThrough(true);
        ccfg.setCacheStoreFactory(new StoreFactory());

        return ccfg;
    }

    /**
     *
     */
    private static class StoreFactory implements Factory<TestStore> {
        /** {@inheritDoc} */
        @Override public TestStore create() {
            return new TestStore();
        }
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
            CountDownLatch latch0 = latch;

            if (latch0 != null)
                U.awaitQuiet(latch0);

            Integer key = entry.getKey();

            if (key.equals(0)) {
                System.out.println(Thread.currentThread().getName() + ": fail operation for key: " + key);

                throw new CacheWriterException("Test error.");
            }
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}