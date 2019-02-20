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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePartialUpdateException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Failover tests for cache.
 */
public abstract class GridCacheAbstractFailoverSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final long TEST_TIMEOUT = 3 * 60 * 1000;

    /** */
    private static final String NEW_IGNITE_INSTANCE_NAME = "newGrid";

    /** */
    private static final int ENTRY_CNT = 100;

    /** */
    private static final int TOP_CHANGE_CNT = 10;

    /** */
    private static final int TOP_CHANGE_THREAD_CNT = 3;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setNetworkTimeout(60_000);
        cfg.setMetricsUpdateFrequency(5_000);

        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        discoSpi.setSocketTimeout(30_000);
        discoSpi.setAckTimeout(30_000);
        discoSpi.setNetworkTimeout(60_000);
        discoSpi.setReconnectCount(2);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setRebalanceMode(SYNC);

        if (cfg.getCacheMode() == CacheMode.PARTITIONED)
            cfg.setBackups(TOP_CHANGE_THREAD_CNT);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCount());

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTopologyChange() throws Exception {
        testTopologyChange(null, null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConstantTopologyChange() throws Exception {
        testConstantTopologyChange(null, null);
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws Exception If failed.
     */
    protected void testTopologyChange(@Nullable TransactionConcurrency concurrency,
        @Nullable TransactionIsolation isolation) throws Exception {
        boolean tx = concurrency != null && isolation != null;

        if (tx)
            put(ignite(0), jcache(), ENTRY_CNT, concurrency, isolation);
        else
            put(jcache(), ENTRY_CNT);

        Ignite g = startGrid(NEW_IGNITE_INSTANCE_NAME);

        check(cache(g), ENTRY_CNT);

        int half = ENTRY_CNT / 2;

        if (tx) {
            remove(g, cache(g), half, concurrency, isolation);
            put(g, cache(g), half, concurrency, isolation);
        }
        else {
            remove(cache(g), half);
            put(cache(g), half);
        }

        stopGrid(NEW_IGNITE_INSTANCE_NAME);

        check(jcache(), ENTRY_CNT);
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws Exception If failed.
     */
    protected void testConstantTopologyChange(@Nullable final TransactionConcurrency concurrency,
        @Nullable final TransactionIsolation isolation) throws Exception {
        final boolean tx = concurrency != null && isolation != null;

        if (tx)
            put(ignite(0), jcache(), ENTRY_CNT, concurrency, isolation);
        else
            put(jcache(), ENTRY_CNT);

        check(jcache(), ENTRY_CNT);

        final int half = ENTRY_CNT / 2;

        final AtomicReference<Exception> err = new AtomicReference<>();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
            @Override public void apply() {
                info("Run topology change.");

                try {
                    String name = "new-node-" + Thread.currentThread().getName();

                    for (int i = 0; i < TOP_CHANGE_CNT && err.get() == null; i++) {
                        info("Topology change " + i);

                        try {
                            final Ignite g = startGrid(name);

                            IgniteCache<String, Object> cache = g.cache(DEFAULT_CACHE_NAME);

                            for (int k = half; k < ENTRY_CNT; k++) {
                                String key = "key" + k;

                                assertNotNull("Failed to get key: 'key" + k + "'", cache.getAsync(key).get(30_000));
                            }
                        }
                        finally {
                            G.stop(name, false);
                        }
                    }
                }
                catch (Exception e) {
                    err.set(e);

                    log.error("Unexpected exception in topology-change-thread: " + e, e);
                }
            }
        }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

        boolean isInterrupted = false;

        try {
            while (!fut.isDone() && !isInterrupted) {
                if (tx) {
                    remove(grid(0), jcache(), half, concurrency, isolation);
                    put(grid(0), jcache(), half, concurrency, isolation);
                }
                else {
                    remove(jcache(), half);
                    put(jcache(), half);
                }

                isInterrupted = Thread.currentThread().isInterrupted();
            }

            if (isInterrupted) {
                Thread.currentThread().interrupt();

                fut.cancel();
            }
        }
        catch (Exception e) {
            err.set(e);

            log.error("Unexpected exception: " + e, e);

            throw e;
        }

        if (!isInterrupted)
            fut.get();

        Exception err0 = err.get();

        if (err0 != null)
            throw err0;
    }

    /**
     * @param cache Cache.
     * @param cnt Entry count.
     * @throws IgniteCheckedException If failed.
     */
    private void put(IgniteCache<String, Integer> cache, int cnt) throws Exception {
        try {
            for (int i = 0; i < cnt; i++)
                cache.put("key" + i, i);
        }
        catch (CacheException e) {
            if (!X.hasCause(e, ClusterTopologyCheckedException.class) && !(e instanceof CachePartialUpdateException))
                throw e;
        }
    }

    /**
     * @param ignite Ignite.
     * @param cache Cache.
     * @param cnt Entry count.
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws IgniteCheckedException If failed.
     */
    private void put(Ignite ignite,
        IgniteCache<String, Integer> cache,
        final int cnt,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation)
        throws Exception {
        try {
            info("Putting values to cache [0," + cnt + ')');

            CU.inTx(ignite, cache, concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++)
                        cache.put("key" + i, i);
                }
            });
        }
        catch (Exception e) {
            // It is ok to fail with topology exception.
            if (!X.hasCause(e, ClusterTopologyCheckedException.class))
                throw e;
            else
                info("Failed to put values to cache due to topology exception [0," + cnt + ')');
        }
    }

    /**
     * @param cache Cache.
     * @param cnt Entry count.
     * @throws IgniteCheckedException If failed.
     */
    private void remove(IgniteCache<String, Integer> cache, int cnt) throws Exception {
        try {
            for (int i = 0; i < cnt; i++)
                cache.remove("key" + i);
        }
        catch (CacheException e) {
            if (!X.hasCause(e, ClusterTopologyCheckedException.class) && !(e instanceof CachePartialUpdateException))
                throw e;
        }
    }

    /**
     * @param ignite Ignite.
     * @param cache Cache.
     * @param cnt Entry count.
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws IgniteCheckedException If failed.
     */
    private void remove(Ignite ignite, IgniteCache<String, Integer> cache, final int cnt,
        TransactionConcurrency concurrency, final TransactionIsolation isolation) throws Exception {
        try {
            info("Removing values form cache [0," + cnt + ')');

            CU.inTx(ignite, cache, concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++) {
                        String key = "key" + i;

                        // Use removeAll for serializable tx to avoid version check.
                        if (isolation == TransactionIsolation.SERIALIZABLE)
                            cache.removeAll(Collections.singleton(key));
                        else
                            cache.remove(key);
                    }
                }
            });
        }
        catch (Exception e) {
            // It is ok to fail with topology exception.
            if (!X.hasCause(e, ClusterTopologyCheckedException.class))
                throw e;
            else
                info("Failed to remove values from cache due to topology exception [0," + cnt + ')');
        }
    }

    /**
     * @param cache Cache.
     * @param expSize Minimum expected cache size.
     * @throws Exception If failed.
     */
    private void check(final IgniteCache<String, Integer> cache, final int expSize) throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return cache.size() >= expSize;
            }
        }, 5000);

        int size = cache.size();

        assertTrue("Key set size is lesser then the expected size [size=" + size + ", expSize=" + expSize + ']',
            size >= expSize);

        for (int i = 0; i < expSize; i++)
            assertNotNull("Failed to get value for key: 'key" + i + "'", cache.get("key" + i));
    }

    /**
     * @param g Grid.
     * @return Cache.
     */
    private IgniteCache<String, Integer> cache(Ignite g) {
        return g.cache(DEFAULT_CACHE_NAME);
    }
}
