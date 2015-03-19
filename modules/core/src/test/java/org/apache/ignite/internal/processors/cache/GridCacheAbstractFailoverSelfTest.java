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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.util.*;

import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Failover tests for cache.
 */
public abstract class GridCacheAbstractFailoverSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final long TEST_TIMEOUT = 3 * 60 * 1000;

    /** */
    private static final String NEW_GRID_NAME = "newGrid";

    /** */
    private static final int ENTRY_CNT = 100;

    /** */
    private static final int TOP_CHANGE_CNT = 5;

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setRebalanceMode(SYNC);

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
    public void testTopologyChange() throws Exception {
        testTopologyChange(null, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTxTopologyChange() throws Exception {
        testTopologyChange(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableTxTopologyChange() throws Exception {
        testTopologyChange(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConstantTopologyChange() throws Exception {
        testConstantTopologyChange(null, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommittedTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableReadTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSerializableTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommittedTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableReadTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializableTxConstantTopologyChange() throws Exception {
        testConstantTopologyChange(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws Exception If failed.
     */
    private void testTopologyChange(@Nullable TransactionConcurrency concurrency,
        @Nullable TransactionIsolation isolation) throws Exception {
        boolean tx = concurrency != null && isolation != null;

        if (tx)
            put(ignite(0), jcache(), ENTRY_CNT, concurrency, isolation);
        else
            put(jcache(), ENTRY_CNT);

        Ignite g = startGrid(NEW_GRID_NAME);

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

        stopGrid(NEW_GRID_NAME);

        check(jcache(), ENTRY_CNT);
    }

    /**
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws Exception If failed.
     */
    private void testConstantTopologyChange(@Nullable final TransactionConcurrency concurrency,
        @Nullable final TransactionIsolation isolation) throws Exception {
        final boolean tx = concurrency != null && isolation != null;

        if (tx)
            put(ignite(0), jcache(), ENTRY_CNT, concurrency, isolation);
        else
            put(jcache(), ENTRY_CNT);

        check(jcache(), ENTRY_CNT);

        final int half = ENTRY_CNT / 2;

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new CA() {
            @Override public void apply() {
                info("Run topology change.");

                try {
                    for (int i = 0; i < TOP_CHANGE_CNT; i++) {
                        info("Topology change " + i);

                        String name = UUID.randomUUID().toString();

                        try {
                            final Ignite g = startGrid(name);

                            for (int k = half; k < ENTRY_CNT; k++)
                                assertNotNull("Failed to get key: 'key" + k + "'", cache(g).get("key" + k));
                        }
                        finally {
                            G.stop(name, false);
                        }
                    }
                }
                catch (Exception e) {
                    throw F.wrap(e);
                }
            }
        }, TOP_CHANGE_THREAD_CNT, "topology-change-thread");

        while (!fut.isDone()) {
            if (tx) {
                remove(grid(0), jcache(), half, concurrency, isolation);
                put(grid(0), jcache(), half, concurrency, isolation);
            }
            else {
                remove(jcache(), half);
                put(jcache(), half);
            }
        }

        fut.get();
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
            // It is ok to fail with topology exception.
            if (!X.hasCause(e, ClusterTopologyCheckedException.class))
                throw e;
        }
    }

    /**
     * @param cache Cache.
     * @param cnt Entry count.
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws IgniteCheckedException If failed.
     */
    private void put(Ignite ignite, IgniteCache<String, Integer> cache, final int cnt,
        TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        try {
            info("Putting values to cache [0," + cnt + ')');

            CU.inTx(ignite, cache, concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++)
                        cache.put("key" + i, i);
                }
            });
        }
        catch (IgniteCheckedException e) {
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
            // It is ok to fail with topology exception.
            if (!X.hasCause(e, ClusterTopologyCheckedException.class))
                throw e;
        }
    }

    /**
     * @param cache Cache.
     * @param cnt Entry count.
     * @param concurrency Concurrency control.
     * @param isolation Isolation level.
     * @throws IgniteCheckedException If failed.
     */
    private void remove(Ignite g, IgniteCache<String, Integer> cache, final int cnt,
        TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        try {
            info("Removing values form cache [0," + cnt + ')');

            CU.inTx(g, cache, concurrency, isolation, new CIX1<IgniteCache<String, Integer>>() {
                @Override public void applyx(IgniteCache<String, Integer> cache) {
                    for (int i = 0; i < cnt; i++)
                        cache.remove("key" + i);
                }
            });
        }
        catch (IgniteCheckedException e) {
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
     */
    private void check(IgniteCache<String,Integer> cache, int expSize) {
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
    private IgniteCache<String,Integer> cache(Ignite g) {
        return g.jcache(null);
    }
}
