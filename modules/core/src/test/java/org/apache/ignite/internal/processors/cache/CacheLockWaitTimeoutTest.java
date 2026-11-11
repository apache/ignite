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

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** Tests primary-side lock wait deadlines independently of response delivery. */
@RunWith(Parameterized.class)
public class CacheLockWaitTimeoutTest extends GridCommonAbstractTest {
    /** Lock wait budget, shorter than the transaction timeout. */
    private static final long WAIT_TIMEOUT = 500;

    /** Whether the initiating client uses a near cache. */
    @Parameterized.Parameter
    public boolean near;

    /** @return Cache configurations. */
    @Parameterized.Parameters(name = "near={0}")
    public static Collection<Object[]> parameters() {
        return List.of(new Object[] {false}, new Object[] {true});
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name).setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);
        startClientGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid(0).createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC));

        if (near)
            grid(2).createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());
        else
            grid(2).cache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        TestRecordingCommunicationSpi.stopBlockAll();
        grid(0).destroyCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** A successful primary response remains valid after the coordinator's lock wait budget expires. */
    @Test
    public void testDelayedSuccessfulResponse() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(2).cache(DEFAULT_CACHE_NAME);
        int key = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));

        cache.put(key, 0);

        CacheEntry<Integer, Integer> entry = cache.getEntry(key);

        spi(grid(0)).blockMessages(GridNearLockResponse.class, grid(2).name());

        IgniteInternalFuture<Boolean> fut = lockAsync(cache, List.of(entry));

        try {
            assertTrue(spi(grid(0)).waitForBlocked(1, 10_000));
            assertFalse("The coordinator must await the primary response",
                GridTestUtils.waitForCondition(fut::isDone, 2 * WAIT_TIMEOUT));
        }
        finally {
            spi(grid(0)).stopBlock();
        }

        assertTrue(fut.get(10_000));
    }

    /** Later primary batches must not receive a fresh lock wait budget. */
    @Test
    public void testWaitBudgetIsSharedByPrimaryBatches() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(2).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> holderCache = grid(1).cache(DEFAULT_CACHE_NAME);
        int first = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        int second = primaryKey(holderCache);

        cache.put(first, 0);
        cache.put(second, 0);

        List<CacheEntry<Integer, Integer>> entries = List.of(cache.getEntry(first), cache.getEntry(second));

        try (Transaction holder = grid(1).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
            holderCache.put(second, 1);

            spi(grid(2)).record(GridNearLockRequest.class);
            spi(grid(0)).blockMessages(GridNearLockResponse.class, grid(2).name());

            IgniteInternalFuture<Boolean> fut = lockAsync(cache, entries);

            try {
                assertTrue(spi(grid(0)).waitForBlocked(1, 10_000));
                assertFalse("The coordinator must await the primary response",
                    GridTestUtils.waitForCondition(fut::isDone, 2 * WAIT_TIMEOUT));
            }
            finally {
                spi(grid(0)).stopBlock();
            }

            assertFalse(fut.get(10_000));

            List<Object> requests = spi(grid(2)).recordedMessages(true);

            assertEquals(2, requests.size());
            assertTrue(((GridNearLockRequest)requests.get(0)).waitTimeout() > 0);
            assertTrue("An exhausted budget requires an immediate attempt on the next primary",
                ((GridNearLockRequest)requests.get(1)).waitTimeout() < 0);
        }
    }

    /**
     * @param cache Client cache.
     * @param entries Entries to lock, in primary mapping order.
     * @return Lock result after transaction cleanup.
     */
    @SuppressWarnings("unchecked")
    private IgniteInternalFuture<Boolean> lockAsync(
        IgniteCache<Integer, Integer> cache,
        List<CacheEntry<Integer, Integer>> entries
    ) {
        return GridTestUtils.runAsync(() -> {
            try (Transaction tx = grid(2).transactions().txStart(PESSIMISTIC, READ_COMMITTED, 30_000, 0)) {
                return cache.unwrap(IgniteCacheProxy.class).internalProxy().lockTxEntries(entries, WAIT_TIMEOUT);
            }
        });
    }
}
