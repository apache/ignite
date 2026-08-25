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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtUnlockRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Test checks near cache entry visibility after a transaction rollback to savepoint.
 */
public class TxSavepointNearCacheVisibilityTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setNearConfiguration(new NearCacheConfiguration<>())
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1))
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRolledBackEntryVisibleWithoutRemoteUnlock() throws Exception {
        Ignite ignite0 = startGridsMultiThreaded(2);
        Ignite ignite1 = grid(1);

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        int node0Key = primaryKey(cache0);
        int node1Key = primaryKey(cache1);

        cache0.put(node0Key, -1);
        cache0.put(node1Key, -1);

        TestRecordingCommunicationSpi commSpi = TestRecordingCommunicationSpi.spi(ignite1);

        commSpi.blockMessages((node, msg) ->
            msg instanceof GridDhtUnlockRequest && node.id().equals(ignite0.cluster().localNode().id()));

        CountDownLatch savepointRolledBackLatch = new CountDownLatch(1);
        CountDownLatch finishFirstTxLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 30_000, 2)) {
                cache0.put(node0Key, 1);

                tx.savepoint("sp");

                cache0.put(node1Key, 1);

                tx.rollbackToSavepoint("sp");

                savepointRolledBackLatch.countDown();

                assertTrue(finishFirstTxLatch.await(10, TimeUnit.SECONDS));

                cache0.put(node1Key, 2);

                tx.commit();
            }
        });

        try {
            assertTrue(savepointRolledBackLatch.await(10, TimeUnit.SECONDS));
            assertTrue(commSpi.waitForBlocked(1, 10_000));

            cache1.put(node1Key, 42);

            assertFalse(fut.isDone());
            assertEquals(Integer.valueOf(42), cache0.get(node1Key));
        }
        finally {
            commSpi.stopBlock();

            finishFirstTxLatch.countDown();
        }

        fut.get(10_000);

        assertEquals(Integer.valueOf(2), cache0.get(node1Key));
    }
}
