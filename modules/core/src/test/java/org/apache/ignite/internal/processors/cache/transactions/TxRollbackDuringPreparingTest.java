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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 * Transaction rolls back during the prepare phase, before a prepare request was sent to dht nodes.
 * It should lead to the full transaction rollback.
 */
public class TxRollbackDuringPreparingTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(2));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ignite0 = startGrids(3);

        awaitPartitionMapExchange();

        int key = primaryKey(ignite(1).cache(DEFAULT_CACHE_NAME));

        IgniteEx client = startClientGrid("client");

        waitForTopology(4);

        TestRecordingCommunicationSpi.spi(ignite(1)).blockMessages(GridDhtTxPrepareRequest.class, getTestIgniteInstanceName(0));
        TestRecordingCommunicationSpi.spi(ignite(1)).blockMessages(GridDhtTxPrepareRequest.class, getTestIgniteInstanceName(2));

        IgniteCache cache = client.cache(DEFAULT_CACHE_NAME);

        long txTimeout = 1_000;

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            Transaction tx = client.transactions().txStart(
                TransactionConcurrency.OPTIMISTIC,
                TransactionIsolation.SERIALIZABLE,
                txTimeout,
                1
            );

            cache.put(key, key);

            tx.commit();
        });

        TestRecordingCommunicationSpi.spi(ignite(1)).waitForBlocked();

        ignite(0).context().discovery().failNode(client.localNode().id(), "Stop the client!");

        doSleep(2 * txTimeout);

        TestRecordingCommunicationSpi.spi(ignite(1)).stopBlock();

        GridTestUtils.assertThrows(log, () -> fut.get(10_000), IgniteCheckedException.class, "");

        ignite0.createCache(DEFAULT_CACHE_NAME + 1);

        awaitPartitionMapExchange();

        assertNull(cache.get(key));
    }
}
