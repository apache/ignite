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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgniteCachePrimarySyncTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 4;

    /** */
    private static final String ATOMIC_CACHE = "atomicCache";

    /** */
    private static final String TX_CACHE = "txCache";

    /** */
    private static final String MVCC_CACHE = "mvccCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>(ATOMIC_CACHE)
            .setReadFromBackup(false)
            .setAtomicityMode(ATOMIC)
            .setBackups(2)
            .setWriteSynchronizationMode(PRIMARY_SYNC);

        CacheConfiguration<Object, Object> ccfg2 = new CacheConfiguration<>(TX_CACHE)
            .setReadFromBackup(false)
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(2)
            .setWriteSynchronizationMode(PRIMARY_SYNC);

        CacheConfiguration<Object, Object> ccfg3 = new CacheConfiguration<>(MVCC_CACHE)
            .setReadFromBackup(false)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT)
            .setBackups(2)
            .setWriteSynchronizationMode(PRIMARY_SYNC);

        cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(SRVS);

        Ignite client = startClientGrid(SRVS);

        assertTrue(client.configuration().isClientMode());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutGet() throws Exception {
        Ignite ignite = ignite(SRVS);

        checkPutGet(ignite.cache(ATOMIC_CACHE), null, null, null);

        checkPutGet(ignite.cache(TX_CACHE), null, null, null);

        checkPutGet(ignite.cache(MVCC_CACHE), null, null, null);

        checkPutGet(ignite.cache(TX_CACHE), ignite.transactions(), OPTIMISTIC, REPEATABLE_READ);

        checkPutGet(ignite.cache(TX_CACHE), ignite.transactions(), OPTIMISTIC, SERIALIZABLE);

        checkPutGet(ignite.cache(TX_CACHE), ignite.transactions(), PESSIMISTIC, READ_COMMITTED);

        checkPutGet(ignite.cache(MVCC_CACHE), ignite.transactions(), PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param cache Cache.
     * @param txs Transactions instance if explicit transaction should be used.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     */
    private void checkPutGet(IgniteCache<Object, Object> cache,
        @Nullable IgniteTransactions txs,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation) {
        log.info("Check cache: " + cache.getName());

        final int KEYS = 50;

        for (int iter = 0; iter < 100; iter++) {
            if (iter % 10 == 0)
                log.info("Iteration: " + iter);

            for (int i = 0; i < KEYS; i++)
                cache.remove(i);

            Map<Integer, Integer> putBatch = new HashMap<>();

            for (int i = 0; i < KEYS; i++)
                putBatch.put(i, iter);

            if (txs != null) {
                try (Transaction tx = txs.txStart(concurrency, isolation)) {
                    cache.putAll(putBatch);

                    tx.commit();
                }
            }
            else
                cache.putAll(putBatch);

            Map<Object, Object> vals = cache.getAll(putBatch.keySet());

            for (int i = 0; i < KEYS; i++)
                assertEquals(iter, vals.get(i));
        }
    }
}
