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
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
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
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SRVS = 4;

    /** */
    private boolean clientMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg1.setName("cache1");
        ccfg1.setAtomicityMode(ATOMIC);
        ccfg1.setBackups(2);
        ccfg1.setWriteSynchronizationMode(PRIMARY_SYNC);

        CacheConfiguration<Object, Object> ccfg2 = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg2.setName("cache2");
        ccfg2.setAtomicityMode(TRANSACTIONAL);
        ccfg2.setBackups(2);
        ccfg2.setWriteSynchronizationMode(PRIMARY_SYNC);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        cfg.setClientMode(clientMode);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(SRVS);

        clientMode = true;

        Ignite client = startGrid(SRVS);

        assertTrue(client.configuration().isClientMode());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGet() throws Exception {
        Ignite ignite = ignite(SRVS);

        checkPutGet(ignite.cache("cache1"), null, null, null);

        checkPutGet(ignite.cache("cache2"), null, null, null);

        checkPutGet(ignite.cache("cache2"), ignite.transactions(), OPTIMISTIC, REPEATABLE_READ);

        checkPutGet(ignite.cache("cache2"), ignite.transactions(), OPTIMISTIC, SERIALIZABLE);

        checkPutGet(ignite.cache("cache2"), ignite.transactions(), PESSIMISTIC, READ_COMMITTED);
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
