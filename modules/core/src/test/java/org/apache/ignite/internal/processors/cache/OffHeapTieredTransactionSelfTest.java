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

import java.util.LinkedHashMap;
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

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Off-heap tiered test.
 */
public class OffHeapTieredTransactionSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setMemoryMode(OFFHEAP_TIERED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setOffHeapMaxMemory(0);
        ccfg.setSwapEnabled(true);
        ccfg.setCacheMode(REPLICATED);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(ccfg);

        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(2);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception In case of error.
     */
    public void testPutAll() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).cache(null);

        final int KEYS = 5;

        Map<String, Integer> data = new LinkedHashMap<>();

        for (int i = 0; i < KEYS; i++)
            data.put("key_" + i, i);

        checkPutAll(cache, data, OPTIMISTIC, READ_COMMITTED);

        checkPutAll(cache, data, OPTIMISTIC, REPEATABLE_READ);

        checkPutAll(cache, data, OPTIMISTIC, SERIALIZABLE);

        checkPutAll(cache, data, PESSIMISTIC, READ_COMMITTED);

        checkPutAll(cache, data, PESSIMISTIC, REPEATABLE_READ);

        checkPutAll(cache, data, PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @throws Exception In case of error.
     */
    private void checkPutAll(IgniteCache<String, Integer> cache, Map<String, Integer> data,
        TransactionConcurrency txConcurrency, TransactionIsolation txIsolation) throws Exception {
        IgniteTransactions txs = cache.unwrap(Ignite.class).transactions();

        try (Transaction tx = txs.txStart(txConcurrency, txIsolation)) {
            cache.putAll(data);

            tx.commit();
        }

        for (Map.Entry<String, Integer> entry : data.entrySet())
            assertEquals(entry.getValue(), cache.get(entry.getKey()));
    }
}