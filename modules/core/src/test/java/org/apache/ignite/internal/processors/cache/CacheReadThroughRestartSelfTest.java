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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test for read through store.
 */
public class CacheReadThroughRestartSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TransactionConfiguration txCfg = new TransactionConfiguration();

        txCfg.setTxSerializableEnabled(true);

        cfg.setTransactionConfiguration(txCfg);

        CacheConfiguration cc = cacheConfiguration(gridName);

        cc.setLoadPreviousValue(false);

        cfg.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadThroughInTx() throws Exception {
        IgniteCache<String, Integer> cache = grid(1).cache(null);

        for (int k = 0; k < 1000; k++)
            cache.put("key" + k, k);

        stopAllGrids();

        startGrids(2);

        Ignite ignite = grid(1);

        cache = ignite.cache(null);

        for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation txIsolation : TransactionIsolation.values()) {
                try (Transaction tx = ignite.transactions().txStart(txConcurrency, txIsolation, 100000, 1000)) {
                    for (int k = 0; k < 1000; k++) {
                        String key = "key" + k;

                        assertNotNull("Null value for key: " + key, cache.get(key));
                    }

                    tx.commit();
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadThrough() throws Exception {
        IgniteCache<String, Integer> cache = grid(1).cache(null);

        for (int k = 0; k < 1000; k++)
            cache.put("key" + k, k);

        stopAllGrids();

        startGrids(2);

        Ignite ignite = grid(1);

        cache = ignite.cache(null);

        for (int k = 0; k < 1000; k++) {
            String key = "key" + k;

            assertNotNull("Null value for key: " + key, cache.get(key));
        }
    }
}