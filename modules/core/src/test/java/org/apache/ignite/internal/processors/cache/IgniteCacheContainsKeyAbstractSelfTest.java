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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Tests various scenarios for {@code containsKey()} method.
 */
public abstract class IgniteCacheContainsKeyAbstractSelfTest extends GridCacheAbstractSelfTest {
    /**
     * @return Number of grids to start.
     */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        jcache(0).removeAll();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        TransactionConfiguration tcfg = new TransactionConfiguration();

        tcfg.setTxSerializableEnabled(true);

        cfg.setTransactionConfiguration(tcfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributedContains() throws Exception {
        String key = "1";

        jcache(0).put(key, 1);

        for (int i = 0; i < gridCount(); i++) {
            assertTrue("Invalid result on grid: " + i, jcache(i).containsKey(key));

            assertFalse("Invalid result on grid: " + i, jcache(i).containsKey("2"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testContainsInTx() throws Exception {
        if (atomicityMode() == TRANSACTIONAL) {
            String key = "1";

            for (int i = 0; i < gridCount(); i++)
                assertFalse("Invalid result on grid: " + i, jcache(i).containsKey(key));

            IgniteCache<String, Integer> cache = jcache(0);

            for (TransactionConcurrency conc : TransactionConcurrency.values()) {
                for (TransactionIsolation iso : TransactionIsolation.values()) {
                    try (Transaction tx = grid(0).transactions().txStart(conc, iso)) {
                        assertFalse("Invalid result on grid inside tx", cache.containsKey(key));

                        assertFalse("Key was enlisted to transaction: " + tx, txContainsKey(tx, key));

                        cache.put(key, 1);

                        assertTrue("Invalid result on grid inside tx", cache.containsKey(key));

                        // Do not commit.
                    }

                    for (int i = 0; i < gridCount(); i++)
                        assertFalse("Invalid result on grid: " + i, jcache(i).containsKey(key));
                }
            }
        }
    }

    /**
     * Checks if transaction has given key enlisted.
     *
     * @param tx Transaction to check.
     * @param key Key to check.
     * @return {@code True} if key was enlisted.
     */
    private boolean txContainsKey(Transaction tx, String key) {
        TransactionProxyImpl<String, Integer> proxy = (TransactionProxyImpl<String, Integer>)tx;

        IgniteInternalTx txEx = proxy.tx();

        IgniteTxEntry entry = txEx.entry(context(0).txKey(context(0).toCacheKeyObject(key)));

        return entry != null;
    }
}