/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.Test;

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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        TransactionConfiguration tcfg = new TransactionConfiguration();

        tcfg.setTxSerializableEnabled(true);

        cfg.setTransactionConfiguration(tcfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
