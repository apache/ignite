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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test for read through store.
 */
@RunWith(JUnit4.class)
public class CacheReadThroughRestartSelfTest extends GridCacheAbstractSelfTest {
    /** */
    @Before
    public void beforeCacheReadThroughRestartSelfTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TransactionConfiguration txCfg = new TransactionConfiguration();

        txCfg.setTxSerializableEnabled(true);

        cfg.setTransactionConfiguration(txCfg);

        CacheConfiguration cc = cacheConfiguration(igniteInstanceName);

        cc.setLoadPreviousValue(false);

        cfg.setCacheConfiguration(cc);

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
    @Test
    public void testReadThroughInTx() throws Exception {
        testReadThroughInTx(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadEntryThroughInTx() throws Exception {
        testReadThroughInTx(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testReadThroughInTx(boolean needVer) throws Exception {
        IgniteCache<String, Integer> cache = grid(1).cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1000; k++)
            cache.put("key" + k, k);

        stopAllGrids();

        startGrids(2);

        awaitPartitionMapExchange();

        Ignite ignite = grid(1);

        cache = ignite.cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();

        for (TransactionConcurrency txConcurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation txIsolation : TransactionIsolation.values()) {
                if (MvccFeatureChecker.forcedMvcc() && !MvccFeatureChecker.isSupported(txConcurrency, txIsolation))
                    continue;

                try (Transaction tx = ignite.transactions().txStart(txConcurrency, txIsolation, 100000, 1000)) {
                    for (int k = 0; k < 1000; k++) {
                        String key = "key" + k;

                        if (needVer) {
                            assertNotNull("Null value for key: " + key, cache.getEntry(key));
                            assertNotNull("Null value for key: " + key, cache.getEntry(key));
                        }
                        else {
                            assertNotNull("Null value for key: " + key, cache.get(key));
                            assertNotNull("Null value for key: " + key, cache.get(key));
                        }
                    }

                    tx.commit();
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadThrough() throws Exception {
        testReadThrough(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReadEntryThrough() throws Exception {
        testReadThrough(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testReadThrough(boolean needVer) throws Exception {
        IgniteCache<String, Integer> cache = grid(1).cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1000; k++)
            cache.put("key" + k, k);

        stopAllGrids();

        startGrids(2);

        Ignite ignite = grid(1);

        cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1000; k++) {
            String key = "key" + k;
            if (needVer)
                assertNotNull("Null value for key: " + key, cache.getEntry(key));
            else
                assertNotNull("Null value for key: " + key, cache.get(key));
        }
    }
}
