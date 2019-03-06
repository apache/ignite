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

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 * Test that no deserialization happens with binary objects and keepBinary set flag.
 */
public class CacheKeepBinaryTransactionTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TransactionConfiguration txCfg = new TransactionConfiguration();

        if (!MvccFeatureChecker.forcedMvcc()) {
            txCfg.setDefaultTxConcurrency(TransactionConcurrency.OPTIMISTIC);
            txCfg.setDefaultTxIsolation(TransactionIsolation.REPEATABLE_READ);
        }

        cfg.setTransactionConfiguration(txCfg);

        cfg.setMarshaller(new BinaryMarshaller());

        CacheConfiguration ccfg = new CacheConfiguration("tx-cache");
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryGet() throws Exception {
        IgniteEx ignite = grid(0);
        IgniteCache<Object, Object> cache = ignite.cache("tx-cache").withKeepBinary();

        try (Transaction tx = ignite.transactions().txStart()) {
            BinaryObject key = ignite.binary().builder("test1")
                .setField("id", 1).build();

            assertNull(cache.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryContains() throws Exception {
        IgniteEx ignite = grid(0);
        IgniteCache<Object, Object> cache = ignite.cache("tx-cache").withKeepBinary();

        try (Transaction tx = ignite.transactions().txStart()) {
            BinaryObject key = ignite.binary().builder("test2")
                .setField("id", 1).build();

            assertFalse(cache.containsKey(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryPutGetContains() throws Exception {
        IgniteEx ignite = grid(0);
        IgniteCache<Object, Object> cache = ignite.cache("tx-cache").withKeepBinary();

        try (Transaction tx = ignite.transactions().txStart()) {
            IgniteBinary binary = ignite.binary();

            BinaryObject key = binary.builder("test-key").setField("id", 1).build();
            BinaryObject val = binary.builder("test-val").setField("id", 22).build();

            cache.put(key, val);

            assertTrue(cache.containsKey(key));
            assertEquals(val, cache.get(key));
        }
    }
}
