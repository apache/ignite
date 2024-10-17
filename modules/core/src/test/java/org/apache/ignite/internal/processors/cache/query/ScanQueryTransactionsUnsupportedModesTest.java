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

package org.apache.ignite.internal.processors.cache.query;

import java.util.HashSet;
import java.util.Set;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/** */
public class ScanQueryTransactionsUnsupportedModesTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxAwareQueriesEnabled(true);

        return cfg;
    }

    /** */
    @Test
    public void testUnsupportedTransactionModes() throws Exception {
        try (IgniteEx srv = startGrid(0)) {
            for (boolean client : new boolean[] {/*false,*/ true}) {
                IgniteEx executor = client ? startClientGrid() : srv;

                IgniteCache<Object, Object> c = executor.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    Set<TransactionIsolation> supported = new HashSet<>(TransactionConfiguration.TX_AWARE_QUERIES_SUPPORTED_MODES);

                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        try (Transaction ignored = executor.transactions().txStart(concurrency, isolation)) {
                            c.query(new ScanQuery<>()).getAll();

                            assertTrue(supported.remove(isolation));
                        }
                        catch (CacheException e) {
                            assertFalse(supported.contains(isolation));
                        }
                    }

                    assertTrue(supported.isEmpty());
                }
            }
        }
    }
}
