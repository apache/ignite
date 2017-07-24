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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgnitePessimisticTxSuspendResumeTest extends GridCommonAbstractTest {
    /**
     * Creates new cache configuration.
     *
     * @return CacheConfiguration New cache configuration.
     */
    protected CacheConfiguration<Integer, String> getCacheConfiguration() {
        CacheConfiguration<Integer, String> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(false);
        cfg.setCacheConfiguration(getCacheConfiguration());

        return cfg;
    }

    /**
     * Test for suspension on pessimistic transaction.
     *
     * @throws Exception If failed.
     */
    public void testSuspendPessimisticTx() throws Exception {
        try (Ignite g = startGrid()) {
            IgniteCache<Integer, String> cache = jcache();

            IgniteTransactions txs = g.transactions();

            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                final Transaction tx = txs.txStart(TransactionConcurrency.PESSIMISTIC, isolation);

                cache.put(1, "1");

                GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        tx.suspend();

                        return null;
                    }
                }, UnsupportedOperationException.class);

                tx.close();

                assertNull(cache.get(1));
            }
        }
    }
}
