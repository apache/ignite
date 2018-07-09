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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/** */
public class TxSavepointsAtomicCacheTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** */
    private CacheConfiguration<Integer, Integer> getConfig(CacheMode cacheMode) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>();

        cfg.setAtomicityMode(ATOMIC);

        cfg.setCacheMode(cacheMode);

        cfg.setName(cacheMode.name());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        for (CacheMode cacheMode : CacheMode.values()) {
            IgniteCache<Integer, Integer> cache = grid().getOrCreateCache(getConfig(cacheMode)).withAllowAtomicOpsInTx();

            for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    try (Transaction tx = grid().transactions().txStart(concurrency, isolation)) {
                        tx.savepoint("sp");

                        cache.put(1, 1);

                        tx.rollbackToSavepoint("sp");

                        tx.commit();
                    }

                    assertEquals("Broken rollback to savepoint in " +
                        concurrency + " " + isolation + " transaction.", (Integer) 1, cache.get(1));

                    cache.remove(1);
                }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        for (CacheMode cacheMode : CacheMode.values()) {
            IgniteCache<Integer, Integer> cache = grid().getOrCreateCache(getConfig(cacheMode)).withAllowAtomicOpsInTx();

            for (TransactionConcurrency concurrency : TransactionConcurrency.values())
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    cache.put(1, 1);

                    try (Transaction tx = grid().transactions().txStart(concurrency, isolation)) {
                        tx.savepoint("sp");

                        cache.remove(1);

                        tx.rollbackToSavepoint("sp");

                        tx.commit();
                    }

                    assertEquals("Broken rollback to savepoint in " +
                        concurrency + " " + isolation + " transaction.", null, cache.get(1));
                }
        }
    }
}
