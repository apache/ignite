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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.transactions.*;

import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 *
 */
public class IgniteCacheNearOnlyTxTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (getTestGridName(1).equals(gridName)) {
            cfg.setClientMode(true);

            cfg.setCacheConfiguration();
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearOnlyPutMultithreaded() throws Exception {
        final Ignite ignite1 = ignite(1);

        assertTrue(ignite1.configuration().isClientMode());

        ignite1.createNearCache(null, new NearCacheConfiguration<>());

        GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache cache = ignite1.cache(null);

                int key = 1;

                for (int i = 0; i < 100; i++)
                    cache.put(key, 1);

                return null;
            }
        }, 5, "put-thread");
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTx() throws Exception {
        txMultithreaded(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTx() throws Exception {
        txMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    private void txMultithreaded(final boolean optimistic) throws Exception {
        final Ignite ignite1 = ignite(1);

        assertTrue(ignite1.configuration().isClientMode());

        ignite1.createNearCache(null, new NearCacheConfiguration<>());

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache cache = ignite1.cache(null);

                int key = 1;

                IgniteTransactions txs = ignite1.transactions();

                for (int i = 0; i < 100; i++) {
                    try (Transaction tx = txs.txStart(optimistic ? OPTIMISTIC : PESSIMISTIC, REPEATABLE_READ)) {
                        cache.get(key);

                        cache.put(key, 1);

                        tx.commit();
                    }
                }

                return null;
            }
        }, 5, "put-thread");
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentTx() throws Exception {
        final Ignite ignite1 = ignite(1);

        assertTrue(ignite1.configuration().isClientMode());

        ignite1.createNearCache(null, new NearCacheConfiguration<>());

        IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache cache = ignite1.cache(null);

                int key = 1;

                for (int i = 0; i < 100; i++)
                    cache.put(key, 1);

                return null;
            }
        }, 5, "put1-thread");

        IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache cache = ignite1.cache(null);

                int key = 1;

                IgniteTransactions txs = ignite1.transactions();

                for (int i = 0; i < 100; i++) {
                    try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.get(key);

                        cache.put(key, 1);

                        tx.commit();
                    }
                }

                return null;
            }
        }, 5, "put2-thread");

        fut1.get();
        fut2.get();
    }
}
