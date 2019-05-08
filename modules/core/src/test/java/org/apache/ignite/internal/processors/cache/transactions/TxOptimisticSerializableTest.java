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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class TxOptimisticSerializableTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    public void testOptimistic() throws Exception {
        AtomicBoolean stop = new AtomicBoolean(false);
        IgniteInternalFuture<Long> fut = null;

        try {
            IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(new CacheConfiguration<>("test")
                .setBackups(1)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            for (int i = 0; i < 10_000; i++)
                cache.put(i, i);

            fut = GridTestUtils.runMultiThreadedAsync(() -> {
                while (!stop.get())
                    doTx(grid(0), cache);
            }, 2, "runner");

            for (int i = 0; i < 4; i++) {
                startGrid(4 + i);

                awaitPartitionMapExchange(true, true, null);

                U.sleep(2_000);
            }
        }
        finally {
            stop.set(true);

            if (fut != null)
                fut.get();
        }
    }

    /**
     * @param ig Ignite instance to run transaction on.
     * @param cache Cache to test.
     */
    private void doTx(Ignite ig, IgniteCache<Object, Object> cache) {
        Random rnd = ThreadLocalRandom.current();

        while (true) {
            try (Transaction tx = ig.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                int key1 = rnd.nextInt(10_000);
                int key2 = rnd.nextInt(10_000);

                cache.get(key1);
                cache.get(key2);

                cache.put(key1, key2);
                cache.put(key2, key1);

                tx.commit();

                break;
            }
            catch (TransactionOptimisticException | TransactionTimeoutException | CacheException ignore) {
                // retry
            }
        }
    }
}
