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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheOperationsInterruptTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInterruptPessimisticTx() throws Exception {
        final int NODES = 3;

        startGrids(NODES);

        awaitPartitionMapExchange();

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        Ignite node = ignite(0);

        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

        final int KEYS = 100;

        final boolean changeTop = true;

        for (int i = 0; i < 10; i++) {
            info("Iteration: " + i);

            final AtomicBoolean stop = new AtomicBoolean();

            try {
                IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                    @Override public void run() {
                        Ignite node = ignite(0);

                        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (!stop.get()) {
                            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                for (int i = 0; i < KEYS; i++) {
                                    if (rnd.nextBoolean())
                                        cache.get(i);
                                }
                            }
                        }
                    }
                }, 3, "tx-thread");

                IgniteInternalFuture<?> changeTopFut = null;

                if (changeTop) {
                    changeTopFut = GridTestUtils.runAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            while (!stop.get()) {
                                startGrid(NODES);

                                stopGrid(NODES);
                            }

                            return null;
                        }
                    });
                }

                U.sleep(rnd.nextInt(500));

                fut.cancel();

                U.sleep(rnd.nextInt(500));

                stop.set(true);

                try {
                    fut.get();
                }
                catch (Exception e) {
                    info("Ignore error: " + e);
                }

                if (changeTopFut != null)
                    changeTopFut.get();

                info("Try get");

                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    for (int k = 0; k < KEYS; k++)
                        cache.get(k);
                }

                info("Try get done");

                startGrid(NODES);
                stopGrid(NODES);
            }
            finally {
                stop.set(true);
            }
        }
    }
}
