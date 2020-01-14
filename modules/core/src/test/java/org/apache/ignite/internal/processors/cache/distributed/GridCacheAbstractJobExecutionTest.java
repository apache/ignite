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

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Tests cache access from within jobs.
 */
public abstract class GridCacheAbstractJobExecutionTest extends GridCommonAbstractTest {
    /** Job counter. */
    private static final AtomicInteger cntr = new AtomicInteger(0);

    /** */
    private static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT, true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cache(DEFAULT_CACHE_NAME).removeAll();

        for (int i = 0; i < GRID_CNT; i++) {
            Ignite g = grid(i);

            IgniteCache<String, int[]> c = g.cache(DEFAULT_CACHE_NAME);

            GridCacheAdapter<Object, Object> cache = ((IgniteEx)g).context().cache().internalCache(DEFAULT_CACHE_NAME);

            info("Node: " + g.cluster().localNode().id());
            info("Entries: " + cache.entries());

            if (cache.context().isNear())
                info("DHT entries: " + cache.context().near().dht().entries());

            assertEquals("Cache is not empty, node [entries=" + c.localEntries() + ", igniteInstanceName=" +
                    g.name() + ']', 0, c.localSize());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticRepeatableRead() throws Exception {
        checkTransactions(PESSIMISTIC, REPEATABLE_READ, 1000);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticSerializable() throws Exception {
        checkTransactions(PESSIMISTIC, SERIALIZABLE, 1000);
    }

    /**
     * @param concur Concurrency.
     * @param isolation Isolation.
     * @param jobCnt Job count.
     * @throws Exception If fails.
     */
    private void checkTransactions(
        final TransactionConcurrency concur,
        final TransactionIsolation isolation,
        final int jobCnt
    ) throws Exception {
        info("Grid 0: " + grid(0).localNode().id());
        info("Grid 1: " + grid(1).localNode().id());
        info("Grid 2: " + grid(2).localNode().id());
        info("Grid 3: " + grid(3).localNode().id());

        Ignite ignite = grid(0);

        Collection<IgniteFuture<?>> futs = new LinkedList<>();

        final String key = "TestKey";

        info("Primary node for test key: " + grid(0).affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key));

        for (int i = 0; i < jobCnt; i++) {
            futs.add(ignite.compute().applyAsync(new CX1<Integer, Void>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Void applyx(final Integer i) {
                    IgniteCache<String, int[]> cache = ignite.cache(DEFAULT_CACHE_NAME);

                    try (Transaction tx = ignite.transactions().txStart(concur, isolation)) {
                        int[] arr = cache.get(key);

                        if (arr == null)
                            arr = new int[jobCnt];

                        arr[i] = 1;

                        cache.put(key, arr);

                        int c = cntr.getAndIncrement();

                        if (c % 50 == 0)
                            X.println("Executing transaction [i=" + i + ", c=" + c + ']');

                        tx.commit();
                    }

                    return null;
                }
            }, i));
        }

        for (IgniteFuture<?> fut : futs)
            fut.get(); // Wait for completion.

        for (int i = 0; i < GRID_CNT; i++) {
            info("Running iteration: " + i);

            for (int g = 0; g < GRID_CNT; g++) {
                info("Will check grid: " + g);

                info("Value: " + grid(i).cache(DEFAULT_CACHE_NAME).localPeek(key));
            }

            IgniteCache<String, int[]> c = grid(i).cache(DEFAULT_CACHE_NAME);

            // Do within transaction to make sure that lock is acquired
            // which means that all previous transactions have committed.
            try (Transaction tx = grid(i).transactions().txStart(concur, isolation)) {
                int[] arr = c.get(key);

                assertNotNull(arr);
                assertEquals(jobCnt, arr.length);

                for (int j : arr)
                    assertEquals(1, j);

                tx.commit();
            }
        }
    }
}
