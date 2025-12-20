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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

/**
 * Test partitions consistency in case of noop operations.
 */
public class TxPartitionCounterStateConsistencyNoopInvokeTest extends TxPartitionCounterStateAbstractTest {
    /**{@inheritDoc} */
    @Override protected int partitions() {
        return 1;
    }

    /**
     * Test primary-backup partitions consistency when entry processor produce NOOP results.
     */
    @CartesianTest(name = "concurrency={0}, isolation={1}")
    public void testPartitionConsistencyAfterNoopInvoke(
        @CartesianTest.Enum(value = TransactionConcurrency.class) TransactionConcurrency concurrency,
        @CartesianTest.Enum(value = TransactionIsolation.class) TransactionIsolation isolation
    ) throws Exception {
        backups = 2;

        startGrids(2).cluster().state(ClusterState.ACTIVE);

        enableCheckpoints(grid(0), false);
        enableCheckpoints(grid(1), false);

        final Ignite pri = grid(0);

        IgniteCache<Integer, Integer> cache = pri.cache(DEFAULT_CACHE_NAME);

        Map<Integer, Integer> data = new TreeMap<>();

        for (int i = 0; i < 25; i++)
            data.put(i, i);
        for (int i = 25; i < 50; i++)
            data.put(i, -i);

        cache.putAll(data);

        try (final Transaction tx = pri.transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < 100; i++)
                cache.invoke(i, new MyEntryProcessor(false));

            tx.commit();
        }

        try (final Transaction tx = pri.transactions().txStart(concurrency, isolation)) {
            for (int i = 0; i < 100; i++)
                cache.invoke(i, new MyEntryProcessor(true));

            tx.commit();
        }

        validateCounters();

        // Restart grid and check WAL records correctly applied.
        stopAllGrids();
        startGrids(2).cluster().state(ClusterState.ACTIVE);

        validateCounters();
    }

    /**
     * Validates partition has same counters on both nodes.
     */
    private void validateCounters() {
        Map<Integer, Long> cntrs = new HashMap<>();

        grid(0).context().cache().context()
            .cacheContext(CU.cacheId(DEFAULT_CACHE_NAME))
            .offheap()
            .cacheDataStores()
            .forEach(ds -> cntrs.put(ds.partId(), ds.updateCounter()));

        grid(1).context().cache().context()
            .cacheContext(CU.cacheId(DEFAULT_CACHE_NAME))
            .offheap()
            .cacheDataStores()
            .forEach(ds -> assertEquals("part=" + ds.partId(), (long)cntrs.get(ds.partId()), ds.updateCounter()));
    }

    /**
     * Entry processor for tests.
     */
    protected static class MyEntryProcessor implements EntryProcessor<Integer, Integer, Object> {
        /** If {@code true} invert positives, otherwise invert negatives. */
        private final boolean invert;

        /**
         * Contructor
         * @param invert If {@code true} invert positives, otherwise invert negatives.
         */
        public MyEntryProcessor(boolean invert) {
            this.invert = invert;
        }

        /**{@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            Integer val = null;

            if (e.exists()) {
                val = e.getValue();

                if (invert) {
                    if (val > 0)
                        e.setValue(-val);
                    else
                        return val;
                }
                else {
                    if (val < 0)
                        e.setValue(-val);
                    else
                        return val;
                }
            }
            else
                return val;

            return val;
        }
    }
}
