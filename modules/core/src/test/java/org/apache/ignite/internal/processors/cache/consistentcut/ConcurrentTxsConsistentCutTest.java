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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;

/** Load Ignite with transactions and starts Consistent Cut concurrently. */
@RunWith(Parameterized.class)
public class ConcurrentTxsConsistentCutTest extends AbstractConsistentCutTest {
    /** Amount of Consistent Cuts to await. */
    private static final int CUTS = 20;

    /** */
    private static final Random RND = new Random();

    /** */
    private final AtomicInteger txCnt = new AtomicInteger();

    /** Notifies data loader to stop preparing new transactions. */
    private volatile CountDownLatch stopLoadLatch;

    /** Number of server nodes. */
    @Parameterized.Parameter
    public int nodes;

    /** Number of backups. */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public boolean withNearCache;

    /** */
    @Parameterized.Parameters(name = "nodes={0}, backups={1}, withNearCache={2}")
    public static List<Object[]> params() {
        int[][] nodesAndBackups = new int[][] {
            new int[] {3, 0},
            new int[] {2, 1},
            new int[] {3, 2}
        };

        List<Object[]> params = new ArrayList<>();

        for (int[] nb: nodesAndBackups) {
            for (boolean near: new boolean[] {false, true})
                params.add(new Object[] {nb[0], nb[1], near});
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return nodes;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return backups;
    }

    /** {@inheritDoc} */
    @Override protected boolean withNearCache() {
        return withNearCache;
    }

    /** */
    @Test
    public void noLoadTest() throws Exception {
        testConcurrentTransactionsAndCuts(() -> false);
    }

    /** */
    @Test
    public void concurrentLoadTransactionsTest() throws Exception {
        testConcurrentTransactionsAndCuts(() -> tx(false));
    }

    /** */
    @Test
    public void concurrentLoadTransactionsWithRollbackTest() throws Exception {
        testConcurrentTransactionsAndCuts(() -> tx(true));
    }

    /** */
    @Test
    public void concurrentLoadImplicitTransactionsTest() throws Exception {
        testConcurrentTransactionsAndCuts(() -> {
            // +1 - client node.
            int n = RND.nextInt(nodes() + 1);

            IgniteCache<Integer, Integer> cache = grid(n).cache(CACHE);

            cache.put(RND.nextInt(), RND.nextInt());

            return true;
        });
    }

    /** */
    @Test
    public void concurrentLoadImplicitTransactionsAndExplicitLocksTest() throws Exception {
        testConcurrentTransactionsAndCuts(() -> {
            // +1 - client node.
            int n = RND.nextInt(nodes() + 1);

            IgniteCache<Integer, Integer> cache = grid(n).cache(CACHE);

            int key = RND.nextInt();

            Lock lock = cache.lock(key);

            lock.lock();

            try {
                cache.put(key, RND.nextInt());
            }
            finally {
                lock.unlock();
            }

            return true;
        });
    }

    /** */
    private boolean tx(boolean withRollback) {
        // +1 - client node.
        int n = RND.nextInt(nodes() + 1);

        Ignite g = grid(n);

        try (Transaction tx = g.transactions().txStart()) {
            int cnt = 1 + RND.nextInt(nodes());

            for (int j = 0; j < cnt; j++) {
                IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                cache.put(RND.nextInt(), RND.nextInt());
            }

            if (withRollback && RND.nextBoolean()) {
                tx.rollback();

                return false;
            }
            else {
                tx.commit();

                return true;
            }
        }
    }

    /** */
    private void testConcurrentTransactionsAndCuts(Supplier<Boolean> tx) throws Exception {
        stopLoadLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> f = GridTestUtils.runMultiThreadedAsync(() -> {
            while (stopLoadLatch.getCount() > 0) {
                if (tx.get())
                    txCnt.incrementAndGet();
            }
        }, 2, "async-load");

        for (int i = 0; i < CUTS; i++) {
            awaitSnapshotResourcesCleaned();

            snp(grid(0)).createIncrementalSnapshot(SNP).get(getTestTimeout());

            log.info("Consistent Cut finished: " + i);
        }

        stopLoadLatch.countDown();

        f.get();

        checkWalsConsistency(txCnt.get(), CUTS);
    }
}
