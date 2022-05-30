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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class ConcurrentTxsConsistentCutTest extends AbstractConsistentCutTest {
    /** */
    private static final int CUTS = 10;

    /** */
    private static final int REPEAT = 1;

    /** */
    private final Map<IgniteUuid, Integer> txOrigNode = new ConcurrentHashMap<>();

    /** */
    @Parameterized.Parameter
    public int nodes;

    /** */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public int repeat;

    /** */
    @Parameterized.Parameters(name = "nodes={0} backups={1} repeat={2}")
    public static List<Object[]> params() {
        List<T2<Integer, Integer>> nodesAndBackups = F.asList(new T2<>(2, 0), new T2<>(2, 1), new T2<>(3, 2));

        List<Object[]> params = new ArrayList<>();

        for (int repeat = 0; repeat < REPEAT; repeat++) {
            for (T2<Integer, Integer> nb: nodesAndBackups)
                params.add(new Object[] {nb.get1(), nb.get2(), repeat});
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

    /** */
    @Test
    public void concurrentLoadAndCutTest() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        IgniteInternalFuture<Long> f = asyncLoadData(latch, 1);

        awaitConsistentCuts(CUTS, 0);

        // Disable new Consistent Cuts.
        grid(0).context().cache().context().consistentCutMgr().disable();

        latch.countDown();

        long expectTxCnt = f.get();

        checkWals(txOrigNode, CUTS, expectTxCnt);
    }

    /**
     * Starts creating transactions with concurrent load.
     *
     * @return Future that completes with full amount of transactions.
     */
    private IgniteInternalFuture<Long> asyncLoadData(CountDownLatch latch, int threads) throws Exception {
        LongAdder adder = new LongAdder();

        IgniteInternalFuture<?> asyncLoad = multithreadedAsync(() -> {
            Random r = new Random();

            while (latch.getCount() > 0) {
                // +1 - client node.
                int n = r.nextInt(nodes() + 1);

                Ignite g = grid(n);

                try (Transaction tx = g.transactions().txStart()) {
                    txOrigNode.put(tx.xid(), n);

                    int cnt = r.nextInt(nodes() + 1);

                    for (int j = 0; j < cnt; j++) {
                        IgniteCache<Integer, Integer> cache = cache(g);

                        cache.put(r.nextInt(100), r.nextInt());
                    }

                    // Skip fast txs on client node.
                    if (!(n == nodes() && cnt == 0))
                        adder.increment();

                    tx.commit();
                }
            }
        }, threads);

        return asyncLoad.chain((f) -> adder.sum());
    }

    /** */
    private IgniteCache<Integer, Integer> cache(Ignite g) {
        return g.cache(CACHE);
    }

    /** */
    private IgniteUuid txId(TxRecord tx) {
        return tx.nearXidVersion().asIgniteUuid();
    }
}
