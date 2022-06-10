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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Load Ignite with transactions and starts Consistent Cut concurrently. */
@RunWith(Parameterized.class)
public class ConcurrentTxsConsistentCutTest extends AbstractConsistentCutTest {
    /** Amount of Consistent Cuts to await. */
    private static final int CUTS = 10;

    /** How many times repeat the test. */
    private static final int REPEAT = 1;

    /** */
    private final Map<IgniteUuid, Integer> txOrigNode = new ConcurrentHashMap<>();

    /** Number of server nodes. */
    @Parameterized.Parameter
    public int nodes;

    /** Number of backups. */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public int repeat;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Enable scheduling Consistent Cut procedure.
        grid(0).context().cache().context().consistentCutMgr().enable();
    }

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

        IgniteInternalFuture<?> f = asyncLoadData(latch, 2);

        awaitConsistentCuts(CUTS, 0);

        // Disable new Consistent Cuts.
        grid(0).context().cache().context().consistentCutMgr().disable();

        latch.countDown();

        f.get();

        checkWalsConsistency(txOrigNode, CUTS);
    }

    /**
     * Starts creating transactions with concurrent load.
     *
     * @return Future that completes with full amount of transactions.
     */
    private IgniteInternalFuture<?> asyncLoadData(CountDownLatch latch, int threads) throws Exception {
        return multithreadedAsync(() -> {
            Random r = new Random();

            while (latch.getCount() > 0) {
                // +1 - client node.
                int n = r.nextInt(nodes() + 1);

                Ignite g = grid(n);

                try (Transaction tx = g.transactions().txStart()) {
                    txOrigNode.put(tx.xid(), n);

                    int cnt = 1 + r.nextInt(nodes());

                    for (int j = 0; j < cnt; j++) {
                        IgniteCache<Integer, Integer> cache = g.cache(CACHE);

                        cache.put(r.nextInt(100), r.nextInt());
                    }

                    tx.commit();
                }
            }
        }, threads);
    }

    /**
     * @param cuts Amount of Consistent Cut to await.
     * @param prevCutVer Previous Consistent Cut version (timestamp).
     */
    protected void awaitConsistentCuts(int cuts, long prevCutVer) throws Exception {
        for (int i = 0; i < cuts; i++) {
            prevCutVer = awaitGlobalCutReady(prevCutVer);

            log.info("Consistent Cut finished: " + prevCutVer);
        }
    }
}
