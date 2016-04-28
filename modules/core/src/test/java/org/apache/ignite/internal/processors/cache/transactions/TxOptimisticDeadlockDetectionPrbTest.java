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

import java.util.Collection;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxOptimisticDeadlockDetectionPrbTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override public boolean isDebug() {
        return false;
    }


    /**
     * @throws Exception If failed.
     */
    public void testLocal() throws Exception {
        try {
            startGrid(0);

            final Ignite ignite = ignite(0);

            CacheConfiguration<Integer, Integer> ccfg = defaultCacheConfiguration();

            ccfg.setCacheMode(LOCAL);
            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setNearConfiguration(null);
            ccfg.setName("cache");

            final IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(ccfg);

            final AtomicInteger threadCnt = new AtomicInteger();

            final CyclicBarrier barrier = new CyclicBarrier(2);

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ, 0, 0)) {
                        int num = threadCnt.incrementAndGet();

                        cache.put(num == 1 ? 1 : 2, 1);

                        //barrier.await();

                        cache.put(num == 1 ? 2 : 1, 2);

                        log.info("!!! precommit");

                        tx.commit();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, 2, "tx-thread");

/*
        U.sleep(2000);

        IgniteTxManager tm = ((IgniteKernal)ignite).context().cache().context().tm();

        Collection<IgniteInternalTx> txs = tm.activeTransactions();
*/

            try {
                fut.get();
            }
            catch (IgniteCheckedException e) {
                assertTrue(X.hasCause(e, TransactionDeadlockException.class));
            }

            System.out.println();
        }
        finally {
            stopAllGrids();
        }
    }


}
