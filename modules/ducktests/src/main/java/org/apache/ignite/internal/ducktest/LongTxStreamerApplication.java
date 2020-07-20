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

package org.apache.ignite.internal.ducktest;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;

/**
 *
 */
public class LongTxStreamerApplication extends IgniteAwareApplication {
    /** Tx count. */
    private static final int TX_CNT = 100;

    /** Started. */
    private static final CountDownLatch started = new CountDownLatch(TX_CNT);

    /**
     * @param ignite Ignite.
     */
    public LongTxStreamerApplication(Ignite ignite) {
        super(ignite);
    }

    /** {@inheritDoc} */
    @Override public void run(String[] args) throws InterruptedException {
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(args[0]);

        log.info("Starting Long Tx...");

        for (int i = 0; i < TX_CNT; i++) {
            int finalI = i;

            new Thread(() -> {
                Transaction tx = ignite.transactions().txStart();

                cache.put(finalI, finalI);

                log.info("Long Tx started [key=" + finalI + "]");

                started.countDown();

                while (!terminated()) {
                    if (tx.state() != TransactionState.ACTIVE) {
                        log.info("Transaction broken. [key=" + finalI + "]");

                        break;
                    }

                    try {
                        U.sleep(1000);
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        // No-op.
                    }
                }

                log.info("Stopping tx thread [state=" + tx.state() + "]");

            }).start();
        }

        started.await();

        markInitialized();

        while (!terminated()) {
            Collection<IgniteInternalTx> active =
                ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

            log.info("Long Txs are in progress [txs=" + active.size() + "]");

            try {
                U.sleep(100); // Keeping node/txs alive.
            }
            catch (IgniteInterruptedCheckedException ignored) {
                log.info("Waiting interrupted.");
            }
        }

        markFinished();
    }
}
