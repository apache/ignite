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

package org.apache.ignite.internal.ducktest.tests.pme_free_switch_test;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import com.fasterxml.jackson.databind.JsonNode;
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

    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) throws InterruptedException {
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(jsonNode.get("cacheName").asText());

        log.info("Starting Long Tx...");

        for (int i = -1; i >= -TX_CNT; i--) { // Negative keys to have no intersection with load.
            int finalI = i;

            new Thread(() -> {
                Transaction tx = ignite.transactions().txStart();

                cache.put(finalI, finalI);

                log.info("Long Tx started [key=" + finalI + "]");

                started.countDown();

                while (!terminated()) {
                    if (tx.state() != TransactionState.ACTIVE) {
                        log.info("Transaction broken. [key=" + finalI + "]");

                        markBroken(new IllegalStateException(
                            "Illegal Tx state [key=" + finalI + " state=" + tx.state() + "]"));
                    }

                    try {
                        U.sleep(10);
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        // No-op.
                    }
                }

                log.info("Stopping tx thread [key=" + finalI + " state=" + tx.state() + "]");

                tx.rollback();

                log.info("Finishing tx thread [key=" + finalI + " state=" + tx.state() + "]");

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
                log.info("Waiting for interrupted.");
            }
        }

        while (!((IgniteEx)ignite).context().cache().context().tm().activeTransactions().isEmpty())
            try {
                U.sleep(100); // Keeping node alive.
            }
            catch (IgniteInterruptedCheckedException ignored) {
                log.info("Waiting for tx rollback.");
            }

        markFinished();
    }
}
