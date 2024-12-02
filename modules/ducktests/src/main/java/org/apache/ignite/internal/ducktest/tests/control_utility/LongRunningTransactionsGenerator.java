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

package org.apache.ignite.internal.ducktest.tests.control_utility;

import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;

/**
 * Run long running transactions on node with specified param.
 */
public class LongRunningTransactionsGenerator extends IgniteAwareApplication {
    /** */
    private static final Duration TOPOLOGY_WAIT_TIMEOUT = Duration.ofSeconds(60);

    /** */
    private static final String KEYS_LOCKED_MESSAGE = "APPLICATION_KEYS_LOCKED";

    /** */
    private static final String LOCKED_KEY_PREFIX = "KEY_";

    /** */
    private volatile Executor pool;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        IgniteCache<String, String> cache = ignite.cache(jsonNode.get("cache_name").asText());

        int txCnt = jsonNode.get("tx_count") != null ? jsonNode.get("tx_count").asInt() : 1;

        int txSize = jsonNode.get("tx_size") != null ? jsonNode.get("tx_size").asInt() : 1;

        String keyPrefix = jsonNode.get("key_prefix") != null ? jsonNode.get("key_prefix").asText() : LOCKED_KEY_PREFIX;

        String lbl = jsonNode.get("label") != null ? jsonNode.get("label").asText() : null;

        long expectedTopVer = jsonNode.get("wait_for_topology_version") != null ?
            jsonNode.get("wait_for_topology_version").asLong() : -1L;

        CountDownLatch lockLatch = new CountDownLatch(txCnt);

        pool = Executors.newFixedThreadPool(2 * txCnt);

        markInitialized();

        if (expectedTopVer > 0) {
            log.info("Start waiting for topology version: " + expectedTopVer + ", " +
                "current version is: " + ignite.cluster().topologyVersion());

            long start = System.nanoTime();

            while (ignite.cluster().topologyVersion() < expectedTopVer
                && Duration.ofNanos(start - System.nanoTime()).compareTo(TOPOLOGY_WAIT_TIMEOUT) < 0)
                Thread.sleep(100L);

            log.info("Finished waiting for topology version: " + expectedTopVer + ", " +
                "current version is: " + ignite.cluster().topologyVersion());
        }

        for (int i = 0; i < txCnt; i++) {
            String key = keyPrefix + i;

            pool.execute(() -> {
                Lock lock = cache.lock(key);

                lock.lock();

                try {
                    lockLatch.countDown();

                    while (!terminated())
                        Thread.sleep(100L);
                }
                catch (InterruptedException e) {
                    markBroken(new RuntimeException("Unexpected thread interruption", e));

                    Thread.currentThread().interrupt();
                }
                finally {
                    lock.unlock();
                }
            });
        }

        lockLatch.await();

        log.info(KEYS_LOCKED_MESSAGE);

        CountDownLatch txLatch = new CountDownLatch(txCnt);

        for (int i = 0; i < txCnt; i++) {
            Map<String, String> data = new TreeMap<>();

            for (int j = 0; j < txSize; j++) {
                String key = keyPrefix + (j == 0 ? String.valueOf(i) : i + "_" + j);

                data.put(key, key);
            }

            IgniteTransactions igniteTransactions = lbl != null ? ignite.transactions().withLabel(lbl) :
                ignite.transactions();

            pool.execute(() -> {
                IgniteUuid xid = null;

                try (Transaction tx = igniteTransactions.txStart()) {
                    xid = tx.xid();

                    cache.putAll(data);

                    tx.commit();
                }
                catch (Exception e) {
                    if (e instanceof CacheException && e.getCause() != null &&
                        e.getCause() instanceof TransactionRollbackException)
                        recordResult("TX_ID", xid != null ? xid.toString() : "");
                    else
                        markBroken(new RuntimeException("Transaction is rolled back with unexpected error", e));
                }
                finally {
                    txLatch.countDown();
                }
            });
        }

        txLatch.await();

        markFinished();
    }
}
