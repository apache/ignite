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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.CacheException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;

/**
 * Run long running transactions on node with specified param.
 */
public class LongRunningTransaction extends IgniteAwareApplication {
    /** */
    private static final Duration TOPOLOGY_WAIT_TIMEOUT = Duration.ofSeconds(60);

    /** */
    private static final String KEY_LOCKED_MESSAGE = "APPLICATION_KEY_LOCKED";

    /** */
    private static final String LOCKED_KEY_PREFIX = "KEY_";

    /** */
    private volatile Executor pool;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        IgniteCache<String, String> cache = ignite.cache(jsonNode.get("cache_name").asText());

        int txCount = jsonNode.get("tx_count") != null ? jsonNode.get("tx_count").asInt() : 1;

        int txSize = jsonNode.get("tx_size") != null ? jsonNode.get("tx_size").asInt() : 1;

        String keyPrefix = jsonNode.get("key_prefix") != null ? jsonNode.get("key_prefix").asText() : LOCKED_KEY_PREFIX;

        String label = jsonNode.get("label") != null ? jsonNode.get("label").asText() : null;

        long expectedTopologyVersion = jsonNode.get("wait_for_topology_version") != null ?
            jsonNode.get("wait_for_topology_version").asLong() : -1L;

        CountDownLatch lockLatch = new CountDownLatch(txCount);

        pool = Executors.newFixedThreadPool(2 * txCount);

        markInitialized();

        if (expectedTopologyVersion > 0) {
            log.info("Start waiting for topology version: " + expectedTopologyVersion + ", " +
                "current version is: " + ignite.cluster().topologyVersion());

            long start = System.nanoTime();

            while (ignite.cluster().topologyVersion() < expectedTopologyVersion
                && Duration.ofNanos(start - System.nanoTime()).compareTo(TOPOLOGY_WAIT_TIMEOUT) < 0)
                Thread.sleep(100L);

            log.info("Finished waiting for topology version: " + expectedTopologyVersion + ", " +
                "current version is: " + ignite.cluster().topologyVersion());
        }

        for (int i = 0; i < txCount; i++) {
            String key = keyPrefix + i;
            String value = "VALUE_" + i;

            pool.execute(new Runnable() {
                @Override public void run() {
                    cache.put(key, value);

                    Lock lock = cache.lock(key);

                    lock.lock();
                    try {
                        lockLatch.countDown();

                        while (!terminated())
                            Thread.sleep(100L);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    finally {
                        lock.unlock();
                    }
                }
            });
        }

        lockLatch.await();

        log.info(KEY_LOCKED_MESSAGE);

        CountDownLatch txLatch = new CountDownLatch(txCount);
        for (int i = 0; i < txCount; i++) {
            int txIdx = i;

            Map<String, String> data = IntStream.range(0, txSize)
                .mapToObj(idx -> {
                    String suffix = idx == 0 ? String.valueOf(txIdx) : txIdx + "_" + idx;

                    return new IgnitePair<>(keyPrefix + suffix, "VALUE_LOCK_" + suffix);
                })
                .collect(
                    Collectors.toMap(
                        IgnitePair::getKey,
                        IgnitePair::getValue,
                        (k0, k1) -> {
                            throw new IllegalStateException(String.format("Duplicate key %s", k0));
                        },
                        TreeMap::new
                    )
                );

            IgniteTransactions igniteTransactions = label != null ? ignite.transactions().withLabel(label) :
                ignite.transactions();

            Runnable txClo = () -> {
                IgniteUuid xid = null;

                try (Transaction tx = igniteTransactions.txStart()) {
                    xid = tx.xid();

                    cache.putAll(data);

                    tx.commit();
                }
                catch (Exception e) {
                    if (e instanceof CacheException && e.getCause() != null &&
                        e.getCause() instanceof TransactionRollbackException)
                        recordResult("TX_ID", xid.toString());
                    else {
                        markBroken();

                        log.info("Transaction is rolled back with error:", e);
                    }
                }
                finally {
                    txLatch.countDown();
                }
            };

            pool.execute(txClo);
        }

        txLatch.await();

        markFinished();
    }
}
