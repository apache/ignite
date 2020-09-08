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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.transactions.Transaction;

public class LongRunningTransaction extends IgniteAwareApplication {
    private static final String LOCKED_KEY_PREFIX = "KEY_";

    private volatile Executor pool;

    private volatile boolean holdLock = true;

    @Override protected void run(JsonNode jsonNode) throws Exception {
        IgniteCache<String, String> cache = ignite.cache(jsonNode.get("cacheName").asText());
        int numTx = jsonNode.get("numTx") != null ? jsonNode.get("numTx").asInt() : 1;
        String keyPrefix = jsonNode.get("keyPrefix") != null ? jsonNode.get("keyPrefix").asText() : LOCKED_KEY_PREFIX;


        CountDownLatch lockLatch = new CountDownLatch(numTx);
        pool = Executors.newFixedThreadPool(2 * numTx);

        for (int i = 0; i < numTx; i++) {
            String key = keyPrefix + i;
            String value = "VALUE_" + i;

            pool.execute(new Runnable() {
                @Override public void run() {
                    cache.put(key, value);

                    Lock lock = cache.lock(key);

                    lock.lock();
                    try {
                        lockLatch.countDown();
                        while (holdLock)
                            Thread.sleep(2000L);
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

        markInitialized();

        CountDownLatch txLatch = new CountDownLatch(numTx);
        for (int i = 0; i < numTx; i++) {
            String key = keyPrefix + i;
            String value = "VALUE_LOCK_" + i;

            Runnable txClo = () -> {
                try (Transaction tx = ignite.transactions().txStart()) {
                    cache.put(key, value);

                    tx.commit();
                } catch (Exception e) {
                    log.info("Transaction is rolled back with error:", e);
                    markFinished();
                } finally {
                    txLatch.countDown();
                }
            };

            if (i == numTx - 1)
                txClo.run();
            else
                pool.execute(txClo);
        }

        txLatch.await();

        holdLock = false;
    }
}
