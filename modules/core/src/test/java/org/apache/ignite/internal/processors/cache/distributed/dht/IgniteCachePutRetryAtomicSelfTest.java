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
package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.transactions.*;

import javax.cache.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 *
 */
public class IgniteCachePutRetryAtomicSelfTest extends IgniteCachePutRetryAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutInsideTransaction() throws Exception {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName("tx-cache");
        ccfg.setAtomicityMode(TRANSACTIONAL);

        try (IgniteCache<Integer, Integer> txCache = ignite(0).getOrCreateCache(ccfg)) {
            final AtomicBoolean finished = new AtomicBoolean();

            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finished.get()) {
                        stopGrid(3);

                        U.sleep(300);

                        startGrid(3);
                    }

                    return null;
                }
            });

            try {
                IgniteTransactions txs = ignite(0).transactions();

                IgniteCache<Object, Object> cache = ignite(0).cache(null);

                long stopTime = System.currentTimeMillis() + 60_000;

                while (System.currentTimeMillis() < stopTime) {
                    for (int i = 0; i < 10_000; i++) {
                        try {
                            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                txCache.put(0, 0);

                                cache.put(i, i);

                                tx.commit();
                            }
                        }
                        catch (IgniteException | CacheException e) {
                            log.info("Ignore exception: " + e);
                        }
                    }
                }

                finished.set(true);

                fut.get();
            }
            finally {
                finished.set(true);
            }
        }
    }
}
