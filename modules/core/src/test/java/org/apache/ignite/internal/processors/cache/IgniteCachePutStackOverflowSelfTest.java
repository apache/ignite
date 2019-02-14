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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCachePutStackOverflowSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStackLocal() throws Exception {
        checkCache(CacheMode.LOCAL);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStackPartitioned() throws Exception {
        checkCache(CacheMode.PARTITIONED);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStackReplicated() throws Exception {
        checkCache(CacheMode.REPLICATED);
    }

    /**
     * @throws Exception if failed.
     */
    private void checkCache(CacheMode mode) throws Exception {
        final Ignite ignite = ignite(0);

        final IgniteCache<Object, Object> cache = ignite.getOrCreateCache(new CacheConfiguration<>("cache")
            .setCacheMode(mode)
            .setAtomicityMode(TRANSACTIONAL));

        try {
            Thread[] threads = new Thread[256];

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                // Lock the key.
                final String key = "key";

                cache.get(key);

                // Simulate high contention.
                for (int i = 0; i < threads.length; i++) {
                    threads[i] = new Thread() {
                        @Override public void run() {
                            cache.put(key, 1);
                        }
                    };

                    threads[i].start();
                }

                U.sleep(2_000);

                cache.put(key, 1);

                tx.commit();
            }

            System.out.println("Waiting for threads to finish...");

            for (Thread thread : threads)
                thread.join();
        }
        finally {
            ignite.destroyCache("cache");
        }
    }
}
