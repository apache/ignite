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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Test ensures that the put operation does not hang during asynchronous cache destroy.
 */
public abstract class IgniteCachePutOnDestroyTest extends GridCommonAbstractTest {
    /** Iteration count. */
    protected static final int ITER_CNT = 50;

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Worker threads timeout. */
    private static final int TIMEOUT = 10_000;

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected <K, V> CacheConfiguration<K, V> cacheConfiguration(String cacheName, String grpName) {
        CacheConfiguration<K, V> cfg = new CacheConfiguration<>();

        cfg.setName(cacheName);
        cfg.setGroupName(grpName);
        cfg.setAtomicityMode(atomicityMode());

        return cfg;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testPutOnCacheDestroy() throws IgniteCheckedException {
        for (int n = 0; n < ITER_CNT; n++)
            doTestPutOnCacheDestroy(null, null);
    }

    /**
     * @param concurrency Transaction concurrency level.
     * @param isolation Transaction isolation level.
     * @throws IgniteCheckedException If failed.
     */
    protected void doTestPutOnCacheDestroy(TransactionConcurrency concurrency,
        TransactionIsolation isolation) throws IgniteCheckedException {
        String grpName = "testGroup";

        boolean explicitTx = concurrency != null && isolation != null;

        Ignite ignite = grid(0);

        IgniteCache additionalCache = ignite.createCache(cacheConfiguration("cache1", grpName));

        try {
            IgniteCache<Integer, Boolean> cache = ignite.getOrCreateCache(cacheConfiguration("cache2", grpName));

            AtomicInteger cntr = new AtomicInteger();

            GridTestUtils.runMultiThreadedAsync(() -> {
                try {
                    int key;

                    while ((key = cntr.getAndIncrement()) < 2_000) {
                        if (key == 1_000) {
                            cache.destroy();

                            break;
                        }

                        if (explicitTx) {
                            try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                                cache.put(key, true);

                                tx.commit();
                            }
                        }
                        else
                            cache.put(key, true);
                    }
                }
                catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), hasCacheStoppedMessage(e));
                }

                return null;
            }, 6, "put-thread").get(TIMEOUT);
        }
        finally {
            additionalCache.destroy();
        }
    }

    /**
     * Validate exception.
     *
     * @param e Exception.
     * @return {@code True} if exception (or cause) is instance of {@link CacheStoppedException} or
     * {@link IllegalStateException} and message contains "cache" and "stopped" keywords.
     */
    private boolean hasCacheStoppedMessage(Exception e) {
        for (Throwable t : X.getThrowableList(e)) {
            if (t.getClass() == CacheStoppedException.class || t.getClass() == IllegalStateException.class) {
                String errMsg = t.getMessage().toLowerCase();

                if (errMsg.contains("cache") && errMsg.contains("stopped"))
                    return true;
            }
        }

        return false;
    }
}
