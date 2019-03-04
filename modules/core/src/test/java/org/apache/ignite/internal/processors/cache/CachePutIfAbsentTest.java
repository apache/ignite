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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CachePutIfAbsentTest extends GridCommonAbstractTest {
    /** */
    private static final int SRVS = 4;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /**
     * @return Cache configurations.
     */
    private List<CacheConfiguration<Integer, Integer>> cacheConfigurations() {
        List<CacheConfiguration<Integer, Integer>> ccfgs = new ArrayList<>();

        ccfgs.add(cacheConfiguration(REPLICATED, FULL_SYNC, 0));

        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 0));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 1));
        ccfgs.add(cacheConfiguration(PARTITIONED, FULL_SYNC, 2));

        return ccfgs;
    }

    /**
     * @param cacheMode Cache mode.
     * @param syncMode Write synchronization mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(
        CacheMode cacheMode,
        CacheWriteSynchronizationMode syncMode,
        int backups) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(syncMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxConflictGetAndPutIfAbsent() throws Exception {
        Ignite ignite0 = ignite(0);

        final IgniteTransactions txs = ignite0.transactions();

        for (CacheConfiguration<Integer, Integer> ccfg : cacheConfigurations()) {
            try {
                IgniteCache<Integer, Integer> cache = ignite0.createCache(ccfg);

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < 10; i++) {
                    Integer key = rnd.nextInt(10_000);

                    cache.put(key, 2);

                    for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                        for (TransactionIsolation isolation : TransactionIsolation.values()) {
                            if (MvccFeatureChecker.forcedMvcc() && !MvccFeatureChecker.isSupported(concurrency, isolation))
                                continue;

                            try (Transaction tx = txs.txStart(concurrency, isolation)) {
                                Object old = cache.getAndPutIfAbsent(key, 3);

                                assertEquals(2, old);

                                Object val = cache.get(key);

                                assertEquals(2, val);

                                tx.commit();
                            }

                            assertEquals((Integer)2, cache.get(key));
                        }
                    }
                }
            }
            finally {
                ignite0.destroyCache(ccfg.getName());
            }
        }
    }
}
