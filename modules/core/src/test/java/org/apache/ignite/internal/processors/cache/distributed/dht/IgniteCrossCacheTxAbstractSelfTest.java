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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests specific combinations of cross-cache transactions.
 */
public abstract class IgniteCrossCacheTxAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String FIRST_CACHE = "FirstCache";

    /** */
    private static final String SECOND_CACHE = "SecondCache";

    /** */
    private static final int TX_CNT = 500;

    /**
     * @return Node count for this test.
     */
    private int nodeCount() {
        return 4;
    }

    /**
     * @return {@code True} if near cache should be enabled.
     */
    protected boolean nearEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(nodeCount());

        CacheConfiguration firstCfg = new CacheConfiguration(FIRST_CACHE);
        firstCfg.setBackups(1);
        firstCfg.setAtomicityMode(atomicityMode());
        firstCfg.setWriteSynchronizationMode(FULL_SYNC);

        grid(0).createCache(firstCfg);

        CacheConfiguration secondCfg = new CacheConfiguration(SECOND_CACHE);
        secondCfg.setBackups(1);
        secondCfg.setAtomicityMode(atomicityMode());
        secondCfg.setWriteSynchronizationMode(FULL_SYNC);

        if (nearEnabled())
            secondCfg.setNearConfiguration(new NearCacheConfiguration());

        grid(0).createCache(secondCfg);
    }

    /**
     * @return Atomicity mode.
     */
    public abstract CacheAtomicityMode atomicityMode();

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    protected void checkTxsSingleOp(TransactionConcurrency concurrency, TransactionIsolation isolation) throws Exception {
        Map<Integer, String> firstCheck = new HashMap<>();
        Map<Integer, String> secondCheck = new HashMap<>();

        for (int i = 0; i < TX_CNT; i++) {
            int grid = ThreadLocalRandom.current().nextInt(nodeCount());

            IgniteCache<Integer, String> first = grid(grid).cache(FIRST_CACHE);
            IgniteCache<Integer, String> second = grid(grid).cache(SECOND_CACHE);

            try (Transaction tx = grid(grid).transactions().txStart(concurrency, isolation)) {
                try {
                    int size = ThreadLocalRandom.current().nextInt(24) + 1;

                    for (int k = 0; k < size; k++) {
                        boolean rnd = ThreadLocalRandom.current().nextBoolean();

                        IgniteCache<Integer, String> cache = rnd ? first : second;
                        Map<Integer, String> check = rnd ? firstCheck : secondCheck;

                        String val = rnd ? "first" + i : "second" + i;

                        cache.put(k, val);
                        check.put(k, val);
                    }

                    tx.commit();
                }
                catch (Throwable e) {
                    e.printStackTrace();

                    throw e;
                }
            }

            if (i > 0 && i % 100 == 0)
                info("Finished iteration: " + i);
        }

        for (int g = 0; g < nodeCount(); g++) {
            IgniteEx grid = grid(g);

            assertEquals(0, grid.context().cache().context().tm().idMapSize());

            ClusterNode locNode = grid.localNode();

            IgniteCache<Object, Object> firstCache = grid.cache(FIRST_CACHE);

            for (Map.Entry<Integer, String> entry : firstCheck.entrySet()) {
                boolean primary = grid.affinity(FIRST_CACHE).isPrimary(locNode, entry.getKey());

                boolean backup = grid.affinity(FIRST_CACHE).isBackup(locNode, entry.getKey());

                assertEquals("Invalid value found first cache [primary=" + primary + ", backup=" + backup +
                        ", node=" + locNode.id() + ", key=" + entry.getKey() + ']',
                    entry.getValue(), firstCache.get(entry.getKey()));
            }

            for (Map.Entry<Integer, String> entry : secondCheck.entrySet()) {
                boolean primary = grid.affinity(SECOND_CACHE).isPrimary(locNode, entry.getKey());

                boolean backup = grid.affinity(SECOND_CACHE).isBackup(locNode, entry.getKey());

                assertEquals("Invalid value found second cache [primary=" + primary + ", backup=" + backup +
                        ", node=" + locNode.id() + ", key=" + entry.getKey() + ']',
                    entry.getValue(), grid.cache(SECOND_CACHE).get(entry.getKey()));
            }
        }
    }
}
