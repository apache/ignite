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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests of replicated cache's 'get' requests distribution.
 */
public class ReplicatedAtomicCacheGetsDistributionTest extends GridCacheAbstractSelfTest {
    /** Cache name. */
    public static final String CACHE_NAME = "replicatedCache";

    /** Value prefix. */
    public static final String VAL_PREFIX = "val";

    /** */
    private static final int PRIMARY_KEYS_NUMBER = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getConfiguration("client").setClientMode(true));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTransactionConfiguration(transactionConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /**
     * Test 'get' operations requests distribution.
     *
     * @throws Exception In case of an error.
     */
    public void testGetRequestsDistribution() throws Exception {
        runTest(false);
    }

    /**
     * Test 'getAll' operations requests distribution.
     *
     * @throws Exception In case of an error.
     */
    public void testGetAllRequestsDistribution() throws Exception {
        runTest(true);
    }

    /**
     * @param batchMode Test mode.
     * @throws Exception In case of an error.
     */
    protected void runTest(boolean batchMode) throws Exception {
        for (int idx = 0; idx < gridCount(); idx++) {
            runTest(ignite(idx), batchMode);

            ignite(0).cache(CACHE_NAME).destroy();
        }
    }

    /**
     * @param dest Destination Ignite instance for requests distribution.
     * @param batchMode Test mode.
     * @throws Exception In case of an error.
     */
    protected void runTest(final Ignite dest, final boolean batchMode) throws Exception {
        final UUID destId = dest.cluster().localNode().id();

        GridTestUtils.setFieldValue(new GridCacheUtils(), "macsFilter", new NodeIdFilter(destId));

        IgniteCache<Integer, String> cache = grid(0).createCache(replicatedCache());

        List<Integer> keys = primaryKeys(cache, PRIMARY_KEYS_NUMBER);

        for (Integer key : keys)
            cache.put(key, VAL_PREFIX + key);

        cache = grid("client").getOrCreateCache(CACHE_NAME);

        try (Transaction tx = grid("client").transactions().txStart()) {
            if (batchMode) {
                Map<Integer, String> results = cache.getAll(new HashSet<>(keys));

                for (Map.Entry<Integer, String> entry : results.entrySet())
                    assertEquals(VAL_PREFIX + entry.getKey(), entry.getValue());
            }
            else {
                for (Integer key : keys)
                    assertEquals(VAL_PREFIX + key, cache.get(key));
            }

            tx.commit();
        }

        validateRequestsDistribution(destId);
    }

    /**
     * @param destId Destination node id.
     */
    protected void validateRequestsDistribution(final UUID destId) {
        for (int i = 0; i < gridCount(); i++) {
            IgniteEx ignite = grid(i);

            long getsCount = ignite.cache(CACHE_NAME).localMetrics().getCacheGets();

            if (destId.equals(ignite.localNode().id()))
                assertEquals(PRIMARY_KEYS_NUMBER, getsCount);
            else
                assertEquals(0L, getsCount);
        }
    }

    /**
     * @return Replicated cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> replicatedCache() {
        return new CacheConfiguration<K, V>(CACHE_NAME)
            .setCacheMode(REPLICATED)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAtomicityMode(atomicityMode())
            .setReadFromBackup(true)
            .setStatisticsEnabled(true);
    }

    /**
     * @return Transaction configuration.
     */
    protected TransactionConfiguration transactionConfiguration() {
        TransactionConfiguration txCfg = new TransactionConfiguration();

        txCfg.setDefaultTxIsolation(transactionIsolation());
        txCfg.setDefaultTxConcurrency(transactionConcurrency());

        return txCfg;
    }

    /**
     * @return Cache transaction isolation.
     */
    protected TransactionIsolation transactionIsolation() {
        return REPEATABLE_READ;
    }

    /**
     * @return Cache transaction concurrency.
     */
    protected TransactionConcurrency transactionConcurrency() {
        return PESSIMISTIC;
    }

    /** */
    protected static class NodeIdFilter implements IgniteBiPredicate<ClusterNode, ClusterNode> {
        /** Priority node id. */
        private UUID id;

        /**
         * @param id Priority node id.
         */
        public NodeIdFilter(UUID id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode n1, ClusterNode n2) {
            return n2.id().equals(id);
        }
    }
}
