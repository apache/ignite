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

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.ignite.lang.IgniteClosure;
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
    private static final int PRIMARY_KEYS_NUMBER = 1_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        assert PRIMARY_KEYS_NUMBER % gridCount() == 0;

        startGrid(getConfiguration("client").setClientMode(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteCache cache = ignite(0).cache(CACHE_NAME);

        if (cache != null)
            cache.destroy();
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
     * Test 'get' operations requests generator distribution.
     *
     * @throws Exception In case of an error.
     */
    public void testGetRequestsGeneratorDistribution() throws Exception {
        runTestGetRequestsGeneratorDistribution(false);
    }

    /**
     * Test 'getAll' operations requests generator distribution.
     *
     * @throws Exception In case of an error.
     */
    public void testGetAllRequestsGeneratorDistribution() throws Exception {
        runTestGetRequestsGeneratorDistribution(true);
    }

    /**
     * @param batchMode Test mode.
     * @throws Exception In case of an error.
     */
    protected void runTestGetRequestsGeneratorDistribution(boolean batchMode) throws Exception {
        GridTestUtils.setFieldValue(new GridCacheUtils(), "macsFilter", new NodeIdFilter(null)); // Always false.

        GridTestUtils.setFieldValue(new GridCacheUtils(), "rndClo", new RoundRobinDistributionClosure());

        IgniteCache<Integer, String> cache = grid(0).createCache(replicatedCache());

        Set<Integer> keys = new TreeSet<>(primaryKeys(cache, PRIMARY_KEYS_NUMBER));

        for (Integer key : keys)
            cache.put(key, VAL_PREFIX + key);

        cache = grid("client").getOrCreateCache(CACHE_NAME);

        getAndValidateData(cache, keys, batchMode);

        int expected = PRIMARY_KEYS_NUMBER / gridCount();

        for (int i = 0; i < gridCount(); i++) {
            IgniteEx ignite = grid(i);

            long getsCount = ignite.cache(CACHE_NAME).localMetrics().getCacheGets();

            assertEquals(expected, getsCount);
        }
    }

    /**
     * Test 'get' operations requests distribution.
     *
     * @throws Exception In case of an error.
     */
    public void testGetRequestsDistribution() throws Exception {
        runTestGetAllRequestsDistribution(false);
    }

    /**
     * Test 'getAll' operations requests distribution.
     *
     * @throws Exception In case of an error.
     */
    public void testGetAllRequestsDistribution() throws Exception {
        runTestGetAllRequestsDistribution(true);
    }

    /**
     * @param batchMode Test mode.
     * @throws Exception In case of an error.
     */
    protected void runTestGetAllRequestsDistribution(boolean batchMode) throws Exception {
        for (int idx = 0; idx < gridCount(); idx++) {
            runTestGetAllRequestsDistribution(ignite(idx), batchMode);

            ignite(0).cache(CACHE_NAME).destroy();
        }
    }

    /**
     * @param dest Destination Ignite instance for requests distribution.
     * @param batchMode Test mode.
     * @throws Exception In case of an error.
     */
    protected void runTestGetAllRequestsDistribution(final Ignite dest, final boolean batchMode) throws Exception {
        final UUID destId = dest.cluster().localNode().id();

        GridTestUtils.setFieldValue(new GridCacheUtils(), "macsFilter", new NodeIdFilter(destId));

        IgniteCache<Integer, String> cache = grid(0).createCache(replicatedCache());

        Set<Integer> keys = new TreeSet<>(primaryKeys(cache, PRIMARY_KEYS_NUMBER));

        for (Integer key : keys)
            cache.put(key, VAL_PREFIX + key);

        cache = grid("client").getOrCreateCache(CACHE_NAME);

        getAndValidateData(cache, keys, batchMode);

        validateRequestsDistribution(destId);
    }

    /**
     * @param cache Ignite cache.
     * @param keys Keys to get.
     * @param batchMode Test mode.
     */
    protected void getAndValidateData(IgniteCache<Integer, String> cache, Set<Integer> keys, boolean batchMode) {
        try (Transaction tx = grid("client").transactions().txStart()) {
            if (batchMode) {
                Map<Integer, String> results = cache.getAll(keys);

                for (Map.Entry<Integer, String> entry : results.entrySet())
                    assertEquals(VAL_PREFIX + entry.getKey(), entry.getValue());
            }
            else {
                for (Integer key : keys)
                    assertEquals(VAL_PREFIX + key, cache.get(key));
            }

            tx.commit();
        }
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
    private static class RoundRobinDistributionClosure implements IgniteClosure<Integer, Integer> {
        /** Counter. */
        private int counter;

        /** {@inheritDoc} */
        @Override public Integer apply(Integer num) {
            return counter++ % num;
        }
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
