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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests of replicated cache's 'get' requests distribution.
 */
public class ReplicatedAtomicCacheGetsDistributionTest extends GridCacheAbstractSelfTest {
    /** Cache name. */
    private static final String CACHE_NAME = "replicatedCache";

    /** Client nodes instance's name. */
    private static final String CLIENT_NAME = "client";

    /** Value prefix. */
    private static final String VAL_PREFIX = "val";

    /** */
    private static final int PRIMARY_KEYS_NUMBER = 1_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(getConfiguration(CLIENT_NAME).setClientMode(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteCache cache = ignite(0).cache(CACHE_NAME);

        if (cache != null)
            cache.destroy();

        // Setting different MAC addresses for all nodes
        Map<UUID, String> macs = getClusterMacs();

        int idx = 0;

        for (Map.Entry<UUID, String> entry : macs.entrySet())
            entry.setValue("x2-xx-xx-xx-xx-x" + idx++);

        replaceMacAddresses(G.allGrids(), macs);
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
     * @see #runTestGetRequestsGeneratorDistribution(boolean)
     */
    public void testGetRequestsGeneratorDistribution() throws Exception {
        runTestGetRequestsGeneratorDistribution(false);
    }

    /**
     * Test 'getAll' operations requests generator distribution.
     *
     * @throws Exception In case of an error.
     * @see #runTestGetRequestsGeneratorDistribution(boolean)
     */
    public void testGetAllRequestsGeneratorDistribution() throws Exception {
        runTestGetRequestsGeneratorDistribution(true);
    }

    /**
     * @param batchMode Test mode.
     * @throws Exception In case of an error.
     */
    protected void runTestGetRequestsGeneratorDistribution(boolean batchMode) throws Exception {
        IgniteCache<Integer, String> cache = grid(0).createCache(replicatedCache());

        List<Integer> keys = primaryKeys(cache, PRIMARY_KEYS_NUMBER);

        for (Integer key : keys)
            cache.put(key, VAL_PREFIX + key);

        cache = grid(CLIENT_NAME).getOrCreateCache(CACHE_NAME);

        validateData(cache, keys, batchMode);

        for (int i = 0; i < gridCount(); i++) {
            IgniteEx ignite = grid(i);

            long getsCnt = ignite.cache(CACHE_NAME).localMetrics().getCacheGets();

            assertTrue(getsCnt > 0);
        }
    }

    /**
     * Tests that the 'get' operation requests are routed to node with same MAC address as at requester.
     *
     * @throws Exception In case of an error.
     * @see #runTestGetAllRequestsDistribution(UUID, boolean)
     */
    public void testGetRequestsDistribution() throws Exception {
        UUID destId = grid(0).localNode().id();

        runTestGetAllRequestsDistribution(destId, false);
    }

    /**
     * Tests that the 'getAll' operation requests are routed to node with same MAC address as at requester.
     *
     * @throws Exception In case of an error.
     * @see #runTestGetAllRequestsDistribution(UUID, boolean)
     */
    public void testGetAllRequestsDistribution() throws Exception {
        UUID destId = grid(gridCount() - 1).localNode().id();

        runTestGetAllRequestsDistribution(destId, true);
    }

    /**
     * Tests that the 'get' and 'getAll requests are routed to node with same MAC address as at requester.
     *
     * @param destId Destination Ignite instance id for requests distribution.
     * @param batchMode Test mode.
     * @throws Exception In case of an error.
     */
    protected void runTestGetAllRequestsDistribution(final UUID destId, final boolean batchMode) throws Exception {
        Map<UUID, String> macs = getClusterMacs();

        String clientMac = macs.get(grid(CLIENT_NAME).localNode().id());

        assert macs.put(destId, clientMac) != null;

        replaceMacAddresses(G.allGrids(), macs);

        IgniteCache<Integer, String> cache = grid(0).createCache(replicatedCache());

        List<Integer> keys = primaryKeys(cache, PRIMARY_KEYS_NUMBER);

        for (Integer key : keys)
            cache.put(key, VAL_PREFIX + key);

        cache = grid(CLIENT_NAME).getOrCreateCache(CACHE_NAME);

        validateData(cache, keys, batchMode);

        validateRequestsDistribution(destId);
    }

    /**
     * @param cache Ignite cache.
     * @param keys Keys to get.
     * @param batchMode Test mode.
     */
    protected void validateData(IgniteCache<Integer, String> cache, List<Integer> keys, boolean batchMode) {
        try (Transaction tx = grid(CLIENT_NAME).transactions().txStart()) {
            if (batchMode) {
                Map<Integer, String> results = cache.getAll(new TreeSet<>(keys));

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

            long getsCnt = ignite.cache(CACHE_NAME).localMetrics().getCacheGets();

            if (destId.equals(ignite.localNode().id()))
                assertEquals(PRIMARY_KEYS_NUMBER, getsCnt);
            else
                assertEquals(0L, getsCnt);
        }
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
     * @param instances Started Ignite instances.
     * @param macs Mapping MAC addresses to UUID.
     */
    private void replaceMacAddresses(List<Ignite> instances, Map<UUID, String> macs) {
        for (Ignite ignite : instances) {
            for (ClusterNode node : ignite.cluster().nodes()) {
                String mac = macs.get(node.id());

                assert mac != null;

                Map<String, Object> attrs = new HashMap<>(node.attributes());
                attrs.put(ATTR_MACS, mac);

                ((TcpDiscoveryNode)node).setAttributes(attrs);
            }
        }
    }

    /**
     * @return Cluster nodes MAC addresses.
     */
    private Map<UUID, String> getClusterMacs() {
        Map<UUID, String> macs = new HashMap<>();

        for (Ignite ignite : G.allGrids()) {
            ClusterNode node = ignite.cluster().localNode();

            String mac = node.attribute(ATTR_MACS);

            assert mac != null;

            macs.put(node.id(), mac);
        }

        return macs;
    }
}
