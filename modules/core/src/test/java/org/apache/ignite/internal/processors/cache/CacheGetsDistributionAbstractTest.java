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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests of replicated cache's 'get' requests distribution.
 */
public abstract class CacheGetsDistributionAbstractTest extends GridCommonAbstractTest {
    /** Client nodes instance's name. */
    private static final String CLIENT_NAME = "client";

    /** Value prefix. */
    private static final String VAL_PREFIX = "val";

    /** */
    private static final int PRIMARY_KEYS_NUMBER = 1_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        assert gridCount() >= 1 : "At least one grid must be started";

        startGridsMultiThreaded(gridCount());

        startClientGrid(getConfiguration(CLIENT_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteCache cache = ignite(0).cache(DEFAULT_CACHE_NAME);

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

        TransactionConfiguration txCfg = new TransactionConfiguration()
            .setDefaultTxIsolation(transactionIsolation())
            .setDefaultTxConcurrency(transactionConcurrency());

        cfg.setTransactionConfiguration(txCfg);

        return cfg;
    }

    /**
     * @return Grids count to start.
     */
    protected int gridCount() {
        return 4;
    }

    /**
     * @return Cache configuration.
     */
    protected <K, V> CacheConfiguration<K, V> cacheConfiguration() {
        CacheConfiguration<K, V> ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(cacheMode());
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setReadFromBackup(true);
        ccfg.setStatisticsEnabled(true);

        if (cacheMode() == CacheMode.PARTITIONED)
            ccfg.setBackups(backupsCount());

        return ccfg;
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

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
     * @return Backups count.
     */
    protected int backupsCount() {
        return gridCount() - 1;
    }

    /**
     * Test 'get' operations requests generator distribution.
     *
     * @throws Exception In case of an error.
     * @see #runTestBalancingDistribution(boolean)
     */
    @Test
    public void testGetRequestsGeneratorDistribution() throws Exception {
        runTestBalancingDistribution(false);
    }

    /**
     * Test 'getAll' operations requests generator distribution.
     *
     * @throws Exception In case of an error.
     * @see #runTestBalancingDistribution(boolean)
     */
    @Test
    public void testGetAllRequestsGeneratorDistribution() throws Exception {
        runTestBalancingDistribution(true);
    }

    /**
     * @param batchMode Whenever 'get' or 'getAll' operations are used in the test.
     * @throws Exception In case of an error.
     */
    protected void runTestBalancingDistribution(boolean batchMode) throws Exception {
        IgniteCache<Integer, String> cache = grid(0).createCache(cacheConfiguration());

        List<Integer> keys = primaryKeys(cache, PRIMARY_KEYS_NUMBER);

        for (Integer key : keys)
            cache.put(key, VAL_PREFIX + key);

        IgniteCache<Integer, String> clientCache = grid(CLIENT_NAME).cache(DEFAULT_CACHE_NAME)
            .withAllowAtomicOpsInTx();

        assertTrue(GridTestUtils.waitForCondition(
            new GridAbsPredicate() {
                int batchSize = 10;
                int idx = 0;

                @Override public boolean apply() {
                    if (idx >= PRIMARY_KEYS_NUMBER)
                        idx = 0;

                    try (Transaction tx = grid(CLIENT_NAME).transactions().txStart()) {
                        if (batchMode) {
                            Set<Integer> keys0 = new TreeSet<>();

                            for (int i = idx; i < idx + batchSize && i < PRIMARY_KEYS_NUMBER; i++)
                                keys0.add(keys.get(i));

                            idx += batchSize;

                            Map<Integer, String> results = clientCache.getAll(keys0);

                            for (Map.Entry<Integer, String> entry : results.entrySet())
                                assertEquals(VAL_PREFIX + entry.getKey(), entry.getValue());
                        }
                        else {
                            for (int i = idx; i < idx + gridCount() && i < PRIMARY_KEYS_NUMBER; i++) {
                                Integer key = keys.get(i);

                                assertEquals(VAL_PREFIX + key, clientCache.get(key));
                            }

                            idx += gridCount();
                        }

                        tx.commit();
                    }

                    for (int i = 0; i < gridCount(); i++) {
                        IgniteEx ignite = grid(i);

                        long getsCnt = ignite.cache(DEFAULT_CACHE_NAME).localMetrics().getCacheGets();

                        if (getsCnt == 0)
                            return false;
                    }

                    return true;
                }
            },
            getTestTimeout())
        );
    }

    /**
     * Tests that the 'get' operation requests are routed to node with same MAC address as at requester.
     *
     * @throws Exception In case of an error.
     * @see #runTestSameHostDistribution(UUID, boolean)
     */
    @Test
    public void testGetRequestsDistribution() throws Exception {
        UUID destId = grid(0).localNode().id();

        runTestSameHostDistribution(destId, false);
    }

    /**
     * Tests that the 'getAll' operation requests are routed to node with same MAC address as at requester.
     *
     * @throws Exception In case of an error.
     * @see #runTestSameHostDistribution(UUID, boolean)
     */
    @Test
    public void testGetAllRequestsDistribution() throws Exception {
        UUID destId = grid(gridCount() - 1).localNode().id();

        runTestSameHostDistribution(destId, true);
    }

    /**
     * Tests that the 'get' and 'getAll' requests are routed to node with same MAC address as at requester.
     *
     * @param destId Destination Ignite instance id for requests distribution.
     * @param batchMode Test mode.
     * @throws Exception In case of an error.
     */
    protected void runTestSameHostDistribution(final UUID destId, final boolean batchMode) throws Exception {
        Map<UUID, String> macs = getClusterMacs();

        String clientMac = macs.get(grid(CLIENT_NAME).localNode().id());

        macs.put(destId, clientMac);

        replaceMacAddresses(G.allGrids(), macs);

        IgniteCache<Integer, String> cache = grid(0).createCache(cacheConfiguration());

        List<Integer> keys = primaryKeys(cache, PRIMARY_KEYS_NUMBER);

        for (Integer key : keys)
            cache.put(key, VAL_PREFIX + key);

        IgniteCache<Integer, String> clientCache = grid(CLIENT_NAME).cache(DEFAULT_CACHE_NAME)
            .withAllowAtomicOpsInTx();

        try (Transaction tx = grid(CLIENT_NAME).transactions().txStart()) {
            if (batchMode) {
                Map<Integer, String> results = clientCache.getAll(new TreeSet<>(keys));

                for (Map.Entry<Integer, String> entry : results.entrySet())
                    assertEquals(VAL_PREFIX + entry.getKey(), entry.getValue());
            }
            else {
                for (Integer key : keys)
                    assertEquals(VAL_PREFIX + key, clientCache.get(key));
            }

            tx.commit();
        }

        for (int i = 0; i < gridCount(); i++) {
            IgniteEx ignite = grid(i);

            long getsCnt = ignite.cache(DEFAULT_CACHE_NAME).localMetrics().getCacheGets();

            if (destId.equals(ignite.localNode().id()))
                assertEquals(PRIMARY_KEYS_NUMBER, getsCnt);
            else
                assertEquals(0L, getsCnt);
        }
    }

    /**
     * @param instances Started Ignite instances.
     * @param macs Mapping MAC addresses to UUID.
     */
    private void replaceMacAddresses(List<Ignite> instances, Map<UUID, String> macs) {
        for (Ignite ignite : instances) {
            for (ClusterNode node : ignite.cluster().nodes()) {
                String mac = macs.get(node.id());

                assertNotNull(mac);

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
