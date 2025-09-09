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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertActive;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 *
 */
public class IgniteClusterActivateDeactivateTestWithPersistence extends IgniteClusterActivateDeactivateTest {
    /** Indicates that additional data region configuration should be added on server node. */
    private boolean addAdditionalDataRegion;

    /** Persistent data region name. */
    private static final String ADDITIONAL_PERSISTENT_DATA_REGION = "additional-persistent-region";

    /** {@inheritDoc} */
    @Override protected boolean persistenceEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName).setAutoActivationEnabled(false);

        if (addAdditionalDataRegion) {
            DataRegionConfiguration[] originRegions = cfg.getDataStorageConfiguration().getDataRegionConfigurations();

            DataRegionConfiguration[] regions = Arrays.copyOf(originRegions, originRegions.length + 1);

            regions[originRegions.length] = new DataRegionConfiguration()
                .setName(ADDITIONAL_PERSISTENT_DATA_REGION)
                .setPersistenceEnabled(true);

            cfg.getDataStorageConfiguration().setDataRegionConfigurations(regions);
        }

        if (cfg.isClientMode())
            cfg.setDataStorageConfiguration(null);

        return cfg;
    }

    /**
     * Tests "soft" deactivation (without using the --force flag)
     * when the client node does not have the configured data storage and the cluster contains persistent caches.
     *
     * Expected behavior: the cluster should be deactivated successfully (there is no data loss).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateClusterWithPersistentCache() throws Exception {
        IgniteEx srv = startGrid(0);

        IgniteEx clientNode = startClientGrid(1);

        clientNode.cluster().state(ACTIVE);

        DataRegionConfiguration dfltDataRegion = srv
            .configuration()
            .getDataStorageConfiguration()
            .getDefaultDataRegionConfiguration();

        assertTrue(
            "It is assumed that the default data storage region is persistent.",
            dfltDataRegion.isPersistenceEnabled());

        // Create new caches that are placed into the default pesristent data region.
        clientNode.getOrCreateCache(new CacheConfiguration<>("test-client-cache-default-region-implicit"));
        clientNode.getOrCreateCache(new CacheConfiguration<>("test-client-cache-default-region-explicit")
            .setDataRegionName(dfltDataRegion.getName()));

        // Try to deactivate the cluster without the `force` flag.
        IgniteInternalFuture<?> deactivateFut = srv
            .context()
            .state()
            .changeGlobalState(INACTIVE, false, Collections.emptyList(), false);

        try {
            deactivateFut.get(10, SECONDS);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to deactivate the cluster.", e);

            fail("Failed to deactivate the cluster. [err=" + e.getMessage() + ']');
        }

        awaitPartitionMapExchange();

        // Let's check that all nodes in the cluster have the same state.
        for (Ignite node : G.allGrids()) {
            IgniteEx n = (IgniteEx)node;

            ClusterState state = n.context().state().clusterState().state();

            assertTrue(
                "Node must be in inactive state. " +
                    "[node=" + n.configuration().getIgniteInstanceName() + ", actual=" + state + ']',
                INACTIVE == state
            );
        }
    }

    /**
     * Tests "soft" deactivation (without using the --force flag)
     * when the client node does not have the configured data storage and the cluster contains in-memory caches.
     *
     * Expected behavior: deactivation should fail due to potential data loss.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateClusterWithInMemoryCaches() throws Exception {
        IgniteEx srv = startGrid(0);

        IgniteEx clientNode = startClientGrid(1);

        clientNode.cluster().state(ACTIVE);

        DataStorageConfiguration dsCfg = srv.configuration().getDataStorageConfiguration();

        DataRegionConfiguration nonPersistentRegion = Arrays.stream(dsCfg.getDataRegionConfigurations())
            .filter(region -> NO_PERSISTENCE_REGION.equals(region.getName()))
            .findFirst()
            .orElse(null);

        assertTrue(
            "It is assumed that the '" + NO_PERSISTENCE_REGION + "' data storage region exists and non-persistent.",
            nonPersistentRegion != null && !nonPersistentRegion.isPersistenceEnabled());

        // Create a new cache that is placed into non persistent data region.
        clientNode.getOrCreateCache(new CacheConfiguration<>("test-client-cache")
            .setDataRegionName(nonPersistentRegion.getName()));

        // Try to deactivate the cluster without the `force` flag.
        IgniteInternalFuture<?> deactivateFut = srv
            .context()
            .state()
            .changeGlobalState(INACTIVE, false, Collections.emptyList(), false);

        assertThrows(
            log,
            () -> deactivateFut.get(10, SECONDS),
            IgniteCheckedException.class,
            "Deactivation stopped. Deactivation clears in-memory caches (without persistence) including the system caches.");

        awaitPartitionMapExchange();

        // Let's check that all nodes in the cluster have the same state.
        for (Ignite node : G.allGrids()) {
            IgniteEx n = (IgniteEx)node;

            ClusterState state = n.context().state().clusterState().state();

            assertTrue(
                "Node must be in active state. " +
                    "[node=" + n.configuration().getIgniteInstanceName() + ", actual=" + state + ']',
                ACTIVE == state
            );
        }
    }

    /**
     * Tests "soft" deactivation (without using the --force flag)
     * when the cluster contains persistent caches and cluster nodes "support" different lists of data regions.
     *
     * Expected behavior: the cluster should be deactivated successfully (there is no data loss)..
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateClusterWithPersistentCachesAndDifferentDataRegions() throws Exception {
        IgniteEx srv = startGrid(0);

        addAdditionalDataRegion = true;

        IgniteEx srv1 = startGrid(1);

        IgniteEx clientNode = startClientGrid(2);

        clientNode.cluster().state(ACTIVE);

        DataStorageConfiguration dsCfg = srv1.configuration().getDataStorageConfiguration();

        DataRegionConfiguration persistentRegion = Arrays.stream(dsCfg.getDataRegionConfigurations())
            .filter(region -> ADDITIONAL_PERSISTENT_DATA_REGION.equals(region.getName()))
            .findFirst()
            .orElse(null);

        assertTrue(
            "It is assumed that the '" + ADDITIONAL_PERSISTENT_DATA_REGION + "' data storage region exists and persistent.",
            persistentRegion != null && persistentRegion.isPersistenceEnabled());

        final UUID srv1NodeId = srv1.localNode().id();

        // Create a new cache that is placed into persistent data region.
        srv.getOrCreateCache(new CacheConfiguration<>("test-client-cache")
            .setDataRegionName(persistentRegion.getName())
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            // This cache should only be started on srv1 node.
            .setNodeFilter(node -> node.id().equals(srv1NodeId)));

        // Try to deactivate the cluster without the `force` flag.
        IgniteInternalFuture<?> deactivateFut = srv
            .context()
            .state()
            .changeGlobalState(INACTIVE, false, Collections.emptyList(), false);

        try {
            deactivateFut.get(10, SECONDS);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to deactivate the cluster.", e);

            fail("Failed to deactivate the cluster. [err=" + e.getMessage() + ']');
        }

        awaitPartitionMapExchange();

        // Let's check that all nodes in the cluster have the same state.
        for (Ignite node : G.allGrids()) {
            IgniteEx n = (IgniteEx)node;

            ClusterState state = n.context().state().clusterState().state();

            assertTrue(
                "Node must be in inactive state. " +
                    "[node=" + n.configuration().getIgniteInstanceName() + ", actual=" + state + ']',
                INACTIVE == state
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateCachesRestore_SingleNode() throws Exception {
        activateCachesRestore(1, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateReadOnlyCachesRestore_SingleNode() throws Exception {
        activateCachesRestore(1, false, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateCachesRestore_SingleNode_WithNewCaches() throws Exception {
        activateCachesRestore(1, true, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateReadOnlyCachesRestore_SingleNode_WithNewCaches() throws Exception {
        activateCachesRestore(1, true, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateCachesRestore_5_Servers() throws Exception {
        activateCachesRestore(5, false, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateReadOnlyCachesRestore_5_Servers() throws Exception {
        activateCachesRestore(5, false, ACTIVE_READ_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateCachesRestore_5_Servers_WithNewCaches() throws Exception {
        activateCachesRestore(5, true, ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateReadOnlyCachesRestore_5_Servers_WithNewCaches() throws Exception {
        activateCachesRestore(5, true, ACTIVE_READ_ONLY);
    }

    /**
     * Test deactivation on cluster that is not yet activated.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateInactiveCluster() throws Exception {
        checkDeactivateInactiveCluster(ACTIVE);
    }

    /**
     * Test deactivation on cluster that is not yet activated.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateInactiveClusterReadOnly() throws Exception {
        checkDeactivateInactiveCluster(ACTIVE_READ_ONLY);
    }

    /** */
    private void checkDeactivateInactiveCluster(ClusterState activationMode) throws Exception {
        assertActive(activationMode);

        ccfgs = new CacheConfiguration[] {
            new CacheConfiguration<>("test_cache_1")
                .setGroupName("test_cache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL),
            new CacheConfiguration<>("test_cache_2")
                .setGroupName("test_cache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        };

        Ignite ignite = startGrids(3);

        ignite.cluster().state(activationMode);

        if (activationMode == ACTIVE) {
            ignite.cache("test_cache_1")
                .put("key1", "val1");
            ignite.cache("test_cache_2")
                .put("key1", "val1");
        }

        ignite.cluster().state(INACTIVE);

        assertEquals(INACTIVE, ignite.cluster().state());

        stopAllGrids();

        ignite = startGrids(2);

        assertEquals(INACTIVE, ignite.cluster().state());

        ignite.cluster().state(INACTIVE);

        assertEquals(INACTIVE, ignite.cluster().state());
    }

    /** */
    private Map<Integer, Integer> startGridsAndLoadData(int srvs, ClusterState activationMode) throws Exception {
        assertActive(activationMode);

        Ignite srv = startGrids(srvs);

        srv.cluster().state(ACTIVE);

        srv.createCaches(Arrays.asList(cacheConfigurations1()));

        srv.cluster().state(activationMode);

        Map<Integer, Integer> cacheData = new LinkedHashMap<>();

        for (CacheConfiguration ccfg : cacheConfigurations1()) {
            for (int i = 1; i <= 100; i++) {
                int key = -i;
                int val = i;

                if (activationMode == ACTIVE) {
                    srv.cache(ccfg.getName()).put(key, val);

                    cacheData.put(key, val);
                }
                else {
                    assertThrowsWithCause(() -> srv.cache(ccfg.getName()).put(key, val), IgniteClusterReadOnlyException.class);

                    cacheData.put(key, null);
                }
            }
        }

        return cacheData;
    }

    /**
     * @param srvs Number of server nodes.
     * @param withNewCaches If {@code true} then after restart has new caches in configuration.
     * @param activationMode Cluster activation mode.
     * @throws Exception If failed.
     */
    private void activateCachesRestore(int srvs, boolean withNewCaches, ClusterState activationMode) throws Exception {
        assertActive(activationMode);

        Map<Integer, Integer> cacheData = startGridsAndLoadData(srvs, activationMode);

        stopAllGrids();

        for (int i = 0; i < srvs; i++) {
            if (withNewCaches)
                ccfgs = cacheConfigurations2();

            startGrid(i);
        }

        Ignite srv = ignite(0);

        checkNoCaches(srvs);

        srv.cluster().state(activationMode);

        final int CACHES = withNewCaches ? 4 : 2;

        for (int i = 0; i < srvs; i++)
            checkCachesOnNode(i, CACHES);

        DataStorageConfiguration dsCfg = srv.configuration().getDataStorageConfiguration();

        checkCachesData(cacheData, dsCfg);

        checkCaches(srvs, CACHES);

        int nodes = srvs;

        startGrid(nodes++, false);

        for (int i = 0; i < nodes; i++)
            checkCachesOnNode(i, CACHES);

        checkCaches(nodes, CACHES);

        startGrid(nodes++, true);

        checkCachesOnNode(nodes - 1, CACHES, false);

        checkCaches(nodes, CACHES);

        for (int i = 0; i < nodes; i++)
            checkCachesOnNode(i, CACHES);

        checkCachesData(cacheData, dsCfg);
    }


    /** {@inheritDoc} */
    @Override protected void doFinalChecks(int startNodes, int nodesCnt) {
        for (int i = 0; i < startNodes; i++) {
            int j = i;

            assertThrowsAnyCause(log, () -> startGrid(j), IgniteSpiException.class, "not compatible");
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-7330">IGNITE-7330</a> for more information about context of the test
     */
    @Test
    public void testClientJoinsWhenActivationIsInProgress() throws Exception {
        checkClientJoinsWhenActivationIsInProgress(ACTIVE);
    }

    /** */
    @Test
    public void testClientJoinsWhenActivationReanOnlyIsInProgress() throws Exception {
        checkClientJoinsWhenActivationIsInProgress(ACTIVE_READ_ONLY);
    }

    /** */
    private void checkClientJoinsWhenActivationIsInProgress(ClusterState state) throws Exception {
        assertActive(state);

        startGridsAndLoadData(5, state);

        stopAllGrids();

        Ignite srv = startGrids(5);

        final CountDownLatch clientStartLatch = new CountDownLatch(1);

        IgniteInternalFuture clStartFut = GridTestUtils.runAsync(
            () -> {
                try {
                    clientStartLatch.await();

                    Thread.sleep(10);

                    Ignite cl = startClientGrid("client0");

                    IgniteCache<Object, Object> atomicCache = cl.cache(CACHE_NAME_PREFIX + '0');
                    IgniteCache<Object, Object> txCache = cl.cache(CACHE_NAME_PREFIX + '1');

                    assertEquals(state == ACTIVE ? 100 : 0, atomicCache.size());
                    assertEquals(state == ACTIVE ? 100 : 0, txCache.size());
                }
                catch (Exception e) {
                    log.error("Error occurred", e);

                    fail("Error occurred in client thread. Msg: " + e.getMessage());
                }
            },
            "client-starter-thread"
        );

        clientStartLatch.countDown();
        srv.cluster().state(state);

        clStartFut.get();
    }

    /**
     * Checks that persistent caches are present with actual data and volatile caches are missing.
     *
     * @param cacheData Cache data.
     * @param dsCfg DataStorageConfiguration.
     */
    private void checkCachesData(Map<Integer, Integer> cacheData, DataStorageConfiguration dsCfg) {
        for (CacheConfiguration ccfg : cacheConfigurations1()) {
            if (CU.isPersistentCache(ccfg, dsCfg))
                checkCacheData(cacheData, ccfg.getName());
            else {
                for (Ignite node : G.allGrids())
                    assertTrue(node.cache(ccfg.getName()) == null || node.cache(ccfg.getName()).size() == 0);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateCacheRestoreConfigurationConflict() throws Exception {
        checkActivateCacheRestoreConfigurationConflict(ACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateReadOnlyCacheRestoreConfigurationConflict() throws Exception {
        checkActivateCacheRestoreConfigurationConflict(ACTIVE_READ_ONLY);
    }

    /** */
    private void checkActivateCacheRestoreConfigurationConflict(ClusterState state) throws Exception {
        assertActive(state);

        final int SRVS = 3;

        Ignite srv = startGrids(SRVS);

        srv.cluster().state(ACTIVE);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        srv.createCache(ccfg);

        srv.cluster().state(state);

        stopAllGrids();

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME + 1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ccfg.setGroupName(DEFAULT_CACHE_NAME);

        ccfgs = new CacheConfiguration[] {ccfg};

        assertThrowsAnyCause(log, () -> startGrids(SRVS), IgniteCheckedException.class, "Failed to start configured cache.");
    }

    /**
     * Test that after deactivation during eviction and rebalance and activation again after
     * all data in cache is consistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateDuringEvictionAndRebalance() throws Exception {
        IgniteEx srv = startGrids(3);

        srv.cluster().state(ACTIVE);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setIndexedTypes(Integer.class, Integer.class)
            .setAffinity(new RendezvousAffinityFunction(false, 64))
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        IgniteCache cache = srv.createCache(ccfg);

        // High number of keys triggers long partition eviction.
        final int keysCnt = 100_000;

        try (IgniteDataStreamer ds = srv.dataStreamer(DEFAULT_CACHE_NAME)) {
            log.info("Writing initial data...");

            ds.allowOverwrite(true);
            for (int k = 1; k <= keysCnt; k++) {
                ds.addData(k, k);

                if (k % 50_000 == 0)
                    log.info("Written " + k + " entities.");
            }

            log.info("Writing initial data finished.");
        }

        AtomicInteger keyCounter = new AtomicInteger(keysCnt);
        AtomicBoolean stop = new AtomicBoolean(false);

        Set<Integer> addedKeys = new GridConcurrentHashSet<>();

        IgniteInternalFuture cacheLoadFut = GridTestUtils.runMultiThreadedAsync(
            () -> {
                while (!stop.get()) {
                    int key = keyCounter.incrementAndGet();
                    try {
                        cache.put(key, key);

                        addedKeys.add(key);

                        Thread.sleep(10);
                    }
                    catch (Exception ignored) {
                        // Ignore.
                    }
                }
            },
            2,
            "cache-load"
        );

        stopGrid(2);

        // Wait for some data.
        Thread.sleep(3000);

        startGrid(2);

        log.info("Stop load...");

        stop.set(true);

        cacheLoadFut.get();

        // Deactivate and activate again.
        srv.cluster().state(INACTIVE);

        srv.cluster().state(ACTIVE);

        awaitPartitionMapExchange();

        log.info("Checking data...");

        for (Ignite ignite : G.allGrids()) {
            IgniteCache cache1 = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

            for (int k = 1; k <= keysCnt; k++) {
                Object val = cache1.get(k);

                Assert.assertNotNull("node=" + ignite.name() + ", key=" + k, val);

                Assert.assertTrue("node=" + ignite.name() + ", key=" + k + ", val=" + val, (int)val == k);
            }

            for (int k : addedKeys) {
                Object val = cache1.get(k);

                Assert.assertNotNull("node=" + ignite.name() + ", key=" + k, val);

                Assert.assertTrue("node=" + ignite.name() + ", key=" + k + ", val=" + val, (int)val == k);
            }
        }
    }
}
