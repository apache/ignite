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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestDelayingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCachePartitionLossPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_EXCLUDE_ON_CRD = "cache-exclude-crd";

    /** */
    private static final String[] CACHE_NAMES = {DEFAULT_CACHE_NAME, CACHE_EXCLUDE_ON_CRD};

    /** */
    private boolean client;

    /** */
    private PartitionLossPolicy partLossPlc;

    /** */
    private int backups;

    /** */
    private final AtomicBoolean delayPartExchange = new AtomicBoolean(false);

    /** */
    private final TopologyChanger killSingleNode = new TopologyChanger(
        false, singletonList(3), asList(0, 1, 2, 4), 0);

    /** */
    private boolean isPersistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setCommunicationSpi(new TestDelayingCommunicationSpi() {
            /** {@inheritDoc} */
            @Override protected boolean delayMessage(Message msg, GridIoMessage ioMsg) {
                return delayPartExchange.get() &&
                    (msg instanceof GridDhtPartitionsFullMessage || msg instanceof GridDhtPartitionsAbstractMessage);
            }

        });

        cfg.setClientMode(client);

        CacheConfiguration[] ccfgs;

        if (gridName.equals(getTestIgniteInstanceName(0)))
            ccfgs = new CacheConfiguration[]{cacheConfiguration(DEFAULT_CACHE_NAME)};
        else
            ccfgs = new CacheConfiguration[]{cacheConfiguration(DEFAULT_CACHE_NAME), cacheConfiguration(CACHE_EXCLUDE_ON_CRD)};

        cfg.setCacheConfiguration(ccfgs);

        cfg.setConsistentId(gridName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(isPersistenceEnabled)
                ));

        return cfg;
    }

    /**
     * @param node Node.
     * @param cacheName Cache name.
     * @return {@code True} if do access cache on given node.
     */
    private boolean skipCache(Ignite node, String cacheName) {
        return cacheName.equals(CACHE_EXCLUDE_ON_CRD) && getTestIgniteInstanceName(0).equals(node.name());
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration(String cacheName) {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(cacheName);

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(backups);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setPartitionLossPolicy(partLossPlc);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        if (CACHE_EXCLUDE_ON_CRD.equals(cacheName))
            cacheCfg.setNodeFilter(new TestCacheNodeExcludingFilter(getTestIgniteInstanceName(0)));

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        delayPartExchange.set(false);

        partLossPlc = PartitionLossPolicy.IGNORE;

        backups = 0;

        isPersistenceEnabled = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadOnlySafe() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_SAFE;

        checkLostPartition(false, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadOnlySafeWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(false, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadOnlyAll() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_ALL;

        checkLostPartition(false, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadOnlyAllWithPersistence() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-10041");

        partLossPlc = PartitionLossPolicy.READ_ONLY_ALL;

        isPersistenceEnabled = true;

        checkLostPartition(false, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafe() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteAll() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_ALL;

        checkLostPartition(true, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteAllWithPersistence() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-10041");

        partLossPlc = PartitionLossPolicy.READ_WRITE_ALL;

        isPersistenceEnabled = true;

        checkLostPartition(true, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeAfterKillTwoNodes() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeAfterKillTwoNodesWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeAfterKillTwoNodesWithDelay() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 20));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeAfterKillTwoNodesWithDelayWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 20));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithBackupsAfterKillThreeNodes() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 2, 1), asList(0, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithBackupsAfterKillThreeNodesWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 2, 1), asList(0, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeAfterKillCrd() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeAfterKillCrdWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithBackups() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithBackupsWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithBackupsAfterKillCrd() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadWriteSafeWithBackupsAfterKillCrdWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    public void testIgnore() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5078");

        partLossPlc = PartitionLossPolicy.IGNORE;

        checkIgnore(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testIgnoreWithPersistence() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5078");

        fail("https://issues.apache.org/jira/browse/IGNITE-10041");

        partLossPlc = PartitionLossPolicy.IGNORE;

        isPersistenceEnabled = true;

        checkIgnore(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    public void testIgnoreKillThreeNodes() throws Exception {
        partLossPlc = PartitionLossPolicy.IGNORE;

        // TODO aliveNodes should include node 4, but it fails due to https://issues.apache.org/jira/browse/IGNITE-5078.
        // TODO need to add 4 to the aliveNodes after IGNITE-5078 is fixed.
        // TopologyChanger onlyCrdIsAlive = new TopologyChanger(false, Arrays.asList(1, 2, 3), Arrays.asList(0, 4), 0);
        TopologyChanger onlyCrdIsAlive = new TopologyChanger(false, asList(1, 2, 3), singletonList(0), 0);

        checkIgnore(onlyCrdIsAlive);
    }

    /**
     * @throws Exception if failed.
     */
    public void testIgnoreKillThreeNodesWithPersistence() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-10041");

        partLossPlc = PartitionLossPolicy.IGNORE;

        isPersistenceEnabled = true;

        // TODO aliveNodes should include node 4, but it fails due to https://issues.apache.org/jira/browse/IGNITE-5078.
        // TODO need to add 4 to the aliveNodes after IGNITE-5078 is fixed.
        // TopologyChanger onlyCrdIsAlive = new TopologyChanger(false, Arrays.asList(1, 2, 3), Arrays.asList(0, 4), 0);
        TopologyChanger onlyCrdIsAlive = new TopologyChanger(false, asList(1, 2, 3), singletonList(0), 0);

        checkIgnore(onlyCrdIsAlive);
    }

    /**
     * @param topChanger topology changer.
     * @throws Exception if failed.
     */
    private void checkIgnore(TopologyChanger topChanger) throws Exception {
        topChanger.changeTopology();

        for (String cacheName : CACHE_NAMES) {
            for (Ignite ig : G.allGrids()) {
                if (skipCache(ig, cacheName))
                    continue;

                IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

                Collection<Integer> lost = cache.lostPartitions();

                assertTrue("[grid=" + ig.name() + ", lost=" + lost.toString() + ']', lost.isEmpty());

                int parts = ig.affinity(cacheName).partitions();

                for (int i = 0; i < parts; i++) {
                    if (cacheName.equals(CACHE_EXCLUDE_ON_CRD) && ig.affinity(cacheName).mapPartitionToNode(i) == null)
                        continue;

                    cache.get(i);

                    cache.put(i, i);
                }
            }
        }
    }

    /**
     * @param canWrite {@code True} if writes are allowed.
     * @param safe {@code True} if lost partition should trigger exception.
     * @param topChanger topology changer.
     * @throws Exception if failed.
     */
    private void checkLostPartition(boolean canWrite, boolean safe, TopologyChanger topChanger) throws Exception {
        assert partLossPlc != null;

        Map<String, List<Integer>> lostPartsMap = topChanger.changeTopology();

        // Wait for all grids (servers and client) have same topology version
        // to make sure that all nodes received map with lost partition.
        boolean success = GridTestUtils.waitForCondition(() -> {
            AffinityTopologyVersion last = null;
            for (Ignite ig : G.allGrids()) {
                AffinityTopologyVersion ver = ((IgniteEx)ig).context().cache().context().exchange().readyAffinityVersion();

                if (last != null && !last.equals(ver))
                    return false;

                last = ver;
            }

            return true;
        }, 10000);

        assertTrue("Failed to wait for new topology", success);

        for (Ignite ig : G.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                if (skipCache(ig, cacheName))
                    continue;

                info("Checking node [cache=" + cacheName + ", node=" + ig.cluster().localNode().id() + ']');

                IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

                verifyLostPartitions(ig, cacheName, lostPartsMap.get(cacheName));

                verifyCacheOps(cacheName, canWrite, safe, ig);

                validateQuery(safe, ig, cacheName);

                if (cacheName.equals(CACHE_EXCLUDE_ON_CRD) && lostPartsMap.get(cacheName).size() == ig.affinity(cacheName).partitions())
                    continue;

                // TODO withPartitionRecover doesn't work with BLT - https://issues.apache.org/jira/browse/IGNITE-10041.
                if (!isPersistenceEnabled) {
                    // Check we can read and write to lost partition in recovery mode.
                    IgniteCache<Integer, Integer> recoverCache = cache.withPartitionRecover();

                    for (int lostPart : recoverCache.lostPartitions()) {
                        recoverCache.get(lostPart);
                        recoverCache.put(lostPart, lostPart);
                    }

                    // Check that writing in recover mode does not clear partition state.
                    verifyLostPartitions(ig, cacheName, lostPartsMap.get(cacheName));

                    verifyCacheOps(cacheName, canWrite, safe, ig);

                    validateQuery(safe, ig, cacheName);
                }
            }
        }

        checkNewNode(true, canWrite, safe);
        checkNewNode(false, canWrite, safe);

        // Bring all nodes back.
        for (int i : topChanger.killNodes) {
            IgniteEx grd = startGrid(i);

            info("Newly started node: " + grd.cluster().localNode().id());

            for (String cacheName : CACHE_NAMES) {
                verifyLostPartitions(grd, cacheName, lostPartsMap.get(cacheName));

                // Check that partition state does not change after we start each node.
                for (Ignite ig : G.allGrids()) {
                    if (skipCache(ig, cacheName))
                        continue;

                    verifyLostPartitions(ig, cacheName, lostPartsMap.get(cacheName));

                    verifyCacheOps(cacheName, canWrite, safe, ig);

                    // TODO Query effectively waits for rebalance due to https://issues.apache.org/jira/browse/IGNITE-10057
                    // TODO and after resetLostPartition there is another OWNING copy in the cluster due to https://issues.apache.org/jira/browse/IGNITE-10058.
                    // TODO Uncomment after https://issues.apache.org/jira/browse/IGNITE-10058 is fixed.
//                    validateQuery(safe, ig);
                }
            }
        }

        // Make sure cache did not really start on coordinator,
        if (topChanger.aliveNodes.contains(0))
            assertNull(((IgniteEx)ignite(0)).context().cache().cacheGroup(CU.cacheId(CACHE_EXCLUDE_ON_CRD)));

        ignite(4).resetLostPartitions(Arrays.asList(CACHE_NAMES));

        awaitPartitionMapExchange(true, true, null);

        for (Ignite ig : G.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                if (skipCache(ig, cacheName))
                    continue;

                IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

                assertTrue(cache.lostPartitions().isEmpty());

                int parts = ig.affinity(cacheName).partitions();

                for (int i = 0; i < parts; i++) {
                    cache.get(i);

                    cache.put(i, i);
                }

                for (int i = 0; i < parts; i++) {
                    checkQueryPasses(ig, false, cacheName, i);

                    if (shouldExecuteLocalQuery(ig, cacheName, i))
                        checkQueryPasses(ig, true, cacheName, i);

                }

                checkQueryPasses(ig, false, cacheName);
            }
        }

        // Make sure cache did not really start on coordinator,
        if (topChanger.aliveNodes.contains(0))
            assertNull(((IgniteEx)ignite(0)).context().cache().cacheGroup(CU.cacheId(CACHE_EXCLUDE_ON_CRD)));

        // Start new node after lost partitions reset.
        startGrid(5);

        for (Ignite ig : G.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                if (skipCache(ig, cacheName))
                    continue;

                IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

                assertTrue(cache.lostPartitions().isEmpty());
            }
        }
    }

    /**
     * @param client Client flag.
     * @param canWrite Can write flag.
     * @param safe Safe flag.
     * @throws Exception If failed to start a new node.
     */
    private void checkNewNode(
        boolean client,
        boolean canWrite,
        boolean safe
    ) throws Exception {
        this.client = client;

        try {
            IgniteEx cl = startGrid("newNode");

            for (String cacheName : CACHE_NAMES) {
                CacheGroupContext grpCtx = cl.context().cache().cacheGroup(CU.cacheId(cacheName));

                assertTrue(grpCtx.needsRecovery());

                verifyCacheOps(cacheName, canWrite, safe, cl);

                validateQuery(safe, cl, cacheName);
            }
        }
        finally {
            stopGrid("newNode", false);

            this.client = false;
        }
    }

    /**
     * @param node Node.
     * @param cacheName Cache name.
     * @param lostParts Lost partition IDs.
     */
    private void verifyLostPartitions(Ignite node, String cacheName, List<Integer> lostParts) {
        IgniteCache<Integer, Integer> cache = node.cache(cacheName);

        Set<Integer> actualSortedLostParts = new TreeSet<>(cache.lostPartitions());
        Set<Integer> expSortedLostParts = new TreeSet<>(lostParts);

        assertEqualsCollections(expSortedLostParts, actualSortedLostParts);
    }

    /**
     * @param canWrite {@code True} if writes are allowed.
     * @param safe {@code True} if lost partition should trigger exception.
     * @param ig Ignite instance.
     */
    private void verifyCacheOps(String cacheName, boolean canWrite, boolean safe, Ignite ig) {
        IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

        int parts = ig.affinity(cacheName).partitions();

        // Check read.
        for (int i = 0; i < parts; i++) {
            try {
                Integer actual = cache.get(i);

                if (cache.lostPartitions().contains(i)) {
                    if (safe)
                        fail("Reading from a lost partition should have failed: " + i + " " + ig.name());
                    // else we could have read anything.
                }
                else
                    assertEquals((Integer)i, actual);
            }
            catch (CacheException e) {
                assertTrue("Read exception should only be triggered in safe mode: " + e, safe);
                assertTrue("Read exception should only be triggered for a lost partition " +
                    "[ex=" + e + ", part=" + i + ']', cache.lostPartitions().contains(i));
            }
        }

        // Check write.
        for (int i = 0; i < parts; i++) {
            try {
                cache.put(i, i);

                assertTrue("Write in read-only mode should be forbidden: " + i, canWrite);

                if (cache.lostPartitions().contains(i))
                    assertFalse("Writing to a lost partition should have failed: " + i, safe);
            }
            catch (CacheException e) {
                if (canWrite) {
                    assertTrue("Write exception should only be triggered in safe mode: " + e, safe);
                    assertTrue("Write exception should only be triggered for a lost partition: " + e,
                        cache.lostPartitions().contains(i));
                }
                // else expected exception regardless of partition.
            }
        }
    }

    /**
     * @param nodes List of nodes to find partition.
     * @return List of partitions that aren't primary or backup for specified nodes.
     */
    private List<Integer> noPrimaryOrBackupPartition(String cacheName, List<Integer> nodes) {
        Affinity<Object> aff = ignite(4).affinity(cacheName);

        List<Integer> parts = new ArrayList<>();

        Integer part;

        for (int i = 0; i < aff.partitions(); i++) {
            part = i;

            for (Integer id : nodes) {
                if (aff.isPrimaryOrBackup(grid(id).cluster().localNode(), i)) {
                    part = null;

                    break;
                }
            }

            if (part != null)
                parts.add(i);
        }

        return parts;
    }

    /**
     * Validate query execution on a node.
     *
     * @param safe Safe flag.
     * @param node Node.
     */
    private void validateQuery(boolean safe, Ignite node, String cacheName) {
        // Get node lost and remaining partitions.
        IgniteCache<?, ?> cache = node.cache(cacheName);

        Collection<Integer> lostParts = cache.lostPartitions();

        int part = cache.lostPartitions().stream().findFirst().orElseThrow(AssertionFailedError::new);

        Integer remainingPart = null;

        for (int i = 0; i < node.affinity(cacheName).partitions(); i++) {
            if (lostParts.contains(i))
                continue;

            remainingPart = i;

            break;
        }

        if (remainingPart == null && cacheName.equals(CACHE_EXCLUDE_ON_CRD))
            return;

        assertNotNull("Failed to find a partition that isn't lost for cache: " + cacheName, remainingPart);

        // 1. Check query against all partitions.
        validateQuery0(safe, node, cacheName);

        // 2. Check query against LOST partition.
        validateQuery0(safe, node, cacheName, part);

        // 3. Check query on remaining partition.
        checkQueryPasses(node, false, cacheName, remainingPart);

        if (shouldExecuteLocalQuery(node, cacheName, remainingPart))
            checkQueryPasses(node, true, cacheName, remainingPart);

        // 4. Check query over two partitions - normal and LOST.
        validateQuery0(safe, node, cacheName, part, remainingPart);
    }

    /**
     * Query validation routine.
     *
     * @param safe Safe flag.
     * @param node Node.
     * @param cacheName Cache name.
     * @param parts Partitions.
     */
    private void validateQuery0(boolean safe, Ignite node, String cacheName, int... parts) {
        if (safe)
            checkQueryFails(node, false, cacheName, parts);
        else
            checkQueryPasses(node, false, cacheName, parts);

        if (shouldExecuteLocalQuery(node, cacheName, parts)) {
            if (safe)
                checkQueryFails(node, true, cacheName, parts);
            else
                checkQueryPasses(node, true, cacheName, parts);
        }
    }

    /**
     * @return true if the given node is primary for all given partitions.
     */
    private boolean shouldExecuteLocalQuery(Ignite node, String cacheName, int... parts) {
        if (parts == null || parts.length == 0)
            return false;

        int numOfPrimaryParts = 0;

        for (int nodePrimaryPart : node.affinity(cacheName).primaryPartitions(node.cluster().localNode())) {
            for (int part : parts) {
                if (part == nodePrimaryPart)
                    numOfPrimaryParts++;
            }
        }

        return numOfPrimaryParts == parts.length;
    }

    /**
     * @param node Node.
     * @param loc Local flag.
     * @param parts Partitions.
     */
    protected void checkQueryPasses(Ignite node, boolean loc, String cacheName, int... parts) {
        // Scan queries don't support multiple partitions.
        if (parts != null && parts.length > 1)
            return;

        // TODO Local scan queries fail in non-safe modes - https://issues.apache.org/jira/browse/IGNITE-10059.
        if (loc)
            return;

        IgniteCache cache = node.cache(cacheName);

        ScanQuery qry = new ScanQuery();

        if (parts != null && parts.length > 0)
            qry.setPartition(parts[0]);

        if (loc)
            qry.setLocal(true);

        cache.query(qry).getAll();
    }

    /**
     * @param node Node.
     * @param loc Local flag.
     * @param parts Partitions.
     */
    protected void checkQueryFails(Ignite node, boolean loc, String cacheName, int... parts) {
        // TODO Scan queries never fail due to partition loss - https://issues.apache.org/jira/browse/IGNITE-9902.
        // TODO Need to add an actual check after https://issues.apache.org/jira/browse/IGNITE-9902 is fixed.
        // No-op.
    }

    /** */
    private class TopologyChanger {
        /** Flag to delay partition exchange */
        private boolean delayExchange;

        /** List of nodes to kill */
        private List<Integer> killNodes;

        /** List of nodes to be alive */
        private List<Integer> aliveNodes;

        /** Delay between node stops */
        private long stopDelay;

        /**
         * @param delayExchange Flag for delay partition exchange.
         * @param killNodes List of nodes to kill.
         * @param aliveNodes List of nodes to be alive.
         * @param stopDelay Delay between stopping nodes.
         */
        private TopologyChanger(boolean delayExchange, List<Integer> killNodes, List<Integer> aliveNodes,
            long stopDelay) {
            this.delayExchange = delayExchange;
            this.killNodes = killNodes;
            this.aliveNodes = aliveNodes;
            this.stopDelay = stopDelay;
        }

        /**
         * @return Lost partition ID.
         * @throws Exception If failed.
         */
        private Map<String, List<Integer>> changeTopology() throws Exception {
            startGrids(4);

            if (isPersistenceEnabled)
                grid(0).cluster().active(true);

            for (String cacheName : CACHE_NAMES) {
                Affinity<Object> aff = ignite(1).affinity(cacheName);

                for (int i = 0; i < aff.partitions(); i++)
                    ignite(1).cache(cacheName).put(i, i);
            }

            client = true;

            startGrid(4);

            client = false;

            for (int i = 0; i < 5; i++)
                info(">>> Node [idx=" + i + ", nodeId=" + ignite(i).cluster().localNode().id() + ']');

            awaitPartitionMapExchange();

            Map<String, List<Integer>> lostParts = new HashMap<>();

            Map<String, List<Map<Integer, Semaphore>>> lostMaps = new HashMap<>();

            for (String cacheName : CACHE_NAMES) {
                final List<Integer> parts = noPrimaryOrBackupPartition(cacheName, aliveNodes);

                if (parts.isEmpty())
                    throw new IllegalStateException("No partition on nodes: " + killNodes);

                lostParts.put(cacheName, parts);

                final List<Map<Integer, Semaphore>> lostMap = new ArrayList<>();

                lostMaps.put(cacheName, lostMap);

                for (int i : aliveNodes) {
                    Ignite node = grid(i);

                    if (skipCache(node, cacheName))
                        continue;

                    HashMap<Integer, Semaphore> semaphoreMap = new HashMap<>();

                    for (Integer part : parts)
                        semaphoreMap.put(part, new Semaphore(0));

                    lostMap.add(semaphoreMap);

                    node.events().localListen(new P1<Event>() {
                        @Override public boolean apply(Event evt) {
                            assert evt.type() == EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

                            CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent)evt;

                            if (F.eq(cacheName, cacheEvt.cacheName())) {
                                if (semaphoreMap.containsKey(cacheEvt.partition()))
                                    semaphoreMap.get(cacheEvt.partition()).release();
                            }

                            return true;
                        }
                    }, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);
                }
            }

            if (delayExchange)
                delayPartExchange.set(true);

            ExecutorService executor = Executors.newFixedThreadPool(killNodes.size());

            for (Integer node : killNodes) {
                executor.submit(new Runnable() {
                    @Override public void run() {
                        grid(node).close();
                    }
                });

                Thread.sleep(stopDelay);
            }

            executor.shutdown();

            delayPartExchange.set(false);

            Thread.sleep(5_000L);

            for (String cacheName : CACHE_NAMES) {
                List<Map<Integer, Semaphore>> lostMap = lostMaps.get(cacheName);

                for (Map<Integer, Semaphore> map : lostMap) {
                    for (Map.Entry<Integer, Semaphore> entry : map.entrySet())
                        assertTrue("Failed to wait for partition LOST event [cache=" + cacheName + ", part=" + entry.getKey() + ']',
                            entry.getValue().tryAcquire(1));
                }

                for (Map<Integer, Semaphore> map : lostMap) {
                    for (Map.Entry<Integer, Semaphore> entry : map.entrySet())
                        assertFalse("Partition LOST event raised twice for partition [cache=" + cacheName + ", part=" + entry.getKey() + ']',
                            entry.getValue().tryAcquire(1));
                }
            }

            return lostParts;
        }
    }
}
