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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.junit.Assume.assumeFalse;

/**
 *
 */
@RunWith(Parameterized.class)
public class IgniteCachePartitionLossPolicySelfTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[]{TRANSACTIONAL});

        if (!MvccFeatureChecker.forcedMvcc())
            params.add(new Object[]{ATOMIC});

        return params;
    }

    /** */
    private static boolean client;

    /** */
    private static PartitionLossPolicy partLossPlc;

    /** */
    private static int backups;

    /** */
    private static final AtomicBoolean delayPartExchange = new AtomicBoolean(false);

    /** */
    private final TopologyChanger killSingleNode = new TopologyChanger(
        false, singletonList(3), asList(0, 1, 2, 4), 0);

    /** */
    private static boolean isPersistenceEnabled;

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicity;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new TestDelayingCommunicationSpi() {
            /** {@inheritDoc} */
            @Override protected boolean delayMessage(Message msg, GridIoMessage ioMsg) {
                return delayPartExchange.get() &&
                    (msg instanceof GridDhtPartitionsFullMessage || msg instanceof GridDhtPartitionsAbstractMessage);
            }

        });

        cfg.setClientMode(client);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setConsistentId(gridName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(isPersistenceEnabled)
                ));

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(backups);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setPartitionLossPolicy(partLossPlc);
        cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        cacheCfg.setAtomicityMode(atomicity);

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
    @Test
    public void testReadOnlySafe() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_ONLY_SAFE;

        checkLostPartition(false, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadOnlySafeWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(false, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadOnlyAll() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_ONLY_ALL;

        checkLostPartition(false, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10041")
    @Test
    public void testReadOnlyAllWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_ONLY_ALL;

        isPersistenceEnabled = true;

        checkLostPartition(false, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafe() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteAll() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_WRITE_ALL;

        checkLostPartition(true, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10041")
    @Test
    public void testReadWriteAllWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_ALL;

        isPersistenceEnabled = true;

        checkLostPartition(true, false, killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillTwoNodes() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillTwoNodesWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillTwoNodesWithDelay() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 20));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillTwoNodesWithDelayWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(false, asList(3, 2), asList(0, 1, 4), 20));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackupsAfterKillThreeNodes() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 2, 1), asList(0, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackupsAfterKillThreeNodesWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 2, 1), asList(0, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillCrd() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeAfterKillCrdWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackups() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackupsWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 2), asList(0, 1, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackupsAfterKillCrd() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11107", MvccFeatureChecker.forcedMvcc());

        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadWriteSafeWithBackupsAfterKillCrdWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.READ_WRITE_SAFE;

        backups = 1;

        isPersistenceEnabled = true;

        checkLostPartition(true, true, new TopologyChanger(true, asList(3, 0), asList(1, 2, 4), 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-5078")
    @Test
    public void testIgnore() throws Exception {
        partLossPlc = PartitionLossPolicy.IGNORE;

        checkIgnore(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-5078,https://issues.apache.org/jira/browse/IGNITE-10041")
    @Test
    public void testIgnoreWithPersistence() throws Exception {
        partLossPlc = PartitionLossPolicy.IGNORE;

        isPersistenceEnabled = true;

        checkIgnore(killSingleNode);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
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
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10041")
    @Test
    public void testIgnoreKillThreeNodesWithPersistence() throws Exception {
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

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

            Collection<Integer> lost = cache.lostPartitions();

            assertTrue("[grid=" + ig.name() + ", lost=" + lost.toString() + ']', lost.isEmpty());

            int parts = ig.affinity(DEFAULT_CACHE_NAME).partitions();

            for (int i = 0; i < parts; i++) {
                cache.get(i);

                cache.put(i, i);
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

        List<Integer> lostParts = topChanger.changeTopology();

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
            info("Checking node: " + ig.cluster().localNode().id());

            IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

            verifyLostPartitions(ig, lostParts);

            verifyCacheOps(canWrite, safe, ig);

            validateQuery(safe, ig);

            // TODO withPartitionRecover doesn't work with BLT - https://issues.apache.org/jira/browse/IGNITE-10041.
            if (!isPersistenceEnabled) {
                // Check we can read and write to lost partition in recovery mode.
                IgniteCache<Integer, Integer> recoverCache = cache.withPartitionRecover();

                for (int lostPart : recoverCache.lostPartitions()) {
                    recoverCache.get(lostPart);
                    recoverCache.put(lostPart, lostPart);
                }

                // Check that writing in recover mode does not clear partition state.
                verifyLostPartitions(ig, lostParts);

                verifyCacheOps(canWrite, safe, ig);

                validateQuery(safe, ig);
            }
        }

        checkNewNode(true, canWrite, safe);
        checkNewNode(false, canWrite, safe);

        // Bring all nodes back.
        for (int i : topChanger.killNodes) {
            IgniteEx grd = startGrid(i);

            info("Newly started node: " + grd.cluster().localNode().id());

            // Check that partition state does not change after we start each node.
            // TODO With persistence enabled LOST partitions become OWNING after a node joins back - https://issues.apache.org/jira/browse/IGNITE-10044.
            if (!isPersistenceEnabled) {
                for (Ignite ig : G.allGrids()) {
                    verifyCacheOps(canWrite, safe, ig);

                    // TODO Query effectively waits for rebalance due to https://issues.apache.org/jira/browse/IGNITE-10057
                    // TODO and after resetLostPartition there is another OWNING copy in the cluster due to https://issues.apache.org/jira/browse/IGNITE-10058.
                    // TODO Uncomment after https://issues.apache.org/jira/browse/IGNITE-10058 is fixed.
//                    validateQuery(safe, ig);
                }
            }
        }

        ignite(4).resetLostPartitions(singletonList(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange(true, true, null);

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

            assertTrue(cache.lostPartitions().isEmpty());

            int parts = ig.affinity(DEFAULT_CACHE_NAME).partitions();

            for (int i = 0; i < parts; i++) {
                cache.get(i);

                cache.put(i, i);
            }

            for (int i = 0; i < parts; i++) {
                checkQueryPasses(ig, false, i);

                if (shouldExecuteLocalQuery(ig, i))
                    checkQueryPasses(ig, true, i);

            }

            checkQueryPasses(ig, false);
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
            IgniteEx cl = (IgniteEx)startGrid("newNode");

            CacheGroupContext grpCtx = cl.context().cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME));

            assertTrue(grpCtx.needsRecovery());

            verifyCacheOps(canWrite, safe, cl);

            validateQuery(safe, cl);
        }
        finally {
            stopGrid("newNode", false);

            this.client = false;
        }
    }

    /**
     * @param node Node.
     * @param lostParts Lost partition IDs.
     */
    private void verifyLostPartitions(Ignite node, List<Integer> lostParts) {
        IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

        Set<Integer> actualSortedLostParts = new TreeSet<>(cache.lostPartitions());
        Set<Integer> expSortedLostParts = new TreeSet<>(lostParts);

        assertEqualsCollections(expSortedLostParts, actualSortedLostParts);
    }

    /**
     * @param canWrite {@code True} if writes are allowed.
     * @param safe {@code True} if lost partition should trigger exception.
     * @param ig Ignite instance.
     */
    private void verifyCacheOps(boolean canWrite, boolean safe, Ignite ig) {
        IgniteCache<Integer, Integer> cache = ig.cache(DEFAULT_CACHE_NAME);

        int parts = ig.affinity(DEFAULT_CACHE_NAME).partitions();

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
    private List<Integer> noPrimaryOrBackupPartition(List<Integer> nodes) {
        Affinity<Object> aff = ignite(4).affinity(DEFAULT_CACHE_NAME);

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
    private void validateQuery(boolean safe, Ignite node) {
        // Get node lost and remaining partitions.
        IgniteCache<?, ?> cache = node.cache(DEFAULT_CACHE_NAME);

        Collection<Integer> lostParts = cache.lostPartitions();

        int part = cache.lostPartitions().stream().findFirst().orElseThrow(AssertionError::new);

        Integer remainingPart = null;

        for (int i = 0; i < node.affinity(DEFAULT_CACHE_NAME).partitions(); i++) {
            if (lostParts.contains(i))
                continue;

            remainingPart = i;

            break;
        }

        assertNotNull("Failed to find a partition that isn't lost", remainingPart);

        // 1. Check query against all partitions.
        validateQuery0(safe, node);

        // 2. Check query against LOST partition.
        validateQuery0(safe, node, part);

        // 3. Check query on remaining partition.
        checkQueryPasses(node, false, remainingPart);

        if (shouldExecuteLocalQuery(node, remainingPart))
            checkQueryPasses(node, true, remainingPart);

        // 4. Check query over two partitions - normal and LOST.
        validateQuery0(safe, node, part, remainingPart);
    }

    /**
     * Query validation routine.
     *
     * @param safe Safe flag.
     * @param node Node.
     * @param parts Partitions.
     */
    private void validateQuery0(boolean safe, Ignite node, int... parts) {
        if (safe)
            checkQueryFails(node, false, parts);
        else
            checkQueryPasses(node, false, parts);

        if (shouldExecuteLocalQuery(node, parts)) {
            if (safe)
                checkQueryFails(node, true, parts);
            else
                checkQueryPasses(node, true, parts);
        }
    }

    /**
     * @return true if the given node is primary for all given partitions.
     */
    private boolean shouldExecuteLocalQuery(Ignite node, int... parts) {
        if (parts == null || parts.length == 0)
            return false;

        int numOfPrimaryParts = 0;

        for (int nodePrimaryPart : node.affinity(DEFAULT_CACHE_NAME).primaryPartitions(node.cluster().localNode())) {
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
    protected void checkQueryPasses(Ignite node, boolean loc, int... parts) {
        // Scan queries don't support multiple partitions.
        if (parts != null && parts.length > 1)
            return;

        // TODO Local scan queries fail in non-safe modes - https://issues.apache.org/jira/browse/IGNITE-10059.
        if (loc)
            return;

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

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
    protected void checkQueryFails(Ignite node, boolean loc, int... parts) {
        if (parts.length == 0)
            return;

        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        String msg = loc ? "forced local query" : "partition has been lost";
        GridTestUtils.assertThrows(log, () -> {
            List res = null;
            for (int partition : parts) {
                ScanQuery qry = new ScanQuery();
                qry.setPartition(partition);

                if (loc)
                    qry.setLocal(true);

                res = cache.query(qry).getAll();
            }

            return res;
        }, IgniteCheckedException.class, msg);
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
        private List<Integer> changeTopology() throws Exception {
            startGrids(4);

            if (isPersistenceEnabled)
                grid(0).cluster().active(true);

            Affinity<Object> aff = ignite(0).affinity(DEFAULT_CACHE_NAME);

            for (int i = 0; i < aff.partitions(); i++)
                ignite(0).cache(DEFAULT_CACHE_NAME).put(i, i);

            client = true;

            startGrid(4);

            client = false;

            for (int i = 0; i < 5; i++)
                info(">>> Node [idx=" + i + ", nodeId=" + ignite(i).cluster().localNode().id() + ']');

            awaitPartitionMapExchange();

            final List<Integer> parts = noPrimaryOrBackupPartition(aliveNodes);

            if (parts.isEmpty())
                throw new IllegalStateException("No partition on nodes: " + killNodes);

            final List<Map<Integer, Semaphore>> lostMap = new ArrayList<>();

            for (int i : aliveNodes) {
                HashMap<Integer, Semaphore> semaphoreMap = new HashMap<>();

                for (Integer part : parts)
                    semaphoreMap.put(part, new Semaphore(0));

                lostMap.add(semaphoreMap);

                grid(i).events().localListen(new P1<Event>() {
                    @Override public boolean apply(Event evt) {
                        assert evt.type() == EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

                        CacheRebalancingEvent cacheEvt = (CacheRebalancingEvent)evt;

                        if (F.eq(DEFAULT_CACHE_NAME, cacheEvt.cacheName())) {
                            if (semaphoreMap.containsKey(cacheEvt.partition()))
                                semaphoreMap.get(cacheEvt.partition()).release();
                        }

                        return true;
                    }
                }, EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);
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

            for (Map<Integer, Semaphore> map : lostMap) {
                for (Map.Entry<Integer, Semaphore> entry : map.entrySet())
                    assertTrue("Failed to wait for partition LOST event for partition: " + entry.getKey(), entry.getValue().tryAcquire(1));
            }

            for (Map<Integer, Semaphore> map : lostMap) {
                for (Map.Entry<Integer, Semaphore> entry : map.entrySet())
                    assertFalse("Partition LOST event raised twice for partition: " + entry.getKey(), entry.getValue().tryAcquire(1));
            }

            return parts;
        }
    }
}
