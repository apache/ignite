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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PREFER_WAL_REBALANCE;

/**
 * Tests the MDC rebalance decision logic: when the engine chooses full, historical, or
 * skips rebalance after a node rejoins the cluster. Live network split-brain is covered
 * by {@link MultiDcSplitBrainRebalanceTest}.
 */
public class MultiDcRebalancingTest extends GridCommonAbstractTest {
    /** */
    private static final String DC1 = "DC1";

    /** */
    private static final String DC2 = "DC2";

    /** */
    private boolean pds;

    /** */
    private IgniteEx startGrid(int idx, String dcId) throws Exception {
        return startGrid(getConfiguration(getTestIgniteInstanceName(idx))
            .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(pds)))
            .setConsistentId(getTestIgniteInstanceName(idx))
            .setCommunicationSpi(new RebalanceAwareCommSPI())
            .setUserAttributes(F.asMap(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, dcId)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testFullRebalanceSameDC() throws Exception {
        checkFullRebalance(false);
    }

    /** */
    @Test
    public void testFullRebalanceCrossDC() throws Exception {
        checkFullRebalance(true);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testHistoricalRebalanceSameDC() throws Exception {
        checkHistoricalRebalance(false);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testHistoricalRebalanceCrossDC() throws Exception {
        checkHistoricalRebalance(true);
    }

    /**
     * Verifies that no rebalance demand is sent when a node restarts without any
     * writes having occurred during its absence.
     *
     * <p>Scenario: 2 nodes, 1000 entries, checkpoint. Node 1 stops, nothing is
     * written, node 1 restarts. Both nodes have identical partition counters,
     * the CRD sees only one value per partition, no node is "behind", no demand.
     */
    @Test
    public void testNoRebalanceWhenNoDelta() throws Exception {
        IgniteCache<Object, Object> cache = startTwoNodeCluster(1);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);
        IgniteEx ignite1 = startGrid(1, DC2);
        awaitPartitionMapExchange(true, true, null);
        RebalanceAwareCommSPI spi = commSPI(ignite1);
        assertFalse(spi.historical);
        assertFalse(spi.full);
    }

    /**
     * Verifies that partitions below {@code historical.rebalance.threshold} (default 500)
     * are NOT eligible for historical rebalance. The node uses full rebalance instead.
     *
     * <p>Scenario: 2 nodes, only 100 entries (each partition has ~1 entry, well
     * below threshold 500). Checkpoint. Node 1 stops, 5 more entries written
     * (small delta), node 1 restarts. Despite the tiny delta, partition size < 500
     * means it is not a historical candidate, so full rebalance is used.
     */
    @Test
    public void testBelowThresholdForcesFull() throws Exception {
        IgniteCache<Object, Object> cache = startTwoNodeCluster(1);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 100; i < 105; i++)
            cache.put(i, i);

        IgniteEx ignite1 = startGrid(1, DC2);
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        RebalanceAwareCommSPI spi = commSPI(ignite1);
        assertTrue(spi.rebalanceMsgCnt > 0);
        assertFalse(spi.historical);
        assertTrue(spi.full);
    }

    /**
     * Verifies that with a very small dataset, only non-empty partitions appear in
     * the rebalance demand (the skipZeros mechanism).
     *
     * <p>Scenario: 2 nodes, only 10 entries. Since there are 512 partitions, at most
     * ~10 have counter > 0. Node 1 stops, 5 more entries written, node 1 restarts.
     * The SingleMessage omits all empty partitions, the CRD only detects differences
     * for the ~10 non-empty partitions, total demanded is well below 512.
     */
    @Test
    public void testSkipZerosReducesRebalancedPartitions() throws Exception {
        IgniteCache<Object, Object> cache = startTwoNodeCluster(1);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 10; i < 15; i++)
            cache.put(i, i);

        IgniteEx ignite1 = startGrid(1, DC2);
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        RebalanceAwareCommSPI spi = commSPI(ignite1);
        assertTrue(spi.rebalanceMsgCnt > 0);
        assertTrue("Expected < 512 but got " + spi.totalDemandedParts,
            spi.totalDemandedParts < 512);
    }

    /**
     * Verifies historical rebalance after a DC node comes back following an outage.
     *
     * <p>Scenario: 3 nodes — node0 (DC1), node1 (DC2), node2 (DC1). All in baseline,
     * 10000 entries written. node1 (DC2) is stopped and writes nothing while down.
     * The two DC1 nodes write 10000 more entries each. Then node1 restarts.
     * Node1 must get cross-DC historical rebalance with the delta from both DC1 nodes.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testDcNodeDownHistoricalRebalance() throws Exception {
        IgniteCache<Object, Object> cache = startThreeNodeCluster(2);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        IgniteCache<Object, Object> cache2 = grid(2).cache(DEFAULT_CACHE_NAME);

        for (int i = 20_000; i < 30_000; i++)
            cache2.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1 = startGrid(1, DC2);
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        RebalanceAwareCommSPI spi = commSPI(ignite1);
        assertTrue(spi.rebalanceMsgCnt > 0);
        assertTrue(spi.historical);
        assertTrue(spi.crossDcRebalance);
    }

    /**
     * Verifies that after a DC node comes back from an outage, no data is lost. Data
     * written by both surviving DC1 nodes while the node was down is fully accessible
     * from the returning DC2 node after recovery.
     *
     * <p>Scenario: 3 nodes, node1 (DC2) down. DC1 nodes write 20000 new entries while
     * it is down. After recovery, all 30000 entries are readable from the DC2 node.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testDcNodeDownNoDataLoss() throws Exception {
        IgniteCache<Object, Object> cache = startThreeNodeCluster(2);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        IgniteCache<Object, Object> cache2 = grid(2).cache(DEFAULT_CACHE_NAME);

        for (int i = 20_000; i < 30_000; i++)
            cache2.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1 = startGrid(1, DC2);
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 30_000; i += 3000) {
            Object val = cache1.get(i);
            assertEquals(i, val);
        }
    }

    /**
     * Verifies that after a DC node comes back from an outage, if partition size is
     * below {@code historical.rebalance.threshold} (default 500), full rebalance is
     * used (no PREFER_WAL_REBALANCE set).
     *
     * <p>Scenario: 3 nodes, 100 entries (below threshold). node1 (DC2) down, DC1
     * writes 200 more entries. After recovery, full rebalance is expected.
     */
    @Test
    public void testDcNodeDownBelowThresholdFullRebalance() throws Exception {
        IgniteCache<Object, Object> cache = startThreeNodeCluster(2);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 100; i < 200; i++)
            cache.put(i, i);

        IgniteCache<Object, Object> cache2 = grid(2).cache(DEFAULT_CACHE_NAME);

        for (int i = 200; i < 300; i++)
            cache2.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1 = startGrid(1, DC2);
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        RebalanceAwareCommSPI spi = commSPI(ignite1);
        assertTrue(spi.rebalanceMsgCnt > 0);
        assertFalse(spi.historical);
        assertTrue(spi.full);
        assertTrue(spi.crossDcRebalance);
    }

    /**
     * Verifies that after a DC node comes back from an outage, if the delta is larger
     * than the partition size (and no PREFER_WAL_REBALANCE), full rebalance is used —
     * transferring the full partition is more efficient than replaying the delta.
     *
     * <p>Scenario: 3 nodes, 1000 base entries. node1 (DC2) down. The DC1 nodes each
     * write 20000 entries (delta >> partition size). After recovery, full rebalance
     * is expected (not historical).
     */
    @Test
    public void testDcNodeDownLargeDeltaFullRebalance() throws Exception {
        IgniteCache<Object, Object> cache = startThreeNodeCluster(2);

        for (int i = 0; i < 1000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 1000; i < 21_000; i++)
            cache.put(i, i);

        IgniteCache<Object, Object> cache2 = grid(2).cache(DEFAULT_CACHE_NAME);

        for (int i = 21_000; i < 41_000; i++)
            cache2.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1 = startGrid(1, DC2);
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        RebalanceAwareCommSPI spi = commSPI(ignite1);
        assertTrue(spi.rebalanceMsgCnt > 0);
        assertFalse(spi.historical);
        assertTrue(spi.full);
        assertTrue(spi.crossDcRebalance);
    }

    /**
     * Verifies that if one of two available source nodes also goes down while the DC
     * node is away, the surviving source node can still serve the historical rebalance.
     *
     * <p>Scenario: 3 nodes (node0 DC1, node1 DC2, node2 DC1). node1 (DC2) is stopped.
     * Then node0 (one DC1 source) is ALSO stopped. Only node2 remains. After recovery,
     * node1 must get historical rebalance from node2 alone.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testDcNodeDownOneSourceNodeSurvives() throws Exception {
        IgniteCache<Object, Object> cache = startThreeNodeCluster(2);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);
        stopGrid(0);
        IgniteCache<Object, Object> cache2 = grid(2).cache(DEFAULT_CACHE_NAME);

        for (int i = 10_000; i < 20_000; i++)
            cache2.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1 = startGrid(1, DC2);
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        RebalanceAwareCommSPI spi = commSPI(ignite1);
        assertTrue(spi.rebalanceMsgCnt > 0);
        assertTrue(spi.historical);
        assertTrue(spi.crossDcRebalance);
        IgniteCache<Object, Object> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 20_000; i += 5000) {
            Object val = cache1.get(i);
            assertEquals(i, val);
        }
    }

    /**
     * Verifies that concurrent writes on the source node during an in-progress
     * historical rebalance do not corrupt data. After the rebalance completes,
     * all entries (old + delta + concurrent) are accessible.
     *
     * <p>Scenario: 3 nodes. node1 (DC2) is stopped. DC1 writes 10000 entries (delta).
     * node1 restarts (triggers rebalance). While the rebalance is in flight,
     * DC1 writes another 5000 entries. After rebalance completes, verify all
     * data is accessible.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testConcurrentWritesDuringRecovery() throws Exception {
        IgniteCache<Object, Object> cache = startThreeNodeCluster(2);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1 = startGrid(1, DC2);
        IgniteCache<Object, Object> cache2 = grid(2).cache(DEFAULT_CACHE_NAME);

        for (int i = 20_000; i < 25_000; i++)
            cache2.put(i, i);

        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 25_000; i += 5000) {
            Object val = cache1.get(i);
            assertEquals(i, val);
        }
    }

    /**
     * Verifies that after a DC node outage recovery, if a new node also joins
     * (triggering affinity reassignment), the returning node gets a mixed demand:
     * historical for its own partitions AND full for reassigned partitions.
     *
     * <p>Scenario: 2 nodes initially. node1 (DC2) is stopped, DC1 writes delta.
     * Recovery: node1 restarts AND a new node (node2, DC1) joins simultaneously.
     * The topology change triggers both historical rebalance (for DC2's old
     * partitions) and full rebalance (for partitions reassigned to node2).
     */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testAffinityReassignmentMixedRebalance() throws Exception {
        IgniteCache<Object, Object> cache = startTwoNodeCluster(1);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1 = startGrid(1, DC2);
        IgniteEx ignite2 = startGrid(2, DC1);
        resetBaselineTopology();
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        waitRebalanceFinished(ignite2, DEFAULT_CACHE_NAME);

        RebalanceAwareCommSPI spi1 = commSPI(ignite1);
        assertTrue(spi1.rebalanceMsgCnt > 0);
        assertTrue(spi1.historical);

        RebalanceAwareCommSPI spi2 = commSPI(ignite2);
        assertTrue("New node should receive full rebalance", spi2.full);

        IgniteCache<Object, Object> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 20_000; i += 5000) {
            Object val = cache1.get(i);
            assertEquals(i, val);
        }
    }

    /**
     * Verifies that with {@code rebalanceDelay > 0} the rebalance always uses the full
     * strategy.
     *
     * <p>Documented limitation (see data-rebalancing.adoc, "Rebalance Strategy"): when a
     * rebalance delay is configured, the rebalance is scheduled outside of the partition
     * map exchange context, and the historical path (which is tied to that context) is
     * never taken — every partition is restored with a full rebalance regardless of the
     * delta size and regardless of {@code IGNITE_PREFER_WAL_REBALANCE}.
     *
     * <p>Related ticket: IGNITE-14524.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testRebalanceDelayForcesFull() throws Exception {
        pds = true;
        IgniteEx ignite0 = startGrid(0, DC1);
        startGrid(1, DC2);
        startGrid(2, DC1);
        ignite0.cluster().state(ClusterState.ACTIVE);
        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        cacheCfg.setBackups(2);
        cacheCfg.setRebalanceDelay(3000);
        IgniteCache<Object, Object> cache = ignite0.createCache(cacheCfg);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1 = startGrid(1, DC2);
        RebalanceAwareCommSPI spi = commSPI(ignite1);

        boolean demandSent = GridTestUtils.waitForCondition(() -> spi.rebalanceMsgCnt > 0, 30_000);

        assertTrue("No rebalance demand received within 30s", demandSent);

        awaitPartitionMapExchange(true, true, null);
        assertTrue(spi.full);
        assertFalse(spi.historical);
    }

    /**
     * Verifies that after MULTIPLE restarts (stop→write→start, repeated), the recovering
     * node accumulates all data across all restart cycles.
     *
     * <p>Per IGNITE-17738: after a crash/restart, historical rebalance may not deliver
     * updates between LWM and HWM. This test exercises two consecutive restart cycles
     * and asserts full data integrity after each.
     *
     * <p>Scenario: 3 nodes. 10_000 base entries.
     * Cycle 1: node1 down → node0 writes 10_000 → node1 restarts → verify 20_000.
     * Cycle 2: node1 down → node0 writes 10_000 → node1 restarts → verify 30_000.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testMultipleRestartsNoDataLoss() throws Exception {
        IgniteCache<Object, Object> cache = startThreeNodeCluster(2);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1 = startGrid(1, DC2);
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);

        IgniteCache<Object, Object> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 20_000; i += 5000) {
            Object val = cache1.get(i);
            assertEquals(i, val);
        }

        forceCheckpoint();
        stopGrid(1);

        for (int i = 20_000; i < 30_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        IgniteEx ignite1Restarted = startGrid(1, DC2);
        waitRebalanceFinished(ignite1Restarted, DEFAULT_CACHE_NAME);

        IgniteCache<Object, Object> cache1After = ignite1Restarted.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 30_000; i += 5000) {
            Object val = cache1After.get(i);
            assertEquals(i, val);
        }
    }

    /**
     * Verifies that when historical (WAL) rebalance is impossible, the supplier falls
     * back to a FULL rebalance and no data is lost.
     *
     * <p>Truncated WAL is simulated deterministically by marking the last checkpoint as
     * inapplicable for WAL rebalance on the source nodes
     * ({@code lastCheckpointInapplicableForWalRebalance}) — exactly the flag the supplier
     * sets itself when a WAL segment turns out to be unreadable
     * ({@code GridDhtPartitionSupplier} fallbackToFullRebalance, line 451). This is the
     * production situation when WAL segments covering the delta were already deleted.
     *
     * <p>Assertion: despite {@code PREFER_WAL_REBALANCE=true} the recovering node gets a
     * FULL rebalance (historical is impossible), and all entries are present afterwards.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PREFER_WAL_REBALANCE, value = "true")
    public void testWalTruncatedForcesFull() throws Exception {
        IgniteCache<Object, Object> cache = startThreeNodeCluster(2);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();
        stopGrid(1);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        forceCheckpoint();

        int grpId = ((GridCacheProxyImpl<Object, Object>)grid(0).cachex(DEFAULT_CACHE_NAME))
            .context().group().groupId();

        markWalRebalanceInapplicable(grid(0), grpId);
        markWalRebalanceInapplicable(grid(2), grpId);

        IgniteEx ignite1 = startGrid(1, DC2);
        waitRebalanceFinished(ignite1, DEFAULT_CACHE_NAME);
        RebalanceAwareCommSPI spi = commSPI(ignite1);

        assertTrue(spi.full);
        assertFalse(spi.historical);

        IgniteCache<Object, Object> cache1 = ignite1.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 20_000; i += 5000) {
            Object val = cache1.get(i);
            assertEquals(i, val);
        }
    }

    /**
     * Starts a 2-node MDC cluster (node0 in DC1, node1 in DC2) in ACTIVE state
     * and creates a cache with the given number of backups.
     *
     * @param backups Number of backups for the cache.
     * @return The created cache.
     */
    private IgniteCache<Object, Object> startTwoNodeCluster(int backups) throws Exception {
        pds = true;
        startGrid(0, DC1);
        startGrid(1, DC2);
        grid(0).cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        cacheCfg.setBackups(backups);
        return grid(0).createCache(cacheCfg);
    }

    /**
     * Starts a 3-node MDC cluster (node0 in DC1, node1 in DC2, node2 in DC1) in
     * ACTIVE state and creates a cache with the given number of backups.
     *
     * @param backups Number of backups for the cache.
     * @return The created cache.
     */
    private IgniteCache<Object, Object> startThreeNodeCluster(int backups) throws Exception {
        pds = true;
        startGrid(0, DC1);
        startGrid(1, DC2);
        startGrid(2, DC1);
        grid(0).cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        cacheCfg.setBackups(backups);
        return grid(0).createCache(cacheCfg);
    }

    /** */
    private void markWalRebalanceInapplicable(IgniteEx ignite, int grpId) {
        IgniteCacheDatabaseSharedManager db = ignite.context().cache().context().database();
        db.lastCheckpointInapplicableForWalRebalance(grpId);
    }

    /** */
    private void checkFullRebalance(boolean crossDC) throws Exception {
        startGrid(0, DC1);
        startGrid(1, crossDC ? DC1 : DC2);

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        IgniteCache<Object, Object> cache = grid(0).createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setBackups(1));

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        IgniteEx ignite2 = startGrid(2, crossDC ? DC2 : DC1);
        IgniteEx ignite3 = startGrid(3, DC2);

        resetBaselineTopology();

        waitRebalanceFinished(ignite2, DEFAULT_CACHE_NAME);
        waitRebalanceFinished(ignite3, DEFAULT_CACHE_NAME);

        assertTrue(commSPI(ignite2).rebalanceMsgCnt > 0);
        assertTrue(commSPI(ignite3).rebalanceMsgCnt > 0);
        assertFalse(commSPI(ignite2).historical);
        assertFalse(commSPI(ignite3).historical);
        assertEquals(crossDC, commSPI(ignite2).crossDcRebalance);
        assertEquals(crossDC, commSPI(ignite3).crossDcRebalance);
    }

    /** */
    private void checkHistoricalRebalance(boolean crossDC) throws Exception {
        pds = true;

        startGrid(0, DC1);
        startGrid(1, DC2);
        startGrid(2, DC1);

        grid(0).cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = grid(0).createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setBackups(crossDC ? 1 : 2));

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        forceCheckpoint();

        stopGrid(2);

        for (int i = 10_000; i < 20_000; i++)
            cache.put(i, i);

        IgniteEx ignite2 = startGrid(2, DC1);

        waitRebalanceFinished(ignite2, DEFAULT_CACHE_NAME);

        assertTrue(commSPI(ignite2).rebalanceMsgCnt > 0);
        assertTrue(commSPI(ignite2).historical);
        assertEquals(crossDC, commSPI(ignite2).crossDcRebalance);
    }

    /** */
    private RebalanceAwareCommSPI commSPI(Ignite ignite) {
        return (RebalanceAwareCommSPI)(ignite.configuration().getCommunicationSpi());
    }

    /** */
    private static class RebalanceAwareCommSPI extends TcpCommunicationSpi {
        /** */
        private int rebalanceMsgCnt;

        /** */
        private boolean historical;

        /** */
        private boolean crossDcRebalance;

        /** */
        private boolean full;

        /** */
        private int totalDemandedParts;

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackClosure
        ) throws IgniteSpiException {
            if (((GridIoMessage)msg).message() instanceof GridDhtPartitionDemandMessage) {
                rebalanceMsgCnt++;

                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)((GridIoMessage)msg).message();

                historical |= demandMsg.partitions().hasHistorical();

                IgniteDhtDemandedPartitionsMap parts = demandMsg.partitions();

                full |= parts.hasFull();

                if (parts.hasHistorical()) {
                    Set<Integer> histSet = parts.historicalSet();
                    totalDemandedParts += histSet.size();
                }

                if (parts.hasFull()) {
                    Set<Integer> fullSet = parts.fullSet();
                    totalDemandedParts += fullSet.size();
                }

                if (!ignite.cluster().localNode().dataCenterId().equals(node.dataCenterId()))
                    crossDcRebalance = true;
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}

