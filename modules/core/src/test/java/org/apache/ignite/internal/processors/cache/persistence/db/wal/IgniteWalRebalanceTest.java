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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.WalTestUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;

/**
 * Historical WAL rebalance base test.
 */
public class IgniteWalRebalanceTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Partitions count. */
    private static final int PARTS_CNT = 32;

    /** Block message predicate to set to Communication SPI in node configuration. */
    private IgniteBiPredicate<ClusterNode, Message> blockMsgPred;

    /** Record message predicate to set to Communication SPI in node configuration. */
    private IgniteBiPredicate<ClusterNode, Message> recordMsgPred;

    /** */
    private int backups;

    /** User attributes. */
    private final Map<String, Serializable> userAttrs = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0"); //to make all rebalance wal-based

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(backups)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setWalSegmentSize(4 * 1024 * 1024)
            .setWalHistorySize(Integer.MAX_VALUE)
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(15 * 60 * 1000)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE));

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setCommunicationSpi(new WalRebalanceCheckingCommunicationSpi());

        if (blockMsgPred != null)
            ((TestRecordingCommunicationSpi) cfg.getCommunicationSpi()).blockMessages(blockMsgPred);

        if (recordMsgPred != null)
            ((TestRecordingCommunicationSpi) cfg.getCommunicationSpi()).record(recordMsgPred);

        cfg.setFailureHandler(new StopNodeFailureHandler());
        cfg.setConsistentId(gridName);
        cfg.setUserAttributes(userAttrs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.clearProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD);
        System.clearProperty(IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING);

        boolean walRebalanceInvoked = !IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.allRebalances()
            .isEmpty();

        IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.cleanup();

        stopAllGrids();

        cleanPersistenceDir();

        if (!walRebalanceInvoked)
            throw new AssertionError("WAL rebalance hasn't been invoked.");
    }

    /**
     * Test simple WAL historical rebalance.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testSimple() throws Exception {
        backups = 4;

        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);

        final int entryCnt = PARTS_CNT * 100;
        final int preloadEntryCnt = PARTS_CNT * 101;

        ig0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = ig0.cache(CACHE_NAME);

        for (int k = 0; k < preloadEntryCnt; k++)
            cache.put(k, new IndexedObject(k));

        forceCheckpoint();

        stopGrid(1, false);

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k + 1));

        forceCheckpoint();

        ig1 = startGrid(1);

        awaitPartitionMapExchange();

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Object, Object> cache1 = ig.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++)
                assertEquals(new IndexedObject(k + 1), cache1.get(k));
        }
    }

    /**
     * Test that cache entry removes are rebalanced properly using WAL.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceRemoves() throws Exception {
        backups = 4;

        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);

        final int entryCnt = PARTS_CNT * 100;
        final int preloadEntryCnt = PARTS_CNT * 135;

        ig0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = ig0.cache(CACHE_NAME);

        for (int k = 0; k < preloadEntryCnt; k++)
            cache.put(k, new IndexedObject(k));

        forceCheckpoint();

        stopGrid(1, false);

        for (int k = 0; k < entryCnt; k++) {
            if (k % 3 != 2)
                cache.put(k, new IndexedObject(k + 1));
            else // Spread removes across all partitions.
                cache.remove(k);
        }

        forceCheckpoint();

        ig1 = startGrid(1);

        awaitPartitionMapExchange();

        for (Ignite ig : G.allGrids()) {
            IgniteCache<Object, Object> cache1 = ig.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++) {
                if (k % 3 != 2)
                    assertEquals(new IndexedObject(k + 1), cache1.get(k));
                else
                    assertNull(cache1.get(k));
            }
        }
    }

    /**
     * Test that WAL rebalance is not invoked if there are gaps in WAL history due to temporary WAL disabling.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWithLocalWalChange() throws Exception {
        backups = 4;

        System.setProperty(IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING, "true");

        IgniteEx crd = startGrids(4);

        crd.cluster().state(ACTIVE);

        final int entryCnt = PARTS_CNT * 10;
        final int preloadEntryCnt = PARTS_CNT * 11;

        {
            IgniteCache<Object, Object> cache = crd.cache(CACHE_NAME);

            for (int k = 0; k < preloadEntryCnt; k++)
                cache.put(k, new IndexedObject(k - 1));
        }

        forceCheckpoint();

        stopAllGrids();

        IgniteEx ig0 = startGrids(2);

        ig0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = ig0.cache(CACHE_NAME);

        int grpId = ig0.cachex(CACHE_NAME).context().groupId();

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k));

        forceCheckpoint();

        // This node should rebalance data from other nodes and shouldn't have WAL history.
        Ignite ignite = startGrid(2);

        awaitPartitionMapExchange();

        Set<Long> topVers = ((WalRebalanceCheckingCommunicationSpi) ignite.configuration().getCommunicationSpi())
            .walRebalanceVersions(grpId);

        Assert.assertTrue(topVers.contains(ignite.cluster().topologyVersion()));

        // Rewrite some data.
        for (int k = 0; k < entryCnt; k++) {
            if (k % 3 == 0)
                cache.put(k, new IndexedObject(k + 1));
            else if (k % 3 == 1) // Spread removes across all partitions.
                cache.remove(k);
        }

        forceCheckpoint();

        // Stop grids which have actual WAL history.
        stopGrid(0);

        stopGrid(1);

        // Start new node which should rebalance all data from node(2) without using WAL,
        // because node(2) doesn't have full history for rebalance.
        ignite = startGrid(3);

        awaitPartitionMapExchange();

        topVers = ((WalRebalanceCheckingCommunicationSpi) ignite.configuration().getCommunicationSpi())
            .walRebalanceVersions(grpId);

        Assert.assertFalse(topVers.contains(ignite.cluster().topologyVersion()));

        // Check data consistency.
        for (Ignite ig : G.allGrids()) {
            IgniteCache<Object, Object> cache1 = ig.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++) {
                if (k % 3 == 0)
                    assertEquals(new IndexedObject(k + 1), cache1.get(k));
                else if (k % 3 == 1)
                    assertNull(cache1.get(k));
                else
                    assertEquals(new IndexedObject(k), cache1.get(k));
            }
        }
    }

    /**
     * Test that WAL rebalance is not invoked if there are gaps in WAL history due to global WAL disabling.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWithGlobalWalChange() throws Exception {
        backups = 4;

        // Prepare some data.
        IgniteEx crd = startGrids(3);

        crd.cluster().state(ACTIVE);

        final int entryCnt = PARTS_CNT * 10;

        final int preloadEntryCnt = PARTS_CNT * 11;

        {
            IgniteCache<Object, Object> cache = crd.cache(CACHE_NAME);

            // Preload should be more than data coming through historical rebalance
            // Otherwise cluster may to choose a full rebalance instead of historical one.
            for (int k = 0; k < preloadEntryCnt; k++)
                cache.put(k, new IndexedObject(k - 1));
        }

        forceCheckpoint();

        stopAllGrids();

        // Rewrite data with globally disabled WAL.
        crd = startGrids(2);

        crd.cluster().state(ACTIVE);

        crd.cluster().disableWal(CACHE_NAME);

        IgniteCache<Object, Object> cache = crd.cache(CACHE_NAME);

        int grpId = crd.cachex(CACHE_NAME).context().groupId();

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k));

        forceCheckpoint();

        crd.cluster().enableWal(CACHE_NAME);

        // This node shouldn't rebalance data using WAL, because it was disabled on other nodes.
        IgniteEx ignite = startGrid(2);

        awaitPartitionMapExchange();

        Set<Long> topVers = ((WalRebalanceCheckingCommunicationSpi) ignite.configuration().getCommunicationSpi())
            .walRebalanceVersions(grpId);

        Assert.assertFalse(topVers.contains(ignite.cluster().topologyVersion()));

        stopGrid(2);

        // Fix actual state to have start point in WAL to rebalance from.
        forceCheckpoint();

        // After another rewriting data with enabled WAL, node should rebalance this diff using WAL rebalance.
        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k + 1));

        ignite = startGrid(2);

        awaitPartitionMapExchange();

        topVers = ((WalRebalanceCheckingCommunicationSpi) ignite.configuration().getCommunicationSpi())
            .walRebalanceVersions(grpId);

        Assert.assertTrue(topVers.contains(ignite.cluster().topologyVersion()));

        // Check data consistency.
        for (Ignite ig : G.allGrids()) {
            IgniteCache<Object, Object> cache1 = ig.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++)
                assertEquals(new IndexedObject(k + 1), cache1.get(k));
        }
    }

    /**
     * Tests that cache rebalance is cancelled if supplyer node got exception during iteration over WAL.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceCancelOnSupplyError() throws Exception {
        backups = 4;

        // Prepare some data.
        IgniteEx crd = startGrids(3);

        crd.cluster().state(ACTIVE);

        final int entryCnt = PARTS_CNT * 10;
        final int preloadEntryCnt = PARTS_CNT * 11;

        {
            IgniteCache<Object, Object> cache = crd.cache(CACHE_NAME);

            for (int k = 0; k < preloadEntryCnt; k++)
                cache.put(k, new IndexedObject(k - 1));
        }

        forceCheckpoint();

        stopAllGrids();

        // Rewrite data to trigger further rebalance.
        IgniteEx supplierNode = startGrid(0);

        supplierNode.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = supplierNode.cache(CACHE_NAME);

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k));

        forceCheckpoint();

        final int grpId = supplierNode.cachex(CACHE_NAME).context().groupId();

        // Delay rebalance process for specified group.
        blockMsgPred = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage)
                return ((GridDhtPartitionDemandMessage) msg).groupId() == grpId;

            return false;
        };

        IgniteEx demanderNode = startGrid(2);

        AffinityTopologyVersion curTopVer = demanderNode.context().discovery().topologyVersionEx();

        // Wait for rebalance process start on demander node.
        final GridCachePreloader preloader = demanderNode.cachex(CACHE_NAME).context().group().preloader();

        GridTestUtils.waitForCondition(() ->
                ((GridDhtPartitionDemander.RebalanceFuture)preloader.rebalanceFuture()).topologyVersion().equals(curTopVer),
            getTestTimeout()
        );

        // Inject I/O factory which can throw exception during WAL read on supplier node.
        FailingIOFactory ioFactory = injectFailingIOFactory(supplierNode);

        // Resume rebalance process.
        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi) demanderNode.configuration().getCommunicationSpi();

        spi.stopBlock();

        // Wait till rebalance will be failed and cancelled.
        Boolean res = preloader.rebalanceFuture().get();

        Assert.assertEquals("Rebalance should be cancelled on demander node: " + preloader.rebalanceFuture(), false, res);

        // Stop blocking messages and fail WAL during read.
        blockMsgPred = null;

        ioFactory.reset();

        // Start last grid and wait for rebalance.
        startGrid(1);

        awaitPartitionMapExchange();

        // Check data consistency.
        for (Ignite ig : G.allGrids()) {
            IgniteCache<Object, Object> cache1 = ig.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++)
                assertEquals(new IndexedObject(k), cache1.get(k));
        }
    }

    /**
     * Tests that demander switches to full rebalance if the previously chosen two of three of suppliers
     * for a group have failed to perform historical rebalance due to an unexpected error.
     *
     * @throws Exception If failed
     */
    @Test
    @WithSystemProperty(key = "IGNITE_DISABLE_WAL_DURING_REBALANCING", value = "true")
    public void testMultipleNodesFailHistoricalRebalance() throws Exception {
        backups = 1;
        int node_cnt = 4;
        int demanderId = node_cnt - 1;

        // Start a new cluster with 3 suppliers.
        startGrids(node_cnt - 1);

        // Start demander node.
        userAttrs.put("TEST_ATTR", "TEST_ATTR");
        startGrid(node_cnt - 1);

        grid(0).cluster().state(ACTIVE);

        // Create a new cache that places a full set of partitions on demander node.
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false, PARTS_CNT);
        aff.setAffinityBackupFilter(new ClusterNodeAttributeAffinityBackupFilter("TEST_ATTR"));

        String cacheName = "test-cache-1";
        IgniteCache<Integer, IndexedObject> cache0 = grid(0).getOrCreateCache(
            new CacheConfiguration<Integer, IndexedObject>(cacheName)
                .setBackups(backups)
                .setAffinity(aff)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));

        // Fill initial data and force checkpoint.
        final int entryCnt = PARTS_CNT * 200;
        final int preloadEntryCnt = PARTS_CNT * 201;

        for (int k = 0; k < preloadEntryCnt; k++)
            cache0.put(k, new IndexedObject(k));

        forceCheckpoint();

        // Stop demander node.
        stopGrid(demanderId);

        // Rewrite data to trigger further rebalance.
        for (int k = 0; k < entryCnt; k++) {
            // Should skip one random partition to be sure that after restarting demander node,
            // it will have at least one partition in OWNING state, and so WAL will not be disabled while rebalancing.
            // This fact allows moving partitions to OWNING state during rebalancing
            // even though the corresponding RebalanceFuture will be cancelled.
            if (grid(0).affinity(cacheName).partition(k) != 12)
                cache0.put(k, new IndexedObject(k));
        }

        // Upload additional data to a particular partition (primary partition belongs to coordinator, for instance)
        // in order to trigger full rebalance for that partition instead of historical one.
        int[] primaries0 = grid(0).affinity(cacheName).primaryPartitions(grid(0).localNode());
        for (int i = 0; i < preloadEntryCnt; ++i)
            cache0.put(primaries0[0], new IndexedObject(primaries0[0]));

        forceCheckpoint();

        // Delay rebalance process for specified group.
        blockMsgPred = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                return msg0.groupId() == CU.cacheId(cacheName);
            }

            return false;
        };

        Queue<RecordedDemandMessage> recorderedMsgs = new ConcurrentLinkedQueue<>();

        // Record demand messages for specified group.
        recordMsgPred = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                if (msg0.groupId() == CU.cacheId(cacheName)) {
                    recorderedMsgs.add(new RecordedDemandMessage(
                        node.id(),
                        msg0.groupId(),
                        msg0.partitions().hasFull(),
                        msg0.partitions().hasHistorical()));
                }
            }

            return false;
        };

        // Corrupt WAL on suppliers, except the one.
        injectFailingIOFactory(grid(0));
        injectFailingIOFactory(grid(1));

        // Trigger rebalance process from suppliers.
        IgniteEx restartedDemander = startGrid(node_cnt - 1);

        TestRecordingCommunicationSpi demanderSpi = TestRecordingCommunicationSpi.spi(restartedDemander);

        // Wait until demander starts historical rebalancning.
        demanderSpi.waitForBlocked();

        final IgniteInternalFuture<Boolean> preloadFut = restartedDemander.cachex(cacheName).context().group()
            .preloader().rebalanceFuture();

        // Unblock messages and start tracking demand and supply messages.
        demanderSpi.stopBlock();

        // Wait until rebalancing will be cancelled for both suppliers.
        assertTrue(
            "Rebalance future was not cancelled [fut=" + preloadFut + ']',
            GridTestUtils.waitForCondition(preloadFut::isDone, getTestTimeout()));

        Assert.assertEquals(
            "Rebalance should be cancelled on demander node: " + preloadFut,
            false,
            preloadFut.get());

        awaitPartitionMapExchange(true, true, null);

        // Check data consistency.
        assertPartitionsSame(idleVerify(restartedDemander, cacheName));

        // Check that historical rebalance switched to full for supplier 1 & 2 and it was historical for supplier3.
        IgnitePredicate<RecordedDemandMessage> histPred = msg ->
            msg.hasHistorical() && !msg.hasFull();

        IgnitePredicate<RecordedDemandMessage> fullPred = msg ->
            !msg.hasHistorical() && msg.hasFull();

        IgnitePredicate<RecordedDemandMessage> mixedPred = msg ->
            msg.hasHistorical() && msg.hasFull();

        IgniteBiInClosure<UUID, Boolean> supplierChecker = (supplierId, mixed) -> {
            List<RecordedDemandMessage> demandMsgsForSupplier = recorderedMsgs.stream()
                // Filter messages correspond to the supplierId
                .filter(msg -> msg.supplierId().equals(supplierId))
                .filter(msg -> msg.groupId() == CU.cacheId(cacheName))
                // Filter out intermediate messages
                .filter(msg -> msg.hasFull() || msg.hasHistorical())
                .collect(toList());

            assertEquals("There should only two demand messages [supplierId=" + supplierId + ']',
                2,
                demandMsgsForSupplier.size());
            assertTrue(
                "The first message should require " + (mixed ? "mixed" : "historical") + " rebalance [msg=" +
                    demandMsgsForSupplier.get(0) + ']',
                (mixed ? mixedPred.apply(demandMsgsForSupplier.get(0)) : histPred.apply(demandMsgsForSupplier.get(0))));
            assertTrue(
                "The second message should require full rebalance [msg=" + demandMsgsForSupplier.get(0) + ']',
                fullPred.apply(demandMsgsForSupplier.get(1)));
        };

        supplierChecker.apply(grid(0).cluster().localNode().id(), true);
        supplierChecker.apply(grid(1).cluster().localNode().id(), false);

        // Check supplier3
        List<RecordedDemandMessage> demandMsgsForSupplier = recorderedMsgs.stream()
            // Filter messages correspond to the supplier3
            .filter(msg -> msg.supplierId().equals(grid(2).cluster().localNode().id()))
            .filter(msg -> msg.groupId() == CU.cacheId(cacheName))
            // Filter out intermediate messages
            .filter(msg -> msg.hasFull() || msg.hasHistorical())
            .collect(toList());

        assertEquals("There should only one demand message.", 1, demandMsgsForSupplier.size());
        assertTrue(
            "The first message should require historical rebalance [msg=" + demandMsgsForSupplier.get(0) + ']',
            histPred.apply(demandMsgsForSupplier.get(0)));
    }


    /**
     * Tests that demander switches to full rebalance if the previously chosen supplier for a group has failed
     * to perform historical rebalance due to an unexpected error while historical iterator (wal iterator) is created.
     * Additionally, the client node joins the cluster between the demand message sent, and the supply message received.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSwitchHistoricalRebalanceToFullAndClientJoin() throws Exception {
        testSwitchHistoricalRebalanceToFull(IgniteWalRebalanceTest::injectFailingIOFactory, true);
    }

    /**
     * Tests that demander switches to full rebalance if the previously chosen supplier for a group has failed
     * to perform historical rebalance due to an unexpected error while historical iterator (wal iterator) is created.
     *
     * @throws Exception If failed
     */
    @Test
    public void testSwitchHistoricalRebalanceToFullDueToFailOnCreatingWalIterator() throws Exception {
        testSwitchHistoricalRebalanceToFull(IgniteWalRebalanceTest::injectFailingIOFactory, false);
    }

    /**
     * Tests that demander switches to full rebalance if the previously chosen supplier for a group has failed
     * to perform historical rebalance due to an unexpected error while iterating over reserved wal.
     *
     * @throws Exception If failed
     */
    @Test
    public void testSwitchHistoricalRebalanceToFullWhileIteratingOverWAL() throws Exception {
        testSwitchHistoricalRebalanceToFull(supplier1 -> {
            try {
                // Corrupt wal record in order to fail historical rebalance from supplier1 node.
                IgniteWriteAheadLogManager walMgr = supplier1.context().cache().context().wal();

                WALPointer ptr = walMgr.log(new DataRecord(new DataEntry(
                    CU.cacheId("test-cache-1"),
                    new KeyCacheObjectImpl(0, null, 0),
                    null,
                    GridCacheOperation.DELETE,
                    new GridCacheVersion(0, 1, 1, 0),
                    new GridCacheVersion(0, 1, 1, 0),
                    0,
                    0,
                    0
                )));

                File walDir = U.field(walMgr, "walWorkDir");

                List<FileDescriptor> walFiles = new IgniteWalIteratorFactory().resolveWalFiles(
                    new IgniteWalIteratorFactory.IteratorParametersBuilder().filesOrDirs(walDir));

                FileDescriptor lastWalFile = walFiles.get(walFiles.size() - 1);

                WalTestUtils.corruptWalSegmentFile(lastWalFile, ptr);

                IgniteCache<Integer, IndexedObject> c1 = supplier1.cache("test-cache-1");
                for (int i = 0; i < PARTS_CNT * 100; i++)
                    c1.put(i, new IndexedObject(i));
            }
            catch (IgniteCheckedException | IOException e) {
                throw new RuntimeException(e);
            }
        }, false);
    }

    /**
     * Tests that demander switches to full rebalance if the previously chosen supplier for a group has failed
     * to perform historical rebalance due to an unexpected error.
     *
     * @param corruptWalClo Closure that corrupts wal iterating on supplier node.
     * @param needClientStart {@code true} if client node should join the cluster between
     *                                    the demand message sent and the supply message received.
     * @throws Exception If failed
     */
    public void testSwitchHistoricalRebalanceToFull(
        IgniteInClosure<IgniteEx> corruptWalClo,
        boolean needClientStart
    ) throws Exception {
        backups = 3;

        IgniteEx supplier1 = startGrid(0);
        IgniteEx supplier2 = startGrid(1);
        IgniteEx demander = startGrid(2);

        supplier1.cluster().state(ACTIVE);

        String supplier1Name = supplier1.localNode().consistentId().toString();
        String supplier2Name = supplier2.localNode().consistentId().toString();
        String demanderName = demander.localNode().consistentId().toString();

        String cacheName1 = "test-cache-1";
        String cacheName2 = "test-cache-2";

        // Cache resides on supplier1 and demander nodes.
        IgniteCache<Integer, IndexedObject> c1 = supplier1.getOrCreateCache(
            new CacheConfiguration<Integer, IndexedObject>(cacheName1)
                .setBackups(backups)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT))
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setRebalanceOrder(10)
                .setNodeFilter(n -> n.consistentId().equals(supplier1Name) || n.consistentId().equals(demanderName)));

        // Cache resides on supplier2 and demander nodes.
        IgniteCache<Integer, IndexedObject> c2 = supplier1.getOrCreateCache(
            new CacheConfiguration<Integer, IndexedObject>("test-cache-2")
                .setBackups(backups)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT))
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setRebalanceOrder(20)
                .setNodeFilter(n -> n.consistentId().equals(supplier2Name) || n.consistentId().equals(demanderName)));

        // Fill initial data.
        final int entryCnt = PARTS_CNT * 200;
        final int preloadEntryCnt = PARTS_CNT * 400;

        for (int k = 0; k < preloadEntryCnt; k++) {
            c1.put(k, new IndexedObject(k));

            c2.put(k, new IndexedObject(k));
        }

        forceCheckpoint();

        stopGrid(2);

        // Rewrite data to trigger further rebalance.
        for (int i = 0; i < entryCnt; i++) {
            c1.put(i, new IndexedObject(i));

            c2.put(i, new IndexedObject(i));
        }

        // Delay rebalance process for specified groups.
        blockMsgPred = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                return msg0.groupId() == CU.cacheId(cacheName1) || msg0.groupId() == CU.cacheId(cacheName2);
            }

            return false;
        };

        Queue<RecordedDemandMessage> recorderedMsgs = new ConcurrentLinkedQueue<>();

        // Record demand messages for specified groups.
        recordMsgPred = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                if (msg0.groupId() == CU.cacheId(cacheName1) || msg0.groupId() == CU.cacheId(cacheName2)) {
                    recorderedMsgs.add(new RecordedDemandMessage(
                        node.id(),
                        msg0.groupId(),
                        msg0.partitions().hasFull(),
                        msg0.partitions().hasHistorical()));
                }
            }

            return false;
        };

        // Delay rebalance process for specified group from supplier2.
        TestRecordingCommunicationSpi supplierSpi2 = TestRecordingCommunicationSpi.spi(supplier2);
        supplierSpi2.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionSupplyMessage) {
                GridDhtPartitionSupplyMessage msg0 = (GridDhtPartitionSupplyMessage)msg;

                return node.consistentId().equals(demanderName) && msg0.groupId() == CU.cacheId(cacheName2);
            }

            return false;
        });

        // Corrupt WAL on supplier1
        corruptWalClo.apply(supplier1);

        // Trigger rebalance process from suppliers.
        IgniteEx restartedDemander = startGrid(2);

        recordMsgPred = null;
        blockMsgPred = null;

        TestRecordingCommunicationSpi demanderSpi = TestRecordingCommunicationSpi.spi(grid(2));

        // Wait until demander starts historical rebalancning.
        demanderSpi.waitForBlocked();

        final IgniteInternalFuture<Boolean> preloadFut1 = restartedDemander.cachex(cacheName1).context().group()
            .preloader().rebalanceFuture();
        final IgniteInternalFuture<Boolean> preloadFut2 = restartedDemander.cachex(cacheName2).context().group()
            .preloader().rebalanceFuture();

        if (needClientStart)
            startClientGrid(3);

        // Unblock messages and start tracking demand and supply messages.
        demanderSpi.stopBlock();

        // Wait until rebalancing will be cancelled for both suppliers.
        GridTestUtils.waitForCondition(() -> preloadFut1.isDone() && preloadFut2.isDone(), getTestTimeout());

        Assert.assertEquals(
            "Rebalance should be cancelled on demander node: " + preloadFut1,
            false,
            preloadFut1.get());
        Assert.assertEquals(
            "Rebalance should be cancelled on demander node: " + preloadFut2,
            false,
            preloadFut2.get());

        // Unblock supply messages from supplier2
        supplierSpi2.stopBlock();

        awaitPartitionMapExchange(true, true, null);

        // Check data consistency.
        assertPartitionsSame(idleVerify(restartedDemander, cacheName2, cacheName1));

        // Check that historical rebalance switched to full for supplier1 and it is still historical for supplier2.
        IgnitePredicate<RecordedDemandMessage> histPred = (msg) ->
            msg.hasHistorical() && !msg.hasFull();

        IgnitePredicate<RecordedDemandMessage> fullPred = (msg) ->
            !msg.hasHistorical() && msg.hasFull();

        // Supplier1
        List<RecordedDemandMessage> demandMsgsForSupplier1 = recorderedMsgs.stream()
            // Filter messages correspond to the supplier1
            .filter(msg -> msg.groupId() == CU.cacheId(cacheName1))
            // Filter out intermediate messages
            .filter(msg -> msg.hasFull() || msg.hasHistorical())
            .collect(toList());

        assertEquals("There should only two demand messages.", 2, demandMsgsForSupplier1.size());
        assertTrue(
            "The first message should require historical rebalance [msg=" + demandMsgsForSupplier1.get(0) + ']',
            histPred.apply(demandMsgsForSupplier1.get(0)));
        assertTrue(
            "The second message should require full rebalance [msg=" + demandMsgsForSupplier1.get(0) + ']',
            fullPred.apply(demandMsgsForSupplier1.get(1)));

        // Supplier2
        List<RecordedDemandMessage> demandMsgsForSupplier2 = recorderedMsgs.stream()
            // Filter messages correspond to the supplier2
            .filter(msg -> msg.groupId() == CU.cacheId(cacheName2))
            // Filter out intermediate messages
            .filter(msg -> msg.hasFull() || msg.hasHistorical())
            .collect(toList());

        assertEquals("There should only two demand messages.", 2, demandMsgsForSupplier2.size());
        assertTrue(
            "Both messages should require historical rebalance [" +
                "msg=" + demandMsgsForSupplier2.get(0) + ", msg=" + demandMsgsForSupplier2.get(1) + ']',
                histPred.apply(demandMsgsForSupplier2.get(0)) && histPred.apply(demandMsgsForSupplier2.get(1)));
    }

    /**
     * Tests that owning partitions (that are trigged by rebalance future) cannot be mapped to a new rebalance future
     * that was created by RebalanceReassignExchangeTask.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceReassignAndOwnPartitions() throws Exception {
        backups = 3;

        IgniteEx supplier1 = startGrid(0);
        IgniteEx supplier2 = startGrid(1);
        IgniteEx demander = startGrid(2);

        supplier1.cluster().state(ACTIVE);

        String cacheName1 = "test-cache-1";
        String cacheName2 = "test-cache-2";

        IgniteCache<Integer, IndexedObject> c1 = supplier1.getOrCreateCache(
            new CacheConfiguration<Integer, IndexedObject>(cacheName1)
                .setBackups(backups)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT))
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setRebalanceOrder(10));

        IgniteCache<Integer, IndexedObject> c2 = supplier1.getOrCreateCache(
            new CacheConfiguration<Integer, IndexedObject>(cacheName2)
                .setBackups(backups)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT))
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setRebalanceOrder(20));

        // Fill initial data.
        final int entryCnt = PARTS_CNT * 200;
        final int preloadEntryCnt = PARTS_CNT * 400;

        for (int k = 0; k < preloadEntryCnt; k++) {
            c1.put(k, new IndexedObject(k));

            c2.put(k, new IndexedObject(k));
        }

        forceCheckpoint();

        stopGrid(2);

        // Rewrite data to trigger further rebalance.
        // Make sure that all partitions will be updated in order to disable wal locally for preloading.
        // Updating entryCnt keys allows to trigger historical rebalance.
        // This is an easy way to emulate missing partitions on the first rebalance.
        for (int i = 0; i < entryCnt; i++)
            c1.put(i, new IndexedObject(i));

        // Full rebalance for the cacheName2.
        for (int i = 0; i < preloadEntryCnt; i++)
            c2.put(i, new IndexedObject(i));

        // Delay rebalance process for specified groups.
        blockMsgPred = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                return msg0.groupId() == CU.cacheId(cacheName1) || msg0.groupId() == CU.cacheId(cacheName2);
            }

            return false;
        };

        // Emulate missing partitions and trigger RebalanceReassignExchangeTask which should re-trigger a new rebalance.
        FailingIOFactory ioFactory = injectFailingIOFactory(supplier1);

        demander = startGrid(2);

        TestRecordingCommunicationSpi demanderSpi = TestRecordingCommunicationSpi.spi(grid(2));

        // Wait until demander starts rebalancning.
        demanderSpi.waitForBlocked();

        // Need to start a client node in order to block RebalanceReassignExchangeTask (and do not change the affinity)
        // until cacheName2 triggers a checkpoint after rebalancing.
        CountDownLatch blockClientJoin = new CountDownLatch(1);
        CountDownLatch unblockClientJoin = new CountDownLatch(1);

        demander.context().cache().context().exchange().registerExchangeAwareComponent(new PartitionsExchangeAware() {
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                blockClientJoin.countDown();

                try {
                    if (!unblockClientJoin.await(getTestTimeout(), MILLISECONDS))
                        throw new IgniteException("Failed to wait for client node joinning the cluster.");
                }
                catch (InterruptedException e) {
                    throw new IgniteException("Unexpected exception.", e);
                }
            }
        });

        startClientGrid(4);

        // Wait for a checkpoint after rebalancing cacheName2.
        CountDownLatch blockCheckpoint = new CountDownLatch(1);
        CountDownLatch unblockCheckpoint = new CountDownLatch(1);

        ((GridCacheDatabaseSharedManager) demander
            .context()
            .cache()
            .context()
            .database())
            .addCheckpointListener(new CheckpointListener() {
                /** {@inheritDoc} */
                @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                    if (!ctx.progress().reason().contains(String.valueOf(CU.cacheId(cacheName2))))
                        return;

                    blockCheckpoint.countDown();

                    try {
                        if (!unblockCheckpoint.await(getTestTimeout(), MILLISECONDS))
                            throw new IgniteCheckedException("Failed to wait for unblocking checkpointer.");
                    }
                    catch (InterruptedException e) {
                        throw new IgniteCheckedException("Unexpected exception", e);
                    }
                }

                /** {@inheritDoc} */
                @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
                }

                /** {@inheritDoc} */
                @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                }
            });

        // Unblock the first rebalance.
        demanderSpi.stopBlock();

        // Wait for start of the checkpoint after rebalancing cacheName2.
        assertTrue("Failed to wait for checkpoint.", blockCheckpoint.await(getTestTimeout(), MILLISECONDS));

        // Block the second rebalancing.
        demanderSpi.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                return msg0.groupId() == CU.cacheId(cacheName1);
            }

            return false;
        });

        ioFactory.reset();

        // Let's unblock client exchange and, therefore, handling of RebalanceReassignExchangeTask,
        // which is already scheduled.
        unblockClientJoin.countDown();

        // Wait for starting the second rebalance (new chain of rebalance futures should be created at this point).
        demanderSpi.waitForBlocked();

        GridFutureAdapter checkpointFut = ((GridCacheDatabaseSharedManager) demander
            .context()
            .cache()
            .context()
            .database())
            .getCheckpointer()
            .currentProgress()
            .futureFor(FINISHED);

        // Unblock checkpointer.
        unblockCheckpoint.countDown();

        assertTrue(
            "Failed to wait for a checkpoint.",
            GridTestUtils.waitForCondition(() -> checkpointFut.isDone(), getTestTimeout()));

        // Well, there is a race between we unblock rebalance and the current checkpoint executes all its listeners.
        demanderSpi.stopBlock();

        awaitPartitionMapExchange(false, true, null);
    }

    /**
     * Injects a new instance of FailingIOFactory into wal manager for the given supplier node.
     * This allows to break historical rebalance from the supplier.
     *
     * @param supplier Supplier node to be modified.
     * @return Instance of FailingIOFactory that was injected.
     */
    private static FailingIOFactory injectFailingIOFactory(IgniteEx supplier) {
        // Inject I/O factory which can throw exception during WAL read on supplier1 node.
        FailingIOFactory ioFactory = new FailingIOFactory(new RandomAccessFileIOFactory());

        ((FileWriteAheadLogManager)supplier.context().cache().context().wal()).setFileIOFactory(ioFactory);

        ioFactory.throwExceptionOnWalRead();

        return ioFactory;
    }

    /**
     *
     */
    private static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /** */
        private byte[] payload = new byte[1024];

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof IndexedObject))
                return false;

            IndexedObject that = (IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexedObject.class, this);
        }
    }

    /**
     * Wrapper of communication spi to detect on what topology versions WAL rebalance has happened.
     */
    public static class WalRebalanceCheckingCommunicationSpi extends TestRecordingCommunicationSpi {
        /** (Group ID, Set of topology versions). */
        private static final Map<Integer, Set<Long>> topVers = new HashMap<>();

        /** Lock object. */
        private static final Object mux = new Object();

        /**
         * @param grpId Group ID.
         * @return Set of topology versions where WAL history has been used for rebalance.
         */
        Set<Long> walRebalanceVersions(int grpId) {
            synchronized (mux) {
                return Collections.unmodifiableSet(topVers.getOrDefault(grpId, Collections.emptySet()));
            }
        }

        /**
         * @return All topology versions for all groups where WAL rebalance has been used.
         */
        public static Map<Integer, Set<Long>> allRebalances() {
            synchronized (mux) {
                return Collections.unmodifiableMap(topVers);
            }
        }

        /**
         * Cleans all rebalances history.
         */
        public static void cleanup() {
            synchronized (mux) {
                topVers.clear();
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (((GridIoMessage)msg).message() instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage) ((GridIoMessage)msg).message();

                IgniteDhtDemandedPartitionsMap map = demandMsg.partitions();

                if (!map.historicalMap().isEmpty()) {
                    int grpId = demandMsg.groupId();
                    long topVer = demandMsg.topologyVersion().topologyVersion();

                    synchronized (mux) {
                        topVers.computeIfAbsent(grpId, v -> new HashSet<>()).add(topVer);
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

    /**
     * Check that historical rebalance doesn't start on the cleared partition when some cluster node restarts.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRebalanceRestartWithNodeBlinking() throws Exception {
        backups = 2;

        int entryCnt = PARTS_CNT * 200;

        IgniteEx crd = (IgniteEx)startGridsMultiThreaded(3);

        crd.cluster().active(true);

        IgniteCache<Integer, String> cache0 = crd.cache(CACHE_NAME);

        for (int i = 0; i < entryCnt / 2; i++)
            cache0.put(i, String.valueOf(i));

        forceCheckpoint();

        stopGrid(2);

        for (int i = entryCnt / 2; i < entryCnt; i++)
            cache0.put(i, String.valueOf(i));

        blockMsgPred = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                return msg0.groupId() == CU.cacheId(CACHE_NAME);
            }

            return false;
        };

        startGrid(2);

        TestRecordingCommunicationSpi spi2 = TestRecordingCommunicationSpi.spi(grid(2));

        // Wait until node2 starts historical rebalancning.
        spi2.waitForBlocked(1);

        // Interruption of rebalancing by left supplier, should remap to new supplier with full rebalancing.
        stopGrid(0);

        // Wait until the full rebalance begins with g1 as a supplier.
        spi2.waitForBlocked(2);

        blockMsgPred = null;

        startGrid(0); // Should not force rebalancing remap.

        startGrid(4);
        resetBaselineTopology(); // Should force rebalancing remap.

        spi2.waitForBlocked(3);
        spi2.stopBlock();

        awaitPartitionMapExchange();

        // Verify data on demander node.
        assertPartitionsSame(idleVerify(grid(0), CACHE_NAME));
    }

    /**
     *
     */
    static class FailingIOFactory implements FileIOFactory {
        /** Fail read operations. */
        private volatile boolean failRead;

        /** Delegate. */
        private final FileIOFactory delegate;

        /**
         * @param delegate Delegate.
         */
        FailingIOFactory(FileIOFactory delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegateIO = delegate.create(file, modes);

            if (file.getName().endsWith(".wal") && failRead)
                return new FileIODecorator(delegateIO) {
                    @Override public int read(ByteBuffer destBuf) throws IOException {
                        throw new IOException("Test exception."); // IO exception is required for correct cleanup.
                    }
                };

            return delegateIO;
        }

        /**
         *
         */
        public void throwExceptionOnWalRead() {
            failRead = true;
        }

        /**
         *
         */
        public void reset() {
            failRead = false;
        }
    }

    /** */
    static class RecordedDemandMessage {
        /** Full rebalance. */
        private final boolean full;

        /** Historical rebalance. */
        private final boolean historical;

        /** Supplier node id. */
        private final UUID supplierId;

        /** Group id. */
        private final int grpId;

        /**
         * Creates a new instance.
         * @param supplierId Supplier node id.
         * @param grpId Cache group id.
         * @param full {@code true} if demand message has partitions that should be fully rebalanced.
         * @param historical {@code true} if demand message has partitions that should be wal rebalanced.
         */
        RecordedDemandMessage(UUID supplierId, int grpId, boolean full, boolean historical) {
            this.supplierId = supplierId;
            this.grpId = grpId;
            this.full = full;
            this.historical = historical;
        }

        /**
         * @return Supplier node id.
         */
        UUID supplierId() {
            return supplierId;
        }

        /**
         * @return cache group id.
         */
        int groupId() {
            return grpId;
        }

        /**
         * @return {@code true} if demand message has partitions that should be fully rebalanced.
         */
        boolean hasFull() {
            return full;
        }

        /**
         * @return {@code true} if demand message has partitions that should be wal rebalanced.
         */
        boolean hasHistorical() {
            return historical;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "RecordedDemandMessage{" +
                "supplierId=" + supplierId +
                ", groupId=" + grpId +
                ", full=" + full +
                ", historical=" + historical +
                '}';
        }
    }
}
