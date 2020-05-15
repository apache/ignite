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
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Historical WAL rebalance base test.
 */
public class IgniteWalRebalanceTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Partitions count. */
    private static final int PARTS_CNT = 32;

    /** Block message predicate to set to Communication SPI in node configuration. */
    private IgniteBiPredicate<ClusterNode, Message> blockMessagePredicate;

    /** */
    private int backups;

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
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
            );

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setCommunicationSpi(new WalRebalanceCheckingCommunicationSpi());

        if (blockMessagePredicate != null) {
            TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi) cfg.getCommunicationSpi();

            spi.blockMessages(blockMessagePredicate);
        }

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

        ig0.cluster().active(true);

        IgniteCache<Object, Object> cache = ig0.cache(CACHE_NAME);

        for (int k = 0; k < entryCnt; k++)
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

        ig0.cluster().active(true);

        IgniteCache<Object, Object> cache = ig0.cache(CACHE_NAME);

        for (int k = 0; k < entryCnt; k++)
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

        crd.cluster().active(true);

        final int entryCnt = PARTS_CNT * 10;

        {
            IgniteCache<Object, Object> cache = crd.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++)
                cache.put(k, new IndexedObject(k - 1));
        }

        forceCheckpoint();

        stopAllGrids();

        IgniteEx ig0 = startGrids(2);

        ig0.cluster().active(true);

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
        IgniteEx crd = (IgniteEx) startGrids(3);

        crd.cluster().active(true);

        final int entryCnt = PARTS_CNT * 10;

        {
            IgniteCache<Object, Object> cache = crd.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++)
                cache.put(k, new IndexedObject(k - 1));
        }

        forceCheckpoint();

        stopAllGrids();

        // Rewrite data with globally disabled WAL.
        crd = (IgniteEx) startGrids(2);

        crd.cluster().active(true);

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
        IgniteEx crd = (IgniteEx) startGrids(3);

        crd.cluster().active(true);

        final int entryCnt = PARTS_CNT * 10;

        {
            IgniteCache<Object, Object> cache = crd.cache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++)
                cache.put(k, new IndexedObject(k - 1));
        }

        forceCheckpoint();

        stopAllGrids();

        // Rewrite data to trigger further rebalance.
        IgniteEx supplierNode = (IgniteEx) startGrid(0);

        supplierNode.cluster().active(true);

        IgniteCache<Object, Object> cache = supplierNode.cache(CACHE_NAME);

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, new IndexedObject(k));

        forceCheckpoint();

        final int groupId = supplierNode.cachex(CACHE_NAME).context().groupId();

        // Delay rebalance process for specified group.
        blockMessagePredicate = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage)
                return ((GridDhtPartitionDemandMessage) msg).groupId() == groupId;

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
        FailingIOFactory ioFactory = new FailingIOFactory(new RandomAccessFileIOFactory());

        ((FileWriteAheadLogManager) supplierNode.cachex(CACHE_NAME).context().shared().wal()).setFileIOFactory(ioFactory);

        ioFactory.throwExceptionOnWalRead();

        // Resume rebalance process.
        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi) demanderNode.configuration().getCommunicationSpi();

        spi.stopBlock();

        // Wait till rebalance will be failed and cancelled.
        Boolean result = preloader.rebalanceFuture().get();

        Assert.assertEquals("Rebalance should be cancelled on demander node: " + preloader.rebalanceFuture(), false, result);

        // Stop blocking messages and fail WAL during read.
        blockMessagePredicate = null;

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

        blockMessagePredicate = (node, msg) -> {
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

        blockMessagePredicate = null;

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
}
