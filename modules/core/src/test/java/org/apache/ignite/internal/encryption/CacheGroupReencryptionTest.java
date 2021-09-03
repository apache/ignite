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

package org.apache.ignite.internal.encryption;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.EncryptionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.configuration.EncryptionConfiguration.DFLT_REENCRYPTION_RATE_MBPS;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.internal.managers.encryption.GridEncryptionManager.INITIAL_KEY_ID;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Cache re-encryption tests.
 */
public class CacheGroupReencryptionTest extends AbstractEncryptionTest {
    /** */
    private static final String GRID_2 = "grid-2";

    /** */
    private static final String GRID_3 = "grid-3";

    /** Timeout. */
    private static final long MAX_AWAIT_MILLIS = 15_000;

    /** File IO fail flag. */
    private final AtomicBoolean failFileIO = new AtomicBoolean();

    /** Count of cache backups. */
    private int backups;

    /** Re-encryption rate limit. */
    private double pageScanRate = DFLT_REENCRYPTION_RATE_MBPS;

    /** The number of pages that is scanned during re-encryption under checkpoint lock. */
    private int pageScanBatchSize = EncryptionConfiguration.DFLT_REENCRYPTION_BATCH_SIZE;

    /** Checkpoint frequency (seconds). */
    private long checkpointFreq = 30;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_STOPPED);

        EncryptionConfiguration encCfg = new EncryptionConfiguration()
            .setReencryptionBatchSize(pageScanBatchSize)
            .setReencryptionRateLimit(pageScanRate);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(1024 * 1024 * 1024L)
                    .setPersistenceEnabled(true))
            .setPageSize(4 * 1024)
            .setWalSegmentSize(10 * 1024 * 1024)
            .setWalSegments(4)
            .setMaxWalArchiveSize(100 * 1024 * 1024L)
            .setCheckpointFrequency(TimeUnit.SECONDS.toMillis(checkpointFreq))
            .setWalMode(LOG_ONLY)
            .setFileIOFactory(new FailingFileIOFactory(new RandomAccessFileIOFactory(), failFileIO))
            .setEncryptionConfiguration(encCfg);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> cacheConfiguration(String name, String grp) {
        CacheConfiguration<K, V> cfg = super.cacheConfiguration(name, grp);

        cfg.setIndexedTypes(Long.class, IndexedObject.class);

        return cfg.setAffinity(new RendezvousAffinityFunction(false, 16)).setBackups(backups);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected Object generateValue(long id) {
        return new IndexedObject(id, "string-" + id);
    }

    /**
     * Check physical recovery after checkpoint failure during re-encryption.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPhysicalRecovery() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> loadData(50_000));

        forceCheckpoint();

        enableCheckpoints(nodes.get1(), false);
        enableCheckpoints(nodes.get2(), false);

        int grpId = CU.cacheId(cacheName());

        failFileIO.set(true);

        nodes.get1().encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

        fut.get();

        assertThrowsAnyCause(log, () -> {
            enableCheckpoints(grid(GRID_0), true);
            enableCheckpoints(grid(GRID_1), true);

            forceCheckpoint();

            return null;
        }, IgniteCheckedException.class, null);

        stopAllGrids(true);

        failFileIO.set(false);

        nodes = startTestGrids(false);

        checkEncryptedCaches(nodes.get1(), nodes.get2());

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /** @throws Exception If failed. */
    @Test
    public void testPhysicalRecoveryWithUpdates() throws Exception {
        pageScanRate = 1.5;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        loadData(50_000);

        IgniteInternalFuture<?> addFut = GridTestUtils.runAsync(() -> loadData(100_000));

        IgniteInternalFuture<?> updateFut = GridTestUtils.runAsync(() -> {
            IgniteCache<Long, String> cache = grid(GRID_0).cache(cacheName());

            while (!Thread.currentThread().isInterrupted()) {
                for (long i = 50_000; i > 20_000; i--) {
                    String val = cache.get(i);

                    cache.put(i, val);
                }
            }
        });

        forceCheckpoint();

        int grpId = CU.cacheId(cacheName());

        nodes.get1().encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        forceCheckpoint();

        enableCheckpoints(G.allGrids(), false);

        failFileIO.set(true);

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

        addFut.get();
        updateFut.cancel();

        assertThrowsAnyCause(log, () -> {
            enableCheckpoints(G.allGrids(), true);

            forceCheckpoint();

            return null;
        }, IgniteCheckedException.class, null);

        stopAllGrids(true);

        failFileIO.set(false);

        nodes = startTestGrids(false);

        checkEncryptedCaches(nodes.get1(), nodes.get2());

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * Ensures that re-encryption continues after a restart.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testLogicalRecovery() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null, true);

        loadData(100_000);

        forceCheckpoint();

        enableCheckpoints(G.allGrids(), false);

        int grpId = CU.cacheId(cacheName());

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

        assertEquals(1, node0.context().encryption().getActiveKey(grpId).id());
        assertEquals(1, node1.context().encryption().getActiveKey(grpId).id());

        stopAllGrids();

        info(">>> Start grids (iteration 1)");

        startTestGrids(false);

        enableCheckpoints(G.allGrids(), false);

        stopAllGrids();

        info(">>> Start grids (iteration 2)");

        startTestGrids(false);

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStopDuringReencryption() throws Exception {
        pageScanRate = 1;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        loadData(100_000);

        IgniteCache<?, ?> cache = node0.cache(cacheName());

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        int grpId = CU.cacheId(cacheName());

        IgniteInternalFuture<Void> fut0 = node0.context().encryption().reencryptionFuture(grpId);

        assertFalse(fut0.isDone());

        assertTrue(isReencryptionInProgress(node0, grpId));

        cache.destroy();

        assertThrowsAnyCause(log, () -> {
            fut0.get();

            return null;
        }, IgniteFutureCancelledCheckedException.class, null);

        awaitPartitionMapExchange();

        assertNull(node0.context().encryption().groupKeyIds(grpId));
        assertNull(node1.context().encryption().groupKeyIds(grpId));
    }

    /** @throws Exception If failed. */
    @Test
    public void testPartitionEvictionDuringReencryption() throws Exception {
        backups = 1;
        pageScanRate = 1;

        CountDownLatch rebalanceFinished = new CountDownLatch(1);

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        loadData(100_000);

        IgniteEx node2 = startGrid(GRID_2);

        node2.events().localListen(evt -> {
            rebalanceFinished.countDown();

            return true;
        }, EventType.EVT_CACHE_REBALANCE_STOPPED);

        resetBaselineTopology();

        rebalanceFinished.await();

        stopGrid(GRID_2);

        resetBaselineTopology();

        int grpId = CU.cacheId(cacheName());

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        stopAllGrids();

        pageScanRate = DFLT_REENCRYPTION_RATE_MBPS;

        startTestGrids(false);

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * Test that partition files are reused correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionFileDestroy() throws Exception {
        backups = 1;
        pageScanRate = 0.2;
        pageScanBatchSize = 10;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        loadData(50_000);

        forceCheckpoint();

        nodes.get1().encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        startGrid(GRID_2);

        // Trigger partitions eviction.
        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        forceCheckpoint();

        assertTrue(isReencryptionInProgress(Collections.singleton(cacheName())));

        // Set unlimited re-encryption rate.
        nodes.get1().context().encryption().setReencryptionRate(0);
        nodes.get2().context().encryption().setReencryptionRate(0);

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * Test that partition files are reused correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionFileDestroyAndRecreate() throws Exception {
        backups = 1;
        pageScanRate = 1;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        loadData(50_000);

        grid(GRID_0).encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        long walSegment = nodes.get1().context().cache().context().wal().currentSegment();

        for (long n = 0; n <= walSegment; n++)
            nodes.get1().context().encryption().onWalSegmentRemoved(n);

        walSegment = nodes.get2().context().cache().context().wal().currentSegment();

        for (long n = 0; n <= walSegment; n++)
            nodes.get2().context().encryption().onWalSegmentRemoved(n);

        // Force checkpoint to prevent logical recovery after key rotation.
        forceCheckpoint();

        startGrid(GRID_2);

        // Trigger partitions eviction.
        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        // Trigger partitions re-create.
        stopGrid(GRID_2);

        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        stopAllGrids();

        nodes = startTestGrids(false);

        checkEncryptedCaches(nodes.get1(), nodes.get2());

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotBltNodeJoin() throws Exception {
        backups = 1;
        pageScanRate = 1;
        pageScanBatchSize = 10;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        createEncryptedCache(nodes.get1(), nodes.get2(), cacheName(), null);

        loadData(50_000);

        forceCheckpoint();

        long startIdx1 = nodes.get1().context().cache().context().wal().currentSegment();
        long startIdx2 = nodes.get2().context().cache().context().wal().currentSegment();

        nodes.get1().encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        long endIdx1 = nodes.get1().context().cache().context().wal().currentSegment();
        long endIdx2 = nodes.get2().context().cache().context().wal().currentSegment();

        stopGrid(GRID_1);

        resetBaselineTopology();

        int grpId = CU.cacheId(cacheName());

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        startGrid(GRID_1);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        assertEquals(2, grid(GRID_0).context().encryption().groupKeyIds(grpId).size());
        assertEquals(2, grid(GRID_1).context().encryption().groupKeyIds(grpId).size());

        // Simulate that wal was removed.
        for (long segment = startIdx1; segment <= endIdx1; segment++)
            grid(GRID_0).context().encryption().onWalSegmentRemoved(segment);

        checkKeysCount(grid(GRID_0), grpId, 1, MAX_AWAIT_MILLIS);

        for (long segment = startIdx2; segment <= endIdx2; segment++)
            grid(GRID_1).context().encryption().onWalSegmentRemoved(segment);

        checkKeysCount(grid(GRID_1), grpId, 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReencryptionStartsAfterNodeRestart() throws Exception {
        pageScanRate = 0.000000001;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        forceCheckpoint();

        int grpId = CU.cacheId(cacheName());

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        forceCheckpoint();

        stopAllGrids();

        nodes = startTestGrids(false);

        node0 = nodes.get1();
        node1 = nodes.get2();

        assertTrue(isReencryptionInProgress(node0, grpId));
        assertTrue(isReencryptionInProgress(node1, grpId));

        stopAllGrids();

        pageScanRate = DFLT_REENCRYPTION_RATE_MBPS;

        startTestGrids(false);

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReencryptionOnUnstableTopology() throws Exception {
        backups = 1;
        pageScanRate = 2;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        startGrid(GRID_2);
        startGrid(GRID_3);

        resetBaselineTopology();

        createEncryptedCache(node0, node1, cacheName(), null);

        String cache2 = "encrypted-2";

        createEncryptedCache(node0, node1, cache2, null);

        loadData(cacheName(), 100_000);
        loadData(cache2, 100_000);

        List<String> cacheGroups = Arrays.asList(cacheName(), cache2);

        node0.encryption().changeCacheGroupKey(cacheGroups).get();

        while (isReencryptionInProgress(cacheGroups)) {
            int rndNode = ThreadLocalRandom.current().nextInt(3);

            String gridName = "grid-" + rndNode;

            stopGrid(gridName);

            startGrid(gridName);
        }

        stopAllGrids();

        startGrid(GRID_0);
        startGrid(GRID_1);
        startGrid(GRID_2);
        startGrid(GRID_3);

        grid(GRID_0).cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
        checkGroupKey(CU.cacheId(cache2), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivation() throws Exception {
        pageScanRate = 1;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        loadData(100_000);

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        int grpId = CU.cacheId(cacheName());

        assertFalse("Re-encryption must be started.", node0.context().encryption().reencryptionFuture(grpId).isDone());
        assertFalse("Re-encryption must be started.", node1.context().encryption().reencryptionFuture(grpId).isDone());

        node0.cluster().state(ClusterState.INACTIVE);

        // Check node join to inactive cluster.
        stopGrid(GRID_1);
        node1 = startGrid(GRID_1);

        assertTrue("Re-encryption should not start ", node0.context().encryption().reencryptionFuture(grpId).isDone());
        assertTrue("Re-encryption should not start ", node1.context().encryption().reencryptionFuture(grpId).isDone());

        node0.context().encryption().setReencryptionRate(DFLT_REENCRYPTION_RATE_MBPS);
        node1.context().encryption().setReencryptionRate(DFLT_REENCRYPTION_RATE_MBPS);

        node0.cluster().state(ClusterState.ACTIVE);

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testChangeBaseline() throws Exception {
        backups = 1;
        pageScanRate = 2;
        checkpointFreq = 10;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        loadData(100_000);

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        assertTrue(isReencryptionInProgress(Collections.singleton(cacheName())));

        startGrid(GRID_2);

        resetBaselineTopology();

        startGrid(GRID_3);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        stopGrid(GRID_2);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 2, MAX_AWAIT_MILLIS);

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        startGrid(GRID_2);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        checkGroupKey(CU.cacheId(cacheName()), INITIAL_KEY_ID + 3, MAX_AWAIT_MILLIS);
    }

    /** @throws Exception If failed. */
    @Test
    public void testKeyCleanup() throws Exception {
        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        forceCheckpoint();

        enableCheckpoints(G.allGrids(), false);

        int grpId = CU.cacheId(cacheName());

        long startIdx = node1.context().cache().context().wal().currentSegment();

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        long endIdx = node1.context().cache().context().wal().currentSegment();

        awaitEncryption(G.allGrids(), grpId, MAX_AWAIT_MILLIS);

        // Simulate that wal was removed.
        for (long segment = startIdx; segment <= endIdx; segment++)
            node1.context().encryption().onWalSegmentRemoved(segment);

        stopGrid(GRID_1);

        node1 = startGrid(GRID_1);

        enableCheckpoints(G.allGrids(), true);

        node1.cluster().state(ClusterState.ACTIVE);

        node1.resetLostPartitions(Collections.singleton(ENCRYPTED_CACHE));

        checkEncryptedCaches(node0, node1);

        checkGroupKey(grpId, INITIAL_KEY_ID + 1, MAX_AWAIT_MILLIS);
    }

    /** @throws Exception If failed. */
    @Test
    public void testReencryptionMetrics() throws Exception {
        pageScanRate = 0.000000001;

        T2<IgniteEx, IgniteEx> nodes = startTestGrids(true);

        IgniteEx node0 = nodes.get1();
        IgniteEx node1 = nodes.get2();

        createEncryptedCache(node0, node1, cacheName(), null);

        node0.encryption().changeCacheGroupKey(Collections.singleton(cacheName())).get();

        validateMetrics(node0, false);
        validateMetrics(node1, false);

        forceCheckpoint();

        pageScanRate = DFLT_REENCRYPTION_RATE_MBPS;

        stopAllGrids();

        nodes = startTestGrids(false);

        node0 = nodes.get1();
        node1 = nodes.get2();

        awaitEncryption(G.allGrids(), CU.cacheId(cacheName()), MAX_AWAIT_MILLIS);

        forceCheckpoint();

        validateMetrics(node0, true);
        validateMetrics(node1, true);
    }

    /**
     * @param node Grid.
     * @param finished Expected reencryption status.
     */
    private void validateMetrics(IgniteEx node, boolean finished) {
        MetricRegistry registry =
            node.context().metric().registry(metricName(CacheGroupMetricsImpl.CACHE_GROUP_METRICS_PREFIX, cacheName()));

        LongMetric bytesLeft = registry.findMetric("ReencryptionBytesLeft");

        if (finished)
            assertEquals(0, bytesLeft.value());
        else
            assertTrue(bytesLeft.value() > 0);

        BooleanMetric reencryptionFinished = registry.findMetric("ReencryptionFinished");

        assertEquals(finished, reencryptionFinished.value());
    }

    /**
     * @param cacheGroups Cache group names.
     * @return {@code True} If reencryption of the specified groups is not yet complete.
     */
    private boolean isReencryptionInProgress(Iterable<String> cacheGroups) {
        for (Ignite node : G.allGrids()) {
            for (String groupName : cacheGroups) {
                if (isReencryptionInProgress((IgniteEx)node, CU.cacheId(groupName)))
                    return true;
            }
        }

        return false;
    }

    /** */
    private static final class FailingFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegateFactory;

        /** */
        private final AtomicBoolean failFlag;

        /**
         * @param factory Delegate factory.
         */
        FailingFileIOFactory(FileIOFactory factory, AtomicBoolean failFlag) {
            delegateFactory = factory;

            this.failFlag = failFlag;
        }

        /** {@inheritDoc}*/
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            return new FailingFileIO(delegate);
        }

        /** */
        final class FailingFileIO extends FileIODecorator {
            /**
             * @param delegate File I/O delegate
             */
            public FailingFileIO(FileIO delegate) {
                super(delegate);
            }

            /** {@inheritDoc} */
            @Override public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
                if (failFlag.get())
                    throw new IOException("Test exception.");

                return delegate.writeFully(srcBuf, position);
            }
        }
    }

    /** */
    private static class IndexedObject {
        /** Id. */
        @QuerySqlField(index = true)
        private final long id;

        /** Name. */
        @QuerySqlField(index = true)
        private final String name;

        /**
         * @param id Id.
         */
        public IndexedObject(long id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            IndexedObject obj = (IndexedObject)o;

            return id == obj.id && Objects.equals(name, obj.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name, id);
        }
    }
}
