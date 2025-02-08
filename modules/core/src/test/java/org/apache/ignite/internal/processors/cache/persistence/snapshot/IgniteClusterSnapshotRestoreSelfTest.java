/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import com.google.common.base.Throwables;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileName;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.TMP_CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Snapshot restore tests.
 */
public class IgniteClusterSnapshotRestoreSelfTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** Type name used for binary and SQL. */
    private static final String TYPE_NAME = "CustomType";

    /** Reset consistent ID flag. */
    private boolean resetConsistentId;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (resetConsistentId)
            cfg.setConsistentId(null);

        return cfg;
    }

    /**
     * Checks snapshot restore if not all "affinity" partitions have been physically created.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestoreWithEmptyPartitions() throws Exception {
        int keysCnt = dfltCacheCfg.getAffinity().partitions() / 2;

        // Skip check because some partitions will be empty - keysCnt == parts/2.
        Ignite ignite = startGridsWithSnapshot(1, keysCnt, false, true);

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), keysCnt);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterSnapshotRestoreFromCustomDir() throws Exception {
        File snpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);

        assertTrue("Target directory is not empty: " + snpDir, F.isEmpty(snpDir.list()));

        try {
            IgniteEx ignite = startGrids(2);
            ignite.cluster().state(ACTIVE);

            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ignite.cache(DEFAULT_CACHE_NAME).put(i, i);

            createAndCheckSnapshot(ignite, SNAPSHOT_NAME, snpDir.toString(), TIMEOUT);

            // Check snapshot.
            IdleVerifyResult res = snp(ignite).checkSnapshot(SNAPSHOT_NAME, snpDir.getAbsolutePath()).get(TIMEOUT)
                .idleVerifyResult();

            StringBuilder sb = new StringBuilder();
            res.print(sb::append, true);

            assertTrue(F.isEmpty(res.exceptions()));
            assertPartitionsSame(res);
            assertContains(log, sb.toString(), "The check procedure has finished, no conflicts have been found");

            ignite.destroyCache(DEFAULT_CACHE_NAME);
            awaitPartitionMapExchange();

            ignite.context().cache().context().snapshotMgr().restoreSnapshot(SNAPSHOT_NAME, snpDir.getAbsolutePath(), null)
                .get();

            IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

            assert cache != null;

            assertSnapshotCacheKeys(cache);
        }
        finally {
            stopAllGrids();

            U.delete(snpDir);
        }
    }

    /**
     * Ensures that system partition verification task is invoked before restoring the snapshot.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRestoreWithMissedPart() throws Exception {
        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        Path part0 = U.searchFileRecursively(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath(),
            getPartitionFileName(0));

        assertNotNull(part0);
        assertTrue(part0.toString(), part0.toFile().exists());
        assertTrue(part0.toFile().delete());

        IgniteFuture<Void> fut = ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null);
        assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), IgniteException.class,
            "Snapshot data doesn't contain required cache group partition");

        ensureCacheAbsent(dfltCacheCfg);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreAllGroups() throws Exception {
        doRestoreAllGroups();
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreAllGroupsWithoutConsistentId() throws Exception {
        resetConsistentId = true;

        doRestoreAllGroups();
    }

    /** @throws Exception If failed. */
    private void doRestoreAllGroups() throws Exception {
        CacheConfiguration<Integer, Object> cacheCfg1 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE1)).setGroupName(SHARED_GRP);

        CacheConfiguration<Integer, Object> cacheCfg2 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE2)).setGroupName(SHARED_GRP);

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(),
            dfltCacheCfg.setBackups(0), cacheCfg1, cacheCfg2);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME, null, TIMEOUT);

        ignite.cache(CACHE1).destroy();
        ignite.cache(CACHE2).destroy();
        ignite.cache(DEFAULT_CACHE_NAME).destroy();

        awaitPartitionMapExchange();

        for (Ignite g : G.allGrids())
            TestRecordingCommunicationSpi.spi(g).record(SnapshotFilesRequestMessage.class);

        // Restore all cache groups.
        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        awaitPartitionMapExchange(true, true, null, true);

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        assertCacheKeys(ignite.cache(CACHE1), CACHE_KEYS_RANGE);
        assertCacheKeys(ignite.cache(CACHE2), CACHE_KEYS_RANGE);

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);

        // Ensure there is no remote snapshot requests occurred.
        for (Ignite g : G.allGrids()) {
            assertTrue("Snapshot files remote requests must not happened due to all the files are available locally",
                TestRecordingCommunicationSpi.spi(g).recordedMessages(true).isEmpty());
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartClusterSnapshotRestoreMultipleThreadsSameNode() throws Exception {
        checkStartClusterSnapshotRestoreMultithreaded(() -> 0);
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartClusterSnapshotRestoreMultipleThreadsDiffNode() throws Exception {
        AtomicInteger nodeIdx = new AtomicInteger();

        checkStartClusterSnapshotRestoreMultithreaded(nodeIdx::getAndIncrement);
    }

    /**
     * @param nodeIdxSupplier Ignite node index supplier.
     */
    private void checkStartClusterSnapshotRestoreMultithreaded(IntSupplier nodeIdxSupplier) throws Exception {
        Ignite ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        AtomicInteger successCnt = new AtomicInteger();
        AtomicInteger failCnt = new AtomicInteger();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                grid(nodeIdxSupplier.getAsInt()).snapshot().restoreSnapshot(
                    SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

                successCnt.incrementAndGet();
            }
            catch (Exception e) {
                assertTrue("Unexpected exception: " + Throwables.getStackTraceAsString(e),
                    X.hasCause(e, "The previous snapshot restore operation was not completed.",
                    IgniteCheckedException.class, IgniteException.class));

                failCnt.incrementAndGet();
            }
        }, 2, "runner");

        fut.get(TIMEOUT);

        assertEquals(1, successCnt.get());
        assertEquals(1, failCnt.get());

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateSnapshotDuringRestore() throws Exception {
        Ignite ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        BlockingCustomMessageDiscoverySpi discoSpi = discoSpi(grid(0));

        discoSpi.block((msg) -> msg instanceof DynamicCacheChangeBatch);

        IgniteFuture<Void> fut =
            ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME));

        discoSpi.waitBlocked(TIMEOUT);

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> snp(grid(1)).createSnapshot("NEW_SNAPSHOT", null, false, onlyPrimary).get(TIMEOUT),
            IgniteException.class,
            "Cache group restore operation is currently in progress."
        );

        discoSpi.unblock();

        fut.get(TIMEOUT);

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /**
     * Ensures that the cache doesn't start if one of the baseline nodes fails.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeLeftDuringCacheStartOnExchangeInit() throws Exception {
        startGridsWithSnapshot(3, CACHE_KEYS_RANGE, true);

        BlockingCustomMessageDiscoverySpi discoSpi = discoSpi(grid(0));

        discoSpi.block((msg) -> msg instanceof DynamicCacheChangeBatch);

        IgniteFuture<Void> fut =
            grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME));

        discoSpi.waitBlocked(TIMEOUT);

        stopGrid(2, true);

        discoSpi.unblock();

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), ClusterTopologyCheckedException.class, null);

        ensureCacheAbsent(dfltCacheCfg);

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED);
    }

    /**
     * Ensures that the cache is not started if non-coordinator node left during the exchange.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeLeftDuringCacheStartOnExchangeFinish() throws Exception {
        checkNodeLeftOnExchangeFinish(
            false, ClusterTopologyCheckedException.class, "Required node has left the cluster");
    }

    /**
     * Ensures that the cache is not started if the coordinator left during the exchange.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCrdLeftDuringCacheStartOnExchangeFinish() throws Exception {
        checkNodeLeftOnExchangeFinish(
            true, IgniteCheckedException.class, "Operation has been cancelled (node is stopping)");
    }

    /**
     * @param crdStop {@code True} to stop coordinator node.
     * @param expCls Expected exception class.
     * @param expMsg Expected exception message.
     * @throws Exception If failed.
     */
    private void checkNodeLeftOnExchangeFinish(
        boolean crdStop,
        Class<? extends Throwable> expCls,
        String expMsg
    ) throws Exception {
        startGridsWithSnapshot(3, CACHE_KEYS_RANGE, true);

        TestRecordingCommunicationSpi node1spi = TestRecordingCommunicationSpi.spi(grid(1));
        TestRecordingCommunicationSpi node2spi = TestRecordingCommunicationSpi.spi(grid(2));

        node1spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionsSingleMessage);
        node2spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionsSingleMessage);

        IgniteFuture<Void> fut =
            grid(1).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME));

        node1spi.waitForBlocked();
        node2spi.waitForBlocked();

        stopGrid(crdStop ? 0 : 2, true);

        node1spi.stopBlock();

        if (crdStop)
            node2spi.stopBlock();

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), expCls, expMsg);

        awaitPartitionMapExchange();

        ensureCacheAbsent(dfltCacheCfg);
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotRestoreRejectOnInActiveCluster() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME, null, TIMEOUT);

        ignite.cluster().state(ClusterState.INACTIVE);

        IgniteFuture<Void> fut =
            ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(TIMEOUT), IgniteException.class, "The cluster should be active");

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED);
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotRestoreOnSmallerTopology() throws Exception {
        startGridsWithSnapshot(2, CACHE_KEYS_RANGE, true);

        stopGrid(1);

        resetBaselineTopology();

        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

        assertCacheKeys(grid(0).cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreSharedCacheGroup() throws Exception {
        CacheConfiguration<Integer, Object> cacheCfg1 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE1)).setGroupName(SHARED_GRP);

        CacheConfiguration<Integer, Object> cacheCfg2 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE2)).setGroupName(SHARED_GRP);

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), cacheCfg1, cacheCfg2);

        ignite.cluster().state(ClusterState.ACTIVE);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME, null, TIMEOUT);

        ignite.cache(CACHE1).destroy();

        awaitPartitionMapExchange();

        locEvts.clear();

        IgniteSnapshot snp = ignite.snapshot();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> snp.restoreSnapshot(SNAPSHOT_NAME, Arrays.asList(CACHE1, CACHE2)).get(TIMEOUT),
            IllegalArgumentException.class,
            "Cache group(s) was not found in the snapshot"
        );

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED);
        assertEquals(2, locEvts.size());

        locEvts.clear();

        ignite.cache(CACHE2).destroy();

        awaitPartitionMapExchange();

        snp.restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(SHARED_GRP)).get(TIMEOUT);

        assertCacheKeys(ignite.cache(CACHE1), CACHE_KEYS_RANGE);
        assertCacheKeys(ignite.cache(CACHE2), CACHE_KEYS_RANGE);

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);
        assertEquals(2, locEvts.size());
    }

    /** @throws Exception If failed. */
    @Test
    public void testIncompatibleMetasUpdate() throws Exception {
        valBuilder = new BinaryValueBuilder(TYPE_NAME);

        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        int typeId = ignite.context().cacheObjects().typeId(TYPE_NAME);

        ignite.context().cacheObjects().removeType(typeId);

        BinaryObject[] objs = new BinaryObject[CACHE_KEYS_RANGE];

        IgniteCache<Integer, Object> cache1 = createCacheWithBinaryType(ignite, "cache1", n -> {
            BinaryObjectBuilder builder = ignite.binary().builder(TYPE_NAME);

            builder.setField("id", n);

            objs[n] = builder.build();

            return objs[n];
        });

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

        // Ensure that existing type has been updated.
        BinaryType type = ignite.context().cacheObjects().metadata(typeId);

        assertTrue(type.fieldNames().contains("name"));

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            assertEquals(objs[i], cache1.get(i));

        cache1.destroy();

        grid(0).cache(DEFAULT_CACHE_NAME).destroy();

        ignite.context().cacheObjects().removeType(typeId);

        // Create cache with incompatible binary type.
        cache1 = createCacheWithBinaryType(ignite, "cache1", n -> {
            BinaryObjectBuilder builder = ignite.binary().builder(TYPE_NAME);

            builder.setField("id", UUID.randomUUID());

            objs[n] = builder.build();

            return objs[n];
        });

        IgniteFuture<Void> fut0 =
            ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut0.get(TIMEOUT), BinaryObjectException.class, null);

        ensureCacheAbsent(dfltCacheCfg);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            assertEquals(objs[i], cache1.get(i));
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param valBuilder Binary value builder.
     * @return Created cache.
     */
    private IgniteCache<Integer, Object> createCacheWithBinaryType(
        Ignite ignite,
        String cacheName,
        Function<Integer, BinaryObject> valBuilder
    ) {
        IgniteCache<Integer, Object> cache = ignite.createCache(new CacheConfiguration<>(cacheName)).withKeepBinary();

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            cache.put(i, valBuilder.apply(i));

        return cache;
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testParallelCacheStartWithTheSameNameOnPrepare() throws Exception {
        checkCacheStartWithTheSameName(RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD, IgniteCheckedException.class,
            "Cache start failed. A cache or group with the same name is currently being restored from a snapshot");
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testParallelCacheStartWithTheSameNameOnStart() throws Exception {
        checkCacheStartWithTheSameName(RESTORE_CACHE_GROUP_SNAPSHOT_START, CacheExistsException.class,
            "Failed to start cache (a cache with the same name is already started):");
    }

    /**
     * @param procType The type of distributed process on which communication is blocked.
     * @throws Exception if failed.
     */
    private void checkCacheStartWithTheSameName(
        DistributedProcessType procType,
        Class<? extends Throwable> expCls,
        String expMsg
    ) throws Exception {
        dfltCacheCfg = txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE1)).setGroupName(SHARED_GRP);

        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, procType, SHARED_GRP);

        GridTestUtils.assertThrowsAnyCause(log, () -> ignite.createCache(SHARED_GRP), IgniteCheckedException.class, null);

        GridTestUtils.assertThrowsAnyCause(log, () -> ignite.createCache(CACHE1), expCls, expMsg);

        spi.stopBlock();

        fut.get(TIMEOUT);

        assertCacheKeys(grid(0).cache(CACHE1), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailDuringRestore() throws Exception {
        startGridsWithSnapshot(4, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(3));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD, DEFAULT_CACHE_NAME);

        IgniteInternalFuture<?> fut0 = runAsync(() -> stopGrid(3, true));

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> fut.get(TIMEOUT),
            ClusterTopologyCheckedException.class,
            "Required node has left the cluster"
        );

        fut0.get(TIMEOUT);

        awaitPartitionMapExchange();

        ensureCacheAbsent(dfltCacheCfg);

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED);

        dfltCacheCfg = null;

        // Should start successfully.
        Ignite ignite = startGrid(3);

        resetBaselineTopology();
        awaitPartitionMapExchange();

        assertNull(ignite.cache(DEFAULT_CACHE_NAME));
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailDuringFilesCopy() throws Exception {
        dfltCacheCfg.setCacheMode(CacheMode.REPLICATED)
            .setAffinity(new RendezvousAffinityFunction());

        startGridsWithSnapshot(3, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(2));
        CountDownLatch stopLatch = new CountDownLatch(1);

        spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage &&
            ((SingleNodeMessage<?>)msg).type() == RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD.ordinal());

        String failingFilePath = Paths.get(NodeFileTree.cacheStorageName(false, DEFAULT_CACHE_NAME),
            PART_FILE_PREFIX + (dfltCacheCfg.getAffinity().partitions() / 2) + FILE_SUFFIX).toString();

        grid(2).context().cache().context().snapshotMgr().ioFactory(
            new CustomFileIOFactory(new RandomAccessFileIOFactory(),
                file -> {
                    if (file.getPath().endsWith(failingFilePath)) {
                        stopLatch.countDown();

                        throw new RuntimeException("Test exception");
                    }
                }));

        File node2dbDir = grid(2).context().pdsFolderResolver().fileTree().cacheStorage(dfltCacheCfg).getParentFile();

        IgniteInternalFuture<Object> stopFut = runAsync(() -> {
            U.await(stopLatch, TIMEOUT, TimeUnit.MILLISECONDS);

            stopGrid(2, true);

            return null;
        });

        IgniteFuture<Void> fut =
            grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME));

        stopFut.get(TIMEOUT);

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), ClusterTopologyCheckedException.class, null);

        File[] files = node2dbDir.listFiles(file -> file.getName().startsWith(TMP_CACHE_DIR_PREFIX));
        assertEquals("A temp directory with potentially corrupted files must exist.", 1, files.length);

        ensureCacheAbsent(dfltCacheCfg);

        dfltCacheCfg = null;

        startGrid(2);

        files = node2dbDir.listFiles(file -> file.getName().startsWith(TMP_CACHE_DIR_PREFIX));
        assertEquals("A temp directory should be removed at node startup", 0, files.length);

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeJoinDuringRestore() throws Exception {
        Ignite ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE, DEFAULT_CACHE_NAME);

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGrid(2),
            IgniteSpiException.class,
            "Joining node during caches restore is not allowed"
        );

        spi.stopBlock();

        fut.get(TIMEOUT);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        assertTrue(cache.indexReadyFuture().isDone());

        assertCacheKeys(cache, CACHE_KEYS_RANGE);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testClusterStateChangeActiveReadonlyOnPrepare() throws Exception {
        checkClusterStateChange(ClusterState.ACTIVE_READ_ONLY, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE,
            IgniteException.class, "Failed to perform start cache operation (cluster is in read-only mode)");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testClusterStateChangeActiveReadonlyOnCacheStart() throws Exception {
        checkClusterStateChange(ClusterState.ACTIVE_READ_ONLY, RESTORE_CACHE_GROUP_SNAPSHOT_START, null, null);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testClusterDeactivateOnPrepare() throws Exception {
        checkClusterStateChange(ClusterState.INACTIVE, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE,
            IgniteException.class, "The cluster has been deactivated.");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testClusterDeactivateOnCacheStart() throws Exception {
        checkClusterStateChange(ClusterState.INACTIVE, RESTORE_CACHE_GROUP_SNAPSHOT_START, null, null);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testUserClassRestored() throws Exception {
        int nodes = 3;
        int keysCnt = 1_000;

        valBuilder = Account::new;

        // Skip check because some partitions will be empty - keysCnt < parts.
        startGridsWithSnapshot(nodes, keysCnt, false, true);

        stopAllGrids();

        cleanPersistenceDir(true);

        dfltCacheCfg = null;

        IgniteEx ign = startGrids(nodes);

        ign.cluster().state(ClusterState.ACTIVE);

        GridTestUtils.waitForCondition(() -> {
            for (int n = 0; n < nodes; n++) {
                if (grid(n).context().state().clusterState().transition())
                    return false;
            }

            return true;
        }, getTestTimeout());

        ign.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

        assertCacheKeys(ign.cache(DEFAULT_CACHE_NAME), keysCnt);
    }

    /**
     * @param state Cluster state.
     * @param procType The type of distributed process on which communication is blocked.
     * @param exCls Expected exception class.
     * @param expMsg Expected exception message.
     * @throws Exception if failed.
     */
    private void checkClusterStateChange(
        ClusterState state,
        DistributedProcessType procType,
        @Nullable Class<? extends Throwable> exCls,
        @Nullable String expMsg
    ) throws Exception {
        int nodesCnt = 2;

        Ignite ignite = startGridsWithSnapshot(nodesCnt, CACHE_KEYS_RANGE, true);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(nodesCnt - 1));

        locEvts.clear();

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, procType, DEFAULT_CACHE_NAME);

        ignite.cluster().state(state);

        spi.stopBlock();

        if (exCls == null) {
            fut.get(TIMEOUT);

            ignite.cluster().state(ClusterState.ACTIVE);

            assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);

            waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);

            return;
        }

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), exCls, expMsg);

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED);

        assertEquals(2, locEvts.size());

        ignite.cluster().state(ClusterState.ACTIVE);

        ensureCacheAbsent(dfltCacheCfg);

        String cacheName = DEFAULT_CACHE_NAME;

        grid(nodesCnt - 1).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(cacheName)).get(TIMEOUT);

        assertCacheKeys(ignite.cache(cacheName), CACHE_KEYS_RANGE);
    }

    /**
     * Custom I/O factory to preprocessing created files.
     */
    private static class CustomFileIOFactory implements FileIOFactory {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Delegate factory. */
        private final FileIOFactory delegate;

        /** Preprocessor for created files. */
        private final Consumer<File> hnd;

        /**
         * @param delegate Delegate factory.
         * @param hnd Preprocessor for created files.
         */
        public CustomFileIOFactory(FileIOFactory delegate, Consumer<File> hnd) {
            this.delegate = delegate;
            this.hnd = hnd;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = this.delegate.create(file, modes);

            hnd.accept(file);

            return delegate;
        }
    }

    /** */
    public static class Account {
        /** */
        private final int id;

        /** */
        Account(int id) {
            this.id = id;
        }

        /** */
        @Override public int hashCode() {
            return id;
        }

        /** */
        @Override public boolean equals(Object obj) {
            return id == ((Account)obj).id;
        }
    }
}
