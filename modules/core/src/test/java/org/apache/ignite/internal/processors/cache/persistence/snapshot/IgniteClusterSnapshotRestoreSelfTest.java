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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.TMP_CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Snapshot restore tests.
 */
public class IgniteClusterSnapshotRestoreSelfTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** Type name used for binary and SQL. */
    private static final String TYPE_NAME = "CustomType";

    /** Cache 1 name. */
    private static final String CACHE1 = "cache1";

    /** Cache 2 name. */
    private static final String CACHE2 = "cache2";

    /** Default shared cache group name. */
    private static final String SHARED_GRP = "shared";

    /** Cache value builder. */
    private Function<Integer, Object> valBuilder = String::valueOf;

    /** {@inheritDoc} */
    @Override protected Function<Integer, Object> valueBuilder() {
        return valBuilder;
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreAllGroups() throws Exception {
        CacheConfiguration<Integer, Object> cacheCfg1 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE1)).setGroupName(SHARED_GRP);

        CacheConfiguration<Integer, Object> cacheCfg2 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE2)).setGroupName(SHARED_GRP);

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valBuilder,
            dfltCacheCfg.setBackups(0), cacheCfg1, cacheCfg2);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cache(CACHE1).destroy();
        ignite.cache(CACHE2).destroy();
        ignite.cache(DEFAULT_CACHE_NAME).destroy();

        awaitPartitionMapExchange();

        // Restore all cache groups.
        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        assertCacheKeys(ignite.cache(CACHE1), CACHE_KEYS_RANGE);
        assertCacheKeys(ignite.cache(CACHE2), CACHE_KEYS_RANGE);
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
                nodeIdxSupplier.getAsInt();

                grid(nodeIdxSupplier.getAsInt()).snapshot().restoreSnapshot(
                    SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

                successCnt.incrementAndGet();
            }
            catch (Exception e) {
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
            () -> grid(1).snapshot().createSnapshot("NEW_SNAPSHOT").get(TIMEOUT),
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
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cluster().state(ClusterState.INACTIVE);

        IgniteFuture<Void> fut =
            ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(TIMEOUT), IgniteException.class, "The cluster should be active");
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotRestoreOnSmallerTopology() throws Exception {
        startGridsWithSnapshot(2, CACHE_KEYS_RANGE, true);

        stopGrid(1);

        resetBaselineTopology();

        IgniteFuture<Void> fut =
            grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), IgniteIllegalStateException.class, null);

        ensureCacheAbsent(dfltCacheCfg);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreSharedCacheGroup() throws Exception {
        CacheConfiguration<Integer, Object> cacheCfg1 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE1)).setGroupName(SHARED_GRP);

        CacheConfiguration<Integer, Object> cacheCfg2 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE2)).setGroupName(SHARED_GRP);

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valBuilder, cacheCfg1, cacheCfg2);

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cache(CACHE1).destroy();

        awaitPartitionMapExchange();

        IgniteSnapshot snp = ignite.snapshot();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> snp.restoreSnapshot(SNAPSHOT_NAME, Arrays.asList(CACHE1, CACHE2)).get(TIMEOUT),
            IllegalArgumentException.class,
            "Cache group(s) was not found in the snapshot"
        );

        ignite.cache(CACHE2).destroy();

        awaitPartitionMapExchange();

        snp.restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(SHARED_GRP)).get(TIMEOUT);

        assertCacheKeys(ignite.cache(CACHE1), CACHE_KEYS_RANGE);
        assertCacheKeys(ignite.cache(CACHE2), CACHE_KEYS_RANGE);
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
        checkCacheStartWithTheSameName(RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE, IgniteCheckedException.class,
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

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE, DEFAULT_CACHE_NAME);

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

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGrid(3),
            IgniteSpiException.class,
            "to add the node to cluster - remove directories with the caches"
        );
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFailDuringFilesCopy() throws Exception {
        dfltCacheCfg.setCacheMode(CacheMode.REPLICATED);

        startGridsWithSnapshot(3, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(2));
        CountDownLatch stopLatch = new CountDownLatch(1);

        spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage &&
            ((SingleNodeMessage<?>)msg).type() == RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE.ordinal());

        String failingFilePath = Paths.get(CACHE_DIR_PREFIX + DEFAULT_CACHE_NAME,
            PART_FILE_PREFIX + (dfltCacheCfg.getAffinity().partitions() / 2) + FILE_SUFFIX).toString();

        grid(2).context().cache().context().snapshotMgr().ioFactory(
            new CustomFileIOFactory(new RandomAccessFileIOFactory(),
                file -> {
                    if (file.getPath().endsWith(failingFilePath)) {
                        stopLatch.countDown();

                        throw new RuntimeException("Test exception");
                    }
                }));

        File node2dbDir = ((FilePageStoreManager)grid(2).context().cache().context().pageStore()).
            cacheWorkDir(dfltCacheCfg).getParentFile();

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

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, procType, DEFAULT_CACHE_NAME);

        ignite.cluster().state(state);

        spi.stopBlock();

        if (exCls == null) {
            fut.get(TIMEOUT);

            ignite.cluster().state(ClusterState.ACTIVE);

            assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);

            return;
        }

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), exCls, expMsg);

        ignite.cluster().state(ClusterState.ACTIVE);

        ensureCacheAbsent(dfltCacheCfg);

        String cacheName = DEFAULT_CACHE_NAME;

        grid(nodesCnt - 1).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(cacheName)).get(TIMEOUT);

        assertCacheKeys(ignite.cache(cacheName), CACHE_KEYS_RANGE);
    }

    /**
     * @param ccfg Cache configuration.
     * @throws IgniteCheckedException if failed.
     */
    private void ensureCacheAbsent(CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException {
        String cacheName = ccfg.getName();

        for (Ignite ignite : G.allGrids()) {
            GridKernalContext kctx = ((IgniteEx)ignite).context();

            if (kctx.clientNode())
                continue;

            CacheGroupDescriptor desc = kctx.cache().cacheGroupDescriptors().get(CU.cacheId(cacheName));

            assertNull("nodeId=" + kctx.localNodeId() + ", cache=" + cacheName, desc);

            GridTestUtils.waitForCondition(
                () -> !kctx.cache().context().snapshotMgr().isRestoring(),
                TIMEOUT);

            File dir = ((FilePageStoreManager)kctx.cache().context().pageStore()).cacheWorkDir(ccfg);

            String errMsg = String.format("%s, dir=%s, exists=%b, files=%s",
                ignite.name(), dir, dir.exists(), Arrays.toString(dir.list()));

            assertTrue(errMsg, !dir.exists() || dir.list().length == 0);
        }
    }

    /**
     * @param spi Test communication spi.
     * @param restorePhase The type of distributed process on which communication is blocked.
     * @param grpName Cache group name.
     * @return Snapshot restore future.
     * @throws InterruptedException if interrupted.
     */
    private IgniteFuture<Void> waitForBlockOnRestore(
        TestRecordingCommunicationSpi spi,
        DistributedProcessType restorePhase,
        String grpName
    ) throws InterruptedException {
        spi.blockMessages((node, msg) ->
            msg instanceof SingleNodeMessage && ((SingleNodeMessage<?>)msg).type() == restorePhase.ordinal());

        IgniteFuture<Void> fut =
            grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(grpName));

        spi.waitForBlocked();

        return fut;
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
}
