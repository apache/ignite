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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
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
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_START;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Snapshot restore tests.
 */
public class IgniteClusterSnapshotRestoreSelfTest extends AbstractSnapshotSelfTest {
    /** Timeout. */
    private static final long TIMEOUT = 15_000;

    /** Binary type name. */
    private static final String BIN_TYPE_NAME = "customType";

    /** Static cache configurations. */
    protected CacheConfiguration<?, ?>[] cacheCfgs;

    /** Cache value builder. */
    protected Function<Integer, Object> valBuilder = new IndexedValueBuilder();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        if (cacheCfgs != null)
            cfg.setCacheConfiguration(cacheCfgs);
        else if (dfltCacheCfg != null) {
            dfltCacheCfg.setSqlIndexMaxInlineSize(255);
            dfltCacheCfg.setQueryEntities(
                Arrays.asList(queryEntity(BIN_TYPE_NAME), queryEntity(IndexedObject.class.getName())));
        }

        return cfg;
    }

    /**
     * @param typeName Type name.
     */
    private QueryEntity queryEntity(String typeName) {
        return new QueryEntity()
            .setKeyType("java.lang.Integer")
            .setValueType(typeName)
            .setFields(new LinkedHashMap<>(F.asMap("id", Integer.class.getName(), "name", String.class.getName())))
            .setIndexes(Arrays.asList(new QueryIndex("id"), new QueryIndex("name")));
    }

    /**
     * Ensures that the cache doesn't start if one of the baseline nodes fails.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheStartFailOnNodeLeft() throws Exception {
        int keysCnt = 10_000;

        startGridsWithSnapshot(3, keysCnt, true);

        BlockingCustomMessageDiscoverySpi discoSpi = discoSpi(grid(0));

        discoSpi.block((msg) -> msg instanceof DynamicCacheChangeBatch);

        IgniteFuture<Void> fut =
            grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        discoSpi.waitBlocked(TIMEOUT);

        stopGrid(2, true);

        discoSpi.unblock();

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), ClusterTopologyCheckedException.class, null);

        ensureCacheDirEmpty(2, dfltCacheCfg.getName());
    }

    /** @throws Exception If failed. */
    @Test
    public void testBasicClusterSnapshotRestore() throws Exception {
        int keysCnt = 10_000;

        IgniteEx ignite = startGridsWithSnapshot(2, keysCnt, true);

        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(TIMEOUT);

        IgniteCache<Object, Object> cache = ignite.cache(dfltCacheCfg.getName());

        assertTrue(cache.indexReadyFuture().isDone());

        checkCacheKeys(cache, keysCnt);
    }

    /** @throws Exception If failed. */
    @Test
    public void testBasicClusterSnapshotRestoreWithMetadata() throws Exception {
        int keysCnt = 10_000;

        valBuilder = new BinaryValueBuilder(0, BIN_TYPE_NAME);

        IgniteEx ignite = startGridsWithSnapshot(2, keysCnt);

        // remove metadata
        int typeId = ignite.context().cacheObjects().typeId(BIN_TYPE_NAME);

        ignite.context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(TIMEOUT);

        IgniteCache<Object, Object> cache = ignite.cache(dfltCacheCfg.getName()).withKeepBinary();

        assertTrue(cache.indexReadyFuture().isDone());

        checkCacheKeys(cache, keysCnt);
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotRestoreRejectOnInActiveCluster() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cluster().state(ClusterState.INACTIVE);

        IgniteFuture<Void> fut =
            ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(TIMEOUT), IgniteException.class, "The cluster should be active");
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotRestoreDiffTopology() throws Exception {
        int nodesCnt = 4;

        int keysCnt = 10_000;

        valBuilder = new BinaryValueBuilder(0, BIN_TYPE_NAME);

        startGridsWithCache(nodesCnt - 2, keysCnt, valBuilder, dfltCacheCfg);

        grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        startGrid(nodesCnt - 2);

        IgniteEx ignite = startGrid(nodesCnt - 1);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        // remove metadata
        int typeId = ignite.context().cacheObjects().typeId(BIN_TYPE_NAME);

        ignite.context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        // Restore from an empty node.
        ignite.snapshot().restoreSnapshot(
            SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(TIMEOUT);

        IgniteCache<Object, Object> cache = ignite.cache(dfltCacheCfg.getName()).withKeepBinary();

        assertTrue(cache.indexReadyFuture().isDone());

        awaitPartitionMapExchange();

        checkCacheKeys(cache, keysCnt);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreSharedCacheGroup() throws Exception {
        String grpName = "shared";
        String cacheName1 = "cache1";
        String cacheName2 = "cache2";

        CacheConfiguration<?, ?> cacheCfg1 = txCacheConfig(new CacheConfiguration<>(cacheName1)).setGroupName(grpName);
        CacheConfiguration<?, ?> cacheCfg2 = txCacheConfig(new CacheConfiguration<>(cacheName2)).setGroupName(grpName);

        cacheCfgs = new CacheConfiguration[] {cacheCfg1, cacheCfg2};

        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Object> cache1 = ignite.cache(cacheName1);
        putKeys(cache1, 0, CACHE_KEYS_RANGE);

        IgniteCache<Integer, Object> cache2 = ignite.cache(cacheName2);
        putKeys(cache2, 0, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        cache1.destroy();

        awaitPartitionMapExchange();

        IgniteSnapshot snp = ignite.snapshot();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> snp.restoreSnapshot(SNAPSHOT_NAME, Arrays.asList(cacheName1, cacheName2)).get(TIMEOUT),
            IllegalArgumentException.class,
            "Cache group(s) was not found in the snapshot"
        );

        cache2.destroy();

        awaitPartitionMapExchange();

        snp.restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(grpName)).get(TIMEOUT);

        checkCacheKeys(ignite.cache(cacheName1), CACHE_KEYS_RANGE);
        checkCacheKeys(ignite.cache(cacheName2), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreCacheGroupWithNodeFilter() throws Exception {
        String cacheName1 = "cache1";
        String cacheName2 = "cache2";

        CacheConfiguration<Integer, Object> cacheCfg1 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(cacheName1)).setCacheMode(CacheMode.REPLICATED);

        CacheConfiguration<Integer, Object> cacheCfg2 =
            txCacheConfig(new CacheConfiguration<Integer, Object>(cacheName2)).setCacheMode(CacheMode.REPLICATED);

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        ignite0.cluster().state(ClusterState.ACTIVE);

        UUID nodeId0 = ignite0.localNode().id();
        UUID nodeId1 = ignite1.localNode().id();

        cacheCfg1.setNodeFilter(node -> node.id().equals(nodeId0));
        cacheCfg2.setNodeFilter(node -> node.id().equals(nodeId1));

        IgniteCache<Integer, Object> cache1 = ignite0.createCache(cacheCfg1);
        putKeys(cache1, 0, CACHE_KEYS_RANGE);

        IgniteCache<Integer, Object> cache2 = ignite0.createCache(cacheCfg2);
        putKeys(cache2, 0, CACHE_KEYS_RANGE);

        ignite0.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        cache1.destroy();
        cache2.destroy();

        awaitPartitionMapExchange();

        forceCheckpoint();

        awaitPartitionMapExchange();

        U.sleep(2_000);

        // After destroying the cache with a node filter, the configuration file remains on the filtered node.
        // todo https://issues.apache.org/jira/browse/IGNITE-14044
        for (String cacheName : new String[] {cacheName1, cacheName2}) {
            for (int nodeIdx = 0; nodeIdx < 2; nodeIdx++)
                U.delete(resolveCacheDir(grid(nodeIdx), cacheName));
        }

        ignite0.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(cacheName1)).get(TIMEOUT);
        awaitPartitionMapExchange();

        ignite1.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(cacheName2)).get(TIMEOUT);
        awaitPartitionMapExchange();

        checkCacheKeys(ignite0.cache(cacheName1), CACHE_KEYS_RANGE);
        checkCacheKeys(ignite0.cache(cacheName2), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testIncompatibleMetasUpdate() throws Exception {
        valBuilder = new BinaryValueBuilder(0, BIN_TYPE_NAME);

        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        int typeId = ignite.context().cacheObjects().typeId(BIN_TYPE_NAME);

        ignite.context().cacheObjects().removeType(typeId);

        BinaryObject[] objs = new BinaryObject[CACHE_KEYS_RANGE];

        IgniteCache<Integer, Object> cache1 = createCacheWithBinaryType(ignite, "cache1", n -> {
            BinaryObjectBuilder builder = ignite.binary().builder(BIN_TYPE_NAME);

            builder.setField("id", n);

            objs[n] = builder.build();

            return objs[n];
        });

        IgniteFuture<Void> fut =
            ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        fut.get(TIMEOUT);

        // Ensure that existing type has been updated
        BinaryType type = ignite.context().cacheObjects().metadata(typeId);

        assertTrue(type.fieldNames().contains("name"));

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            assertEquals(objs[i], cache1.get(i));

        cache1.destroy();

        grid(0).cache(dfltCacheCfg.getName()).destroy();

        ignite.context().cacheObjects().removeType(typeId);

        // Create cache with incompatible binary type.
        cache1 = createCacheWithBinaryType(ignite, "cache1", n -> {
            BinaryObjectBuilder builder = ignite.binary().builder(BIN_TYPE_NAME);

            builder.setField("id", UUID.randomUUID());

            objs[n] = builder.build();

            return objs[n];
        });

        IgniteFuture<Void> fut0 =
            ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut0.get(TIMEOUT), BinaryObjectException.class, null);

        ensureCacheDirEmpty(2, dfltCacheCfg.getName());

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
            "Cache start failed. A cache named \"cache1\" is currently being restored from a snapshot.");
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
        String grpName = "shared";
        String cacheName = "cache1";

        dfltCacheCfg = txCacheConfig(new CacheConfiguration<Integer, Object>(cacheName)).setGroupName(grpName);

        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, procType, grpName);

        GridTestUtils.assertThrowsAnyCause(log, () -> ignite.createCache(grpName), IgniteCheckedException.class, null);
        GridTestUtils.assertThrowsAnyCause(log, () -> ignite.createCache(cacheName), expCls, expMsg);

        spi.stopBlock();

        fut.get(TIMEOUT);

        checkCacheKeys(grid(0).cache(cacheName), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeFail() throws Exception {
        checkTopologyChange(true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeJoin() throws Exception {
        checkTopologyChange(false);
    }

    /**
     * @param stopNode {@code True} to check node fail, {@code False} to check node join.
     * @throws Exception if failed.
     */
    private void checkTopologyChange(boolean stopNode) throws Exception {
        int keysCnt = 10_000;

        IgniteEx ignite = startGridsWithSnapshot(4, keysCnt);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(3));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE, dfltCacheCfg.getName());

        if (stopNode) {
            IgniteInternalFuture<?> fut0 = runAsync(() -> stopGrid(3, true));

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> fut.get(TIMEOUT),
                IgniteException.class,
                "Cache group restore operation was rejected. Server node(s) has left the cluster"
            );

            ensureCacheDirEmpty(3, dfltCacheCfg.getName());

            fut0.get(TIMEOUT);

            awaitPartitionMapExchange();

            dfltCacheCfg = null;

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> startGrid(3),
                IgniteSpiException.class,
                "to add the node to cluster - remove directories with the caches"
            );

            return;
        }

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGrid(4),
            IgniteSpiException.class,
            "Joining node during caches restore is not allowed"
        );

        spi.stopBlock();

        fut.get(TIMEOUT);

        IgniteCache<Object, Object> cache = ignite.cache(dfltCacheCfg.getName());

        assertTrue(cache.indexReadyFuture().isDone());

        checkCacheKeys(cache, keysCnt);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testClusterStateChangeActiveReadonlyOnPrepare() throws Exception {
        checkClusterStateChange(ClusterState.ACTIVE_READ_ONLY, RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE,
            IgniteException.class, "The cluster should be active.");
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
            IgniteException.class, "The cluster should be active.");
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
        checkClusterStateChange(state, procType, exCls, expMsg, false);
    }

    /**
     * @param state Cluster state.
     * @param procType The type of distributed process on which communication is blocked.
     * @param exCls Expected exception class.
     * @param expMsg Expected exception message.
     * @param stopNode Stop node flag.
     * @throws Exception if failed.
     */
    private void checkClusterStateChange(
        ClusterState state,
        DistributedProcessType procType,
        @Nullable Class<? extends Throwable> exCls,
        @Nullable String expMsg,
        boolean stopNode
    ) throws Exception {
        int nodesCnt = stopNode ? 3 : 2;

        Ignite ignite = startGridsWithSnapshot(nodesCnt, CACHE_KEYS_RANGE, true);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(nodesCnt - 1));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, procType, dfltCacheCfg.getName());

        ignite.cluster().state(state);

        if (stopNode)
            stopGrid(nodesCnt - 1);
        else
            spi.stopBlock();

        if (exCls == null) {
            fut.get(TIMEOUT);

            ignite.cluster().state(ClusterState.ACTIVE);

            checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);

            return;
        }

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), exCls, expMsg);

        ignite.cluster().state(ClusterState.ACTIVE);

        ensureCacheDirEmpty(stopNode ? nodesCnt - 1 : nodesCnt, dfltCacheCfg.getName());

        String cacheName = dfltCacheCfg.getName();

        if (stopNode) {
            dfltCacheCfg = null;

            startGrid(nodesCnt - 1);

            resetBaselineTopology();
        }

        grid(nodesCnt - 1).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(cacheName)).get(TIMEOUT);

        checkCacheKeys(ignite.cache(cacheName), CACHE_KEYS_RANGE);
    }

    /**
     * @param nodesCnt Count of nodes.
     * @param cacheName Cache name.
     * @throws IgniteCheckedException if failed.
     */
    private void ensureCacheDirEmpty(int nodesCnt, String cacheName) throws IgniteCheckedException {
        for (int nodeIdx = 0; nodeIdx < nodesCnt; nodeIdx++) {
            IgniteEx grid = grid(nodeIdx);

            CacheGroupDescriptor desc = grid.context().cache().cacheGroupDescriptors().get(CU.cacheId(cacheName));

            assertNull("nodeIdx=" + nodeIdx + ", cache=" + cacheName, desc);

            GridTestUtils.waitForCondition(
                () -> !grid.context().cache().context().snapshotMgr().isCacheRestoring(null),
                TIMEOUT);

            File dir = resolveCacheDir(grid, cacheName);

            String errMsg = String.format("%s, dir=%s, exists=%b, files=%s",
                grid.name(), dir, dir.exists(), Arrays.toString(dir.list()));

            assertTrue(errMsg, !dir.exists() || dir.list().length == 0);
        }
    }

    /**
     * @param ignite Ignite.
     * @param cacheOrGrpName Cache (or group) name.
     * @return Local path to the cache directory.
     * @throws IgniteCheckedException if failed.
     */
    private File resolveCacheDir(IgniteEx ignite, String cacheOrGrpName) throws IgniteCheckedException {
        File workDIr = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        String nodeDirName = ignite.context().pdsFolderResolver().resolveFolders().folderName() + File.separator;

        File cacheDir = new File(workDIr, nodeDirName + CACHE_DIR_PREFIX + cacheOrGrpName);

        if (cacheDir.exists())
            return cacheDir;

        return new File(workDIr, nodeDirName + CACHE_GRP_DIR_PREFIX + cacheOrGrpName);
    }

    /**
     * @param nodesCnt Nodes count.
     * @param keysCnt Number of keys to create.
     * @return Ignite coordinator instance.
     * @throws Exception if failed.
     */
    private IgniteEx startGridsWithSnapshot(int nodesCnt, int keysCnt) throws Exception {
        return startGridsWithSnapshot(nodesCnt, keysCnt, false);
    }

    /**
     * @param nodesCnt Nodes count.
     * @param keysCnt Number of keys to create.
     * @param startClient {@code True} to start an additional client node.
     * @return Ignite coordinator instance.
     * @throws Exception if failed.
     */
    private IgniteEx startGridsWithSnapshot(int nodesCnt, int keysCnt, boolean startClient) throws Exception {
        IgniteEx ignite = startGridsWithCache(nodesCnt, keysCnt, valBuilder, dfltCacheCfg.setBackups(0));

        if (startClient)
            ignite = startClientGrid("client");

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        return ignite;
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
     * @param cache Cache.
     * @param startIdx The initial value of the number for the key.
     * @param cnt Number of entries to put in the cache.
     */
    private void putKeys(IgniteCache<Integer, Object> cache, int startIdx, int cnt) {
        for (int i = startIdx; i < (startIdx + cnt); i++)
            cache.put(i, valBuilder.apply(i));
    }

    /**
     * @param cache Cache.
     * @param keysCnt Expected number of keys.
     */
    private void checkCacheKeys(IgniteCache<Object, Object> cache, int keysCnt) {
        assertEquals(keysCnt, cache.size());

        for (int i = 0; i < keysCnt; i++)
            assertEquals(valBuilder.apply(i), cache.get(i));
    }

    /** */
    private static class IndexedValueBuilder implements Function<Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object apply(Integer key) {
            return new IndexedObject(key, "Person number #" + key);
        }
    }

    /** */
    private static class IndexedObject {
        /** Id. */
        @QuerySqlField(index = true)
        private final int id;

        /** Name. */
        @QuerySqlField(index = true)
        private final String name;

        /**
         * @param id Id.
         */
        public IndexedObject(int id, String name) {
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

    /** */
    private class BinaryValueBuilder implements Function<Integer, Object> {
        /** Ignite node index. */
        private final int nodeIdx;

        /** Binary type name. */
        private final String typeName;

        /**
         * @param nodeIdx Ignite node index.
         * @param typeName Binary type name.
         */
        BinaryValueBuilder(int nodeIdx, String typeName) {
            this.nodeIdx = nodeIdx;
            this.typeName = typeName;
        }

        /** {@inheritDoc} */
        @Override public Object apply(Integer key) {
            BinaryObjectBuilder builder = grid(nodeIdx).binary().builder(typeName);

            builder.setField("id", key);
            builder.setField("name", String.valueOf(key));

            return builder.build();
        }
    }
}
