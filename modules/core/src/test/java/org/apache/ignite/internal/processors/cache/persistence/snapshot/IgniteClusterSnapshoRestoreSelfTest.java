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
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Snapshot restore tests.
 */
public class IgniteClusterSnapshoRestoreSelfTest extends AbstractSnapshotSelfTest {
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
        else {
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

    /** @throws Exception If failed. */
    @Test
    public void testBasicClusterSnapshotRestore() throws Exception {
        int keysCnt = 10_000;

        IgniteEx ignite = startGridsWithSnapshot(2, keysCnt);

        ignite.snapshot().restoreCacheGroups(
            SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(TIMEOUT);

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

        ignite.snapshot().restoreCacheGroups(
            SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(TIMEOUT);

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
            ignite.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(TIMEOUT), IgniteException.class, "The cluster should be active");
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreWithMissedPartitions() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valBuilder, dfltCacheCfg.setBackups(0));

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        forceCheckpoint();

        stopGrid(1);

        IgniteFuture<Void> fut =
            ignite.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(TIMEOUT), IgniteCheckedException.class, "not all partitions available");

        startGrid(1);

        IgniteFuture<Void> fut1 =
            ignite.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut1.get(TIMEOUT), IllegalStateException.class,
            "Cache \"" + dfltCacheCfg.getName() + "\" should be destroyed manually");

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        ignite.snapshot().restoreCacheGroups(
            SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(TIMEOUT);

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotRestoreDiffTopology() throws Exception {
        int nodesCnt = 4;

        int keysCnt = 10_000;

        valBuilder = new BinaryValueBuilder(0, BIN_TYPE_NAME);

        IgniteEx ignite = startGridsWithCache(nodesCnt - 2, keysCnt, valBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        startGrid(nodesCnt - 2);
        startGrid(nodesCnt - 1);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        // remove metadata
        int typeId = grid(nodesCnt - 1).context().cacheObjects().typeId(BIN_TYPE_NAME);

        grid(nodesCnt - 1).context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        ignite.snapshot().restoreCacheGroups(
            SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(TIMEOUT);

        IgniteCache<Object, Object> cache = grid(nodesCnt - 1).cache(dfltCacheCfg.getName()).withKeepBinary();

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
            () -> snp.restoreCacheGroups(SNAPSHOT_NAME, Arrays.asList(cacheName1, cacheName2)).get(TIMEOUT),
            IllegalArgumentException.class,
            "Cache group(s) not found in snapshot"
        );

        cache2.destroy();

        awaitPartitionMapExchange();

        snp.restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(grpName)).get(TIMEOUT);

        checkCacheKeys(ignite.cache(cacheName1), CACHE_KEYS_RANGE);
        checkCacheKeys(ignite.cache(cacheName2), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreCacheGroupWithNodeFilter() throws Exception {
        String cacheName1 = "cache1";
        String cacheName2 = "cache2";

        CacheConfiguration<Integer, Object> cacheCfg1 = txCacheConfig(new CacheConfiguration<Integer, Object>(cacheName1)).setCacheMode(CacheMode.REPLICATED);
        CacheConfiguration<Integer, Object> cacheCfg2 = txCacheConfig(new CacheConfiguration<Integer, Object>(cacheName2)).setCacheMode(CacheMode.REPLICATED);

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

        // After destroying the cache with a node filter, the configuration file remains on the filtered node.
        // todo https://issues.apache.org/jira/browse/IGNITE-14044
        for (String cacheName : new String[] {cacheName1, cacheName2}) {
            for (int nodeIdx = 0; nodeIdx < 2; nodeIdx++)
                U.delete(resolveCacheDir(grid(nodeIdx), cacheName));
        }

        ignite0.cluster().state(ClusterState.ACTIVE);

        ignite0.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(cacheName1)).get(TIMEOUT);
        awaitPartitionMapExchange();

        ignite1.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(cacheName2)).get(TIMEOUT);
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
            ignite.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

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
            ignite.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> fut0.get(TIMEOUT),
            IgniteException.class,
            "Cache group restore operation was rejected. Incompatible binary types found"
        );

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
        checkCacheStartWithTheSameName(true);
    }

    /**
     * @throws Exception if failed
     */
    @Test
    public void testParallelCacheStartWithTheSameNameOnPerform() throws Exception {
        checkCacheStartWithTheSameName(false);
    }

    /**
     * @param prepare {@code True} to start cache during prepare phase, {@code False} to start cache during perform phase.
     * @throws Exception if failed.
     */
    private void checkCacheStartWithTheSameName(boolean prepare) throws Exception {
        String grpName = "shared";
        String cacheName = "cache1";

        dfltCacheCfg = txCacheConfig(new CacheConfiguration<Integer, Object>(cacheName)).setGroupName(grpName);

        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, prepare ?
            RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE : RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM, grpName);

        String msgFormat = "Cache start failed. A cache named \"%s\" is currently being restored from a snapshot.";

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> ignite.createCache(grpName),
            IgniteCheckedException.class,
            String.format(msgFormat, grpName)
        );

        if (prepare)
            ignite.createCache(cacheName);
        else {
            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> ignite.createCache(cacheName),
                IgniteCheckedException.class,
                String.format(msgFormat, cacheName)
            );
        }

        spi.stopBlock();

        // We don't know shared cache names during prepare phase - so we just interrupting process.
        if (prepare) {
            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> fut.get(TIMEOUT),
                IgniteException.class,
                "Cache \"" + cacheName + "\" should be destroyed manually before perform restore operation."
            );

            ensureCacheDirEmpty(0, grpName);
            ensureCacheDirEmpty(1, grpName);
        }
        else {
            fut.get(TIMEOUT);

            checkCacheKeys(grid(0).cache(cacheName), CACHE_KEYS_RANGE);
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testRollbackOnNodeFail() throws Exception {
        checkBaselineChange(true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeJoin() throws Exception {
        checkBaselineChange(false);
    }

    /**
     * @param stopNode {@code True} to check node fail, {@code False} to check node join.
     * @throws Exception if failed.
     */
    private void checkBaselineChange(boolean stopNode) throws Exception {
        int keysCnt = 10_000;

        IgniteEx ignite = startGridsWithSnapshot(4, keysCnt);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(3));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM, dfltCacheCfg.getName());

        if (stopNode) {
            runAsync(() -> stopGrid(3, true));

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> fut.get(TIMEOUT),
                IgniteException.class,
                "Cache group restore operation was rejected. Baseline node has left the cluster"
            );

            ensureCacheDirEmpty(3, dfltCacheCfg.getName());

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
    public void testClusterStateChangeActiveReadonlyDuringPrepare() throws Exception {
        checkReadOnlyDuringRestoring(RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE);
    }


    /**
     * @throws Exception if failed.
     */
    @Test
    public void testClusterStateChangeActiveReadonlyDuringPerform() throws Exception {
        checkReadOnlyDuringRestoring(RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM);
    }

    /**
     * @param procType The type of distributed process on which communication is blocked.
     * @throws Exception if failed.
     */
    private void checkReadOnlyDuringRestoring(DistributedProcessType procType) throws Exception {
        checkClusterStateChange(ClusterState.ACTIVE_READ_ONLY, procType, IgniteClusterReadOnlyException.class,
            "Failed to perform start cache operation (cluster is in read-only mode)");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testClusterDeactivateOnPrepare() throws Exception {
        checkDeactivationDuringRestoring(RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testClusterDeactivateOnPerform() throws Exception {
        checkDeactivationDuringRestoring(RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM);
    }

    /**
     * @param procType The type of distributed process on which communication is blocked.
     * @throws Exception if failed.
     */
    private void checkDeactivationDuringRestoring(DistributedProcessType procType) throws Exception {
        checkClusterStateChange(ClusterState.INACTIVE, procType, IgniteCheckedException.class,
            "The cluster has been deactivated.");
    }

    /**
     * @param state Cluster state.
     * @param procType The type of distributed process on which communication is blocked.
     * @param expCls Expected exception class.
     * @param expMsg Expected exception message.
     * @throws Exception if failed.
     */
    private void checkClusterStateChange(
        ClusterState state,
        DistributedProcessType procType,
        Class<? extends Throwable> expCls,
        String expMsg
    ) throws Exception {
        Ignite ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, procType, dfltCacheCfg.getName());

        ignite.cluster().state(state);

        spi.stopBlock();

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), expCls, expMsg);

        ensureCacheDirEmpty(2, dfltCacheCfg.getName());

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(TIMEOUT);

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /**
     * @param nodesCnt Count of nodes.
     * @param cacheName Cache name.
     * @throws IgniteCheckedException if failed.
     */
    private void ensureCacheDirEmpty(int nodesCnt, String cacheName) throws IgniteCheckedException {
        for (int nodeIdx = 0; nodeIdx < nodesCnt; nodeIdx++) {
            IgniteEx grid = grid(nodeIdx);

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
        IgniteEx ignite = startGridsWithCache(nodesCnt, keysCnt, valBuilder, dfltCacheCfg);

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
            grid(0).snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(grpName));

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
