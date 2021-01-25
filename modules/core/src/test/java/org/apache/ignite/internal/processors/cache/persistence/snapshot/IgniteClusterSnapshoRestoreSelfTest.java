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
import org.junit.Ignore;
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

    private static final String BIN_TYPE_NAME = "customType";

    protected CacheConfiguration[] cacheCfgs;

    protected Function<Integer, Object> valueBuilder = new IndexedValueBuilder();

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

    private QueryEntity queryEntity(String typeName) {
        return new QueryEntity()
            .setKeyType("java.lang.Integer")
            .setValueType(typeName)
            .setFields(new LinkedHashMap<>(F.asMap("id", Integer.class.getName(), "name", String.class.getName())))
            .setIndexes(Arrays.asList(new QueryIndex("id"), new QueryIndex("name")));
    }

    /** {@inheritDoc} */
    @Override public void afterTestSnapshot() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If fails. */
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

    /** @throws Exception If fails. */
    @Test
    public void testBasicClusterSnapshotRestoreWithMetadata() throws Exception {
        int keysCnt = 10_000;

        valueBuilder = new BinaryValueBuilder(0, BIN_TYPE_NAME);

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

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotRestoreRejectOnInActiveCluster() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cluster().state(ClusterState.INACTIVE);

        IgniteFuture<Void> fut =
            ignite.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(TIMEOUT), IgniteException.class, "The cluster should be active");
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreWithMissedPartitions() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg.setBackups(0));

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

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

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotRestoreDiffTopology() throws Exception {
        int nodesCnt = 4;

        int keysCnt = 10_000;

        valueBuilder = new BinaryValueBuilder(0, BIN_TYPE_NAME);

        IgniteEx ignite = startGridsWithCache(nodesCnt - 2, keysCnt, valueBuilder, dfltCacheCfg);

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

    /** @throws Exception If fails. */
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
            () -> snp.restoreCacheGroups(SNAPSHOT_NAME, Arrays.asList(grpName, cacheName1, cacheName2)).get(TIMEOUT),
            IllegalArgumentException.class,
            "Cache group(s) not found in snapshot"
        );

        cache2.destroy();

        awaitPartitionMapExchange();

        snp.restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(grpName)).get(TIMEOUT);

        checkCacheKeys(ignite.cache(cacheName1), CACHE_KEYS_RANGE);
        checkCacheKeys(ignite.cache(cacheName2), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
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
        ignite1.snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(cacheName2)).get(TIMEOUT);

        checkCacheKeys(ignite0.cache(cacheName1), CACHE_KEYS_RANGE);
        checkCacheKeys(ignite0.cache(cacheName2), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testIncompatibleMetasUpdate() throws Exception {
        valueBuilder = new BinaryValueBuilder(0, BIN_TYPE_NAME);

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

    private IgniteCache<Integer, Object> createCacheWithBinaryType(Ignite ignite, String cacheName, Function<Integer, BinaryObject> valBuilder) {
        IgniteCache<Integer, Object> cache = ignite.createCache(new CacheConfiguration<>(cacheName)).withKeepBinary();

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            cache.put(i, valBuilder.apply(i));

        return cache;
    }

    @Test
    public void testParallelCacheStartWithTheSameNameOnPrepare() throws Exception {
        checkCacheStartWithTheSameName(true);
    }

    @Test
    public void testParallelCacheStartWithTheSameNameOnPerform() throws Exception {
        checkCacheStartWithTheSameName(false);
    }

    private void checkCacheStartWithTheSameName(boolean prepare) throws Exception {
        String grpName = "shared";
        String cacheName = "cache1";

        dfltCacheCfg = txCacheConfig(new CacheConfiguration<Integer, Object>(cacheName)).setGroupName(grpName);

        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, prepare ? RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE : RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM, grpName);

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

    /** @throws Exception If fails. */
    @Test
    public void testRollbackOnNodeFail() throws Exception {
        checkBaselineChange(true);
    }

    /** @throws Exception If fails. */
    @Test
    public void testNodeJoin() throws Exception {
        checkBaselineChange(false);
    }

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

    @Test
    public void testClusterStateChangeActiveReadonlyDuringPrepare() throws Exception {
        checkReadOnlyDuringRestoring(RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE);
    }

    @Test
    public void testClusterStateChangeActiveReadonlyDuringPerform() throws Exception {
        checkReadOnlyDuringRestoring(RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM);
    }

    private void checkReadOnlyDuringRestoring(DistributedProcessType procType) throws Exception {
        checkClusterStateChange(ClusterState.ACTIVE_READ_ONLY, procType, IgniteClusterReadOnlyException.class,
            "Failed to perform start cache operation (cluster is in read-only mode)");
    }

    @Test
    public void testClusterDeactivateOnPrepare() throws Exception {
        checkDeactivationDuringRestoring(RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE);
    }

    @Test
    public void testClusterDeactivateOnPerform() throws Exception {
        checkDeactivationDuringRestoring(RESTORE_CACHE_GROUP_SNAPSHOT_PERFORM);
    }

    private void checkDeactivationDuringRestoring(DistributedProcessType procType) throws Exception {
        checkClusterStateChange(ClusterState.INACTIVE, procType, IgniteCheckedException.class,
            "The cluster has been deactivated.");
    }

    private void checkClusterStateChange(ClusterState state, DistributedProcessType procType, Class<? extends Throwable> expCls, String expMsg) throws Exception {
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

    private void ensureCacheDirEmpty(int nodesCnt, String cacheName) throws IgniteCheckedException {
        for (int nodeIdx = 0; nodeIdx < nodesCnt; nodeIdx++) {
            IgniteEx grid = grid(nodeIdx);

            File dir = resolveCacheDir(grid, cacheName);

            String errMsg = String.format("%s, dir=%s, exists=%b, files=%s",
                grid.name(), dir, dir.exists(), Arrays.toString(dir.list()));

            assertTrue(errMsg, !dir.exists() || dir.list().length == 0);
        }
    }

    private File resolveCacheDir(IgniteEx ignite, String cacheOrGrpName) throws IgniteCheckedException {
        File workDIr = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        String nodeDirName = ignite.context().pdsFolderResolver().resolveFolders().folderName() + File.separator;

        File cacheDir = new File(workDIr, nodeDirName + CACHE_DIR_PREFIX + cacheOrGrpName);

        if (cacheDir.exists())
            return cacheDir;

        return new File(workDIr, nodeDirName + CACHE_GRP_DIR_PREFIX + cacheOrGrpName);
    }

    private IgniteEx startGridsWithSnapshot(int nodesCnt, int keysCnt) throws Exception {
        IgniteEx ignite = startGridsWithCache(nodesCnt, keysCnt, valueBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        return ignite;
    }

    private IgniteFuture<Void> waitForBlockOnRestore(TestRecordingCommunicationSpi spi, DistributedProcessType restorePhase, String grpName) throws InterruptedException {
        spi.blockMessages((node, msg) ->
            msg instanceof SingleNodeMessage && ((SingleNodeMessage<?>)msg).type() == restorePhase.ordinal());

        IgniteFuture<Void> fut =
            grid(0).snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(grpName));

        spi.waitForBlocked();

        return fut;
    }

    /** @throws Exception If fails. */
    @Test
    // todo
    @Ignore
    public void testActivateFromClientWhenRestoring() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg);

        IgniteEx client = startClientGrid("client");

        client.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        putKeys(client.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

        client.cluster().state(ClusterState.INACTIVE);

        // todo block distribprocess and try to activate cluster
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.blockMessages((node, msg) -> {
            if (msg instanceof SingleNodeMessage)
                return true;

            System.out.println(">xxx> " + node.id());

            return false;
        });

        IgniteFuture<Void> fut =
            grid(1).snapshot().restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        spi.waitForBlocked();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                client.cluster().state(ClusterState.ACTIVE);

                return null;
            },
            IllegalStateException.class,
            "The cluster cannot be activated until the snapshot restore operation is complete."
        );

        spi.stopBlock();

        fut.get(TIMEOUT);

        client.cluster().state(ClusterState.ACTIVE);

        checkCacheKeys(client.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    private void checkCacheKeys(IgniteCache<Object, Object> testCache, int keysCnt) {
        assertEquals(keysCnt, testCache.size());

        for (int i = 0; i < keysCnt; i++)
            assertEquals(valueBuilder.apply(i), testCache.get(i));
    }

    private void putKeys(IgniteCache<Integer, Object> cache, int startIdx, int cnt) {
        for (int i = startIdx; i < (startIdx + cnt); i++)
            cache.put(i, valueBuilder.apply(i));
    }

    /** */
    private static class IntValueBuilder implements Function<Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object apply(Integer key) {
            return key;
        }
    }

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
