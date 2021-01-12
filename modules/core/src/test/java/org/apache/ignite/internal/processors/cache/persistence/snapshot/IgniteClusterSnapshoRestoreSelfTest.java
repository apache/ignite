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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT_RESTORE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT_RESTORE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Snapshot restore tests.
 */
public class IgniteClusterSnapshoRestoreSelfTest extends AbstractSnapshotSelfTest {
    /** Timeout. */
    private static final long MAX_AWAIT_MILLIS = 15_000;

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

//    /** @throws Exception If fails. */
//    @Before
//    @Override public void beforeTestSnapshot() throws Exception {
//        super.beforeTestSnapshot();
//    }

    /** {@inheritDoc} */
    @Override public void afterTestSnapshot() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If fails. */
    @Test
    public void testBasicClusterSnapshotRestore() throws Exception {
        int keysCnt = 10_000;

        IgniteEx ignite = startGridsWithSnapshot(2, keysCnt);

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

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

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        IgniteCache<Object, Object> cache = ignite.cache(dfltCacheCfg.getName()).withKeepBinary();

        assertTrue(cache.indexReadyFuture().isDone());

        checkCacheKeys(cache, keysCnt);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotRestoreRejectOnInActiveCluster() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        ignite.cluster().state(ClusterState.INACTIVE);

        IgniteFuture<Void> fut = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(MAX_AWAIT_MILLIS), IgniteException.class, "The cluster should be active");
    }

    /** @throws Exception If fails. */
    @Test
    public void testRestoreWithMissedPartitions() throws Exception {
        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder, dfltCacheCfg.setBackups(0));

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

        forceCheckpoint();

        stopGrid(1);

        IgniteFuture<Void> fut = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut.get(MAX_AWAIT_MILLIS), IgniteCheckedException.class, "not all partitions available");

        startGrid(1);

        IgniteFuture<Void> fut1 = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log, () -> fut1.get(MAX_AWAIT_MILLIS), IllegalStateException.class, "Cache group \"" + dfltCacheCfg.getName() + "\" should be destroyed manually");

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotRestoreDiffTopology() throws Exception {
        int nodesCnt = 4;

        int keysCnt = 10_000;

        valueBuilder = new BinaryValueBuilder(0, BIN_TYPE_NAME);

        IgniteEx ignite = startGridsWithCache(nodesCnt - 2, keysCnt, valueBuilder, dfltCacheCfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

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

        ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);

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
        CacheConfiguration<?, ?> cacheCfg2 = txCacheConfig(new CacheConfiguration<>(cacheName2))
            .setAtomicityMode(CacheAtomicityMode.ATOMIC).setGroupName(grpName);

        cacheCfgs = new CacheConfiguration[] {cacheCfg1, cacheCfg2};

        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache1 = ignite.cache(cacheName1);
        IgniteCache<Object, Object> cache2 = ignite.cache(cacheName2);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        cache1.destroy();

        awaitPartitionMapExchange();

        IgniteSnapshotManager snapshotMgr = ignite.context().cache().context().snapshotMgr();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> snapshotMgr.restoreCacheGroups(SNAPSHOT_NAME, Arrays.asList(cacheName1, cacheName2)).get(MAX_AWAIT_MILLIS),
            IllegalArgumentException.class,
            "Cache group(s) \"" + cacheName1 + ", " + cacheName2 + "\" not found in snapshot \"" + SNAPSHOT_NAME + "\""
        );

        cache2.destroy();

        awaitPartitionMapExchange();

        snapshotMgr.restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(grpName)).get(MAX_AWAIT_MILLIS);

        cache1 = ignite.cache(cacheName1);
        cache2 = ignite.cache(cacheName2);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            assertEquals(i, cache1.get(i));
            assertEquals(i, cache2.get(i));
        }
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

        IgniteFuture<Void> fut = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        fut.get(MAX_AWAIT_MILLIS);

        // Ensure that existing type has been updated
        BinaryType type = ignite.context().cacheObjects().metadata(typeId);

        assertTrue(type.fieldNames().contains("name"));

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            assertEquals(objs[i], cache1.get(i));

        cache1.destroy();

        grid(0).cache(dfltCacheCfg.getName()).destroy();

        ignite.context().cacheObjects().removeType(typeId);

        // Create cache with incompatible binary type
        cache1 = createCacheWithBinaryType(ignite, "cache1", n -> {
            BinaryObjectBuilder builder = ignite.binary().builder(BIN_TYPE_NAME);

            builder.setField("id", UUID.randomUUID());

            objs[n] = builder.build();

            return objs[n];
        });

        final IgniteFuture<Void> fut0 = ignite.context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> fut0.get(MAX_AWAIT_MILLIS),
            IgniteException.class,
            "Snapshot restore operation was rejected. Incompatible binary types found"
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
    @Ignore
    public void testParallelCacheStartWithTheSameName() throws Exception {
        int keysCnt = 10_000;

        IgniteEx ignite = startGridsWithSnapshot(2, keysCnt);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, START_SNAPSHOT_RESTORE);

        IgniteCache cache = ignite.createCache(dfltCacheCfg);

        spi.stopBlock();

        fut.get(MAX_AWAIT_MILLIS);

        checkCacheKeys(grid(0).cache(dfltCacheCfg.getName()), keysCnt);
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

        IgniteFuture<Void> fut = waitForBlockOnRestore(spi, END_SNAPSHOT_RESTORE);

        if (stopNode) {
            runAsync(() -> stopGrid(3, true));

            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> fut.get(MAX_AWAIT_MILLIS),
                IgniteException.class,
                "Snapshot restore operation was rejected. Baseline node has left the cluster"
            );

            ensureCacheDirEmpty(3, dfltCacheCfg.getName());

            return;
        }

        startGrid(4);

        resetBaselineTopology();

        spi.stopBlock();

        fut.get(MAX_AWAIT_MILLIS);

        IgniteCache<Object, Object> cache = grid(4).cache(dfltCacheCfg.getName());

        assertTrue(cache.indexReadyFuture().isDone());

        checkCacheKeys(cache, keysCnt);
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

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        return ignite;
    }

    private IgniteFuture<Void> waitForBlockOnRestore(TestRecordingCommunicationSpi spi, DistributedProcess.DistributedProcessType restorePhase) throws InterruptedException {
        spi.blockMessages((node, msg) ->
            msg instanceof SingleNodeMessage && ((SingleNodeMessage<?>)msg).type() == restorePhase.ordinal());

        IgniteFuture<Void> fut = grid(0).context().cache().context().snapshotMgr().
            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

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

        client.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);

        putKeys(client.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);

        client.cluster().state(ClusterState.INACTIVE);

        IgniteSnapshotManager snapshotMgr = grid(1).context().cache().context().snapshotMgr();

        // todo block distribprocess and try to activate cluster
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.blockMessages((node, msg) -> {
            if (msg instanceof SingleNodeMessage)
                return true;

            System.out.println(">xxx> " + node.id());

            return false;
        });

        IgniteFuture<Void> fut = snapshotMgr.restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName()));

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

        fut.get(MAX_AWAIT_MILLIS);

        client.cluster().state(ClusterState.ACTIVE);

        checkCacheKeys(client.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

//    @Test
//    public void testPreventRecoveryOnRestoredCacheGroup() throws Exception {
//        IgniteEx ignite = startGridsWithCache(2, dfltCacheCfg, CACHE_KEYS_RANGE);
//
//        resetBaselineTopology();
//
//        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(MAX_AWAIT_MILLIS);
//
//        enableCheckpoints(G.allGrids(), false);
//
//        putKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE, CACHE_KEYS_RANGE);
//
//        stopAllGrids();
//
//        ignite = startGrid(0);
//        startGrid(1);
//
//        ignite.cluster().state(ClusterState.ACTIVE);
//
//        ignite.cache(dfltCacheCfg.getName()).destroy();
//
//        awaitPartitionMapExchange();
//
//        ignite.context().cache().context().snapshotMgr().
//            restoreCacheGroups(SNAPSHOT_NAME, Collections.singleton(dfltCacheCfg.getName())).get(MAX_AWAIT_MILLIS);
//
//        stopAllGrids();
//
//        ignite = startGrid(0);
//        startGrid(1);
//
//        ignite.cluster().state(ClusterState.ACTIVE);
//
//        checkCacheKeys(ignite.cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
//    }

    private void checkCacheKeys(IgniteCache<Object, Object> testCache, int keysCnt) {
        assertEquals(keysCnt, testCache.size());

        for (int i = 0; i < keysCnt; i++)
            assertEquals(valueBuilder.apply(i), testCache.get(i));
    }

    private void putKeys(IgniteCache<Object, Object> cache, int startIdx, int cnt) {
        for (int i = startIdx; i < (startIdx + cnt); i++)
            cache.put(i, i);
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
