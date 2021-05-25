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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridJobExecuteRequest;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.verify.CacheFilterEnum;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.TTL_ETERNAL;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileName;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getTypeByPartId;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METAFILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Cluster-wide snapshot check procedure tests.
 */
public class IgniteClusterSnapshotCheckTest extends AbstractSnapshotSelfTest {
    /** Map of intermediate compute task results collected prior performing reduce operation on them. */
    private final Map<Class<?>, Map<PartitionKeyV2, List<PartitionHashRecordV2>>> jobResults = new ConcurrentHashMap<>();

    /** Partition id used for tests. */
    private static final int PART_ID = 0;

    /** Optional cache name to be created on demand. */
    private static final String OPTIONAL_CACHE_NAME = "CacheName";

    /** Cleanup data of task execution results if need. */
    @Before
    public void beforeCheck() {
        jobResults.clear();
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheck() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        startClientGrid();
        
        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertPartitionsSame(res);
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckMissedPart() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        Path part0 = U.searchFileRecursively(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath(),
            getPartitionFileName(0));

        assertNotNull(part0);
        assertTrue(part0.toString(), part0.toFile().exists());
        assertTrue(part0.toFile().delete());

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertFalse(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(), "Snapshot data doesn't contain required cache group partition");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckMissedGroup() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        Path dir = Files.walk(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath())
            .filter(d -> d.toFile().getName().equals(cacheDirName(dfltCacheCfg)))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Cache directory not found"));

        assertTrue(dir.toString(), dir.toFile().exists());
        assertTrue(U.delete(dir));

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertFalse(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(), "Snapshot data doesn't contain required cache groups");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckMissedMeta() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg, CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME)
            .get();

        File[] smfs = snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).listFiles((dir, name) ->
            name.toLowerCase().endsWith(SNAPSHOT_METAFILE_EXT));

        assertNotNull(smfs);
        assertTrue(smfs[0].toString(), smfs[0].exists());
        assertTrue(U.delete(smfs[0]));

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertFalse(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(), "Some metadata is missing from the snapshot");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithNodeFilter() throws Exception {
        IgniteEx ig0 = startGridsWithoutCache(3);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            ig0.getOrCreateCache(txCacheConfig(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME))
                .setNodeFilter(node -> node.consistentId().toString().endsWith("0"))).put(i, i);
        }

        ig0.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        IdleVerifyResultV2 res = snp(ig0).checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertPartitionsSame(res);
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckPartitionCounters() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg.
            setAffinity(new RendezvousAffinityFunction(false, 1)),
            CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        Path part0 = U.searchFileRecursively(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath(),
            getPartitionFileName(PART_ID));

        assertNotNull(part0);
        assertTrue(part0.toString(), part0.toFile().exists());

        try (FilePageStore pageStore = (FilePageStore)((FilePageStoreManager)ignite.context().cache().context().pageStore())
            .getPageStoreFactory(CU.cacheId(dfltCacheCfg.getName()), false)
            .createPageStore(getTypeByPartId(PART_ID),
                () -> part0,
                val -> {
                })
        ) {
            ByteBuffer buff = ByteBuffer.allocateDirect(ignite.configuration().getDataStorageConfiguration().getPageSize())
                .order(ByteOrder.nativeOrder());

            buff.clear();
            pageStore.read(0, buff, false);

            PagePartitionMetaIO io = PageIO.getPageIO(buff);

            long pageAddr = GridUnsafe.bufferAddress(buff);

            io.setUpdateCounter(pageAddr, CACHE_KEYS_RANGE * 2);

            pageStore.beginRecover();

            buff.flip();
            pageStore.write(PageIO.getPageId(buff), buff, 0, true);
            pageStore.finishRecover();
        }

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(),
            "The check procedure has finished, found 1 conflict partitions: [counterConflicts=1, hashConflicts=0]");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckOtherCluster() throws Exception {
        IgniteEx ig0 = startGridsWithCache(3, dfltCacheCfg.
                setAffinity(new RendezvousAffinityFunction(false, 1)),
            CACHE_KEYS_RANGE);

        ig0.snapshot().createSnapshot(SNAPSHOT_NAME).get();
        stopAllGrids();

        // Cleanup persistence directory except created snapshots.
        Arrays.stream(new File(U.defaultWorkDirectory()).listFiles())
            .filter(f -> !f.getName().equals(DFLT_SNAPSHOT_DIRECTORY))
            .forEach(U::delete);

        Set<UUID> assigns = Collections.newSetFromMap(new ConcurrentHashMap<>());

        for (int i = 4; i < 7; i++) {
            startGrid(optimize(getConfiguration(getTestIgniteInstanceName(i)).setCacheConfiguration()));

            UUID locNodeId = grid(i).localNode().id();

            grid(i).context().io().addMessageListener(GridTopic.TOPIC_JOB, new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                    if (msg instanceof GridJobExecuteRequest) {
                        GridJobExecuteRequest msg0 = (GridJobExecuteRequest)msg;

                        if (msg0.getTaskName().contains(SnapshotPartitionsVerifyTask.class.getName()))
                            assigns.add(locNodeId);
                    }
                }
            });
        }

        IgniteEx ignite = grid(4);
        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ACTIVE);

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        // GridJobExecuteRequest is not send to the local node.
        assertTrue("Number of jobs must be equal to the cluster size (except local node): " + assigns + ", count: "
                + assigns.size(), waitForCondition(() -> assigns.size() == 2, 5_000L));

        assertTrue(F.isEmpty(res.exceptions()));
        assertPartitionsSame(res);
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckCRCFail() throws Exception {
        IgniteEx ignite = startGridsWithCache(3, dfltCacheCfg.
                setAffinity(new RendezvousAffinityFunction(false, 1)), CACHE_KEYS_RANGE);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        corruptPartitionFile(ignite, SNAPSHOT_NAME, dfltCacheCfg, PART_ID);

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertEquals(1, res.exceptions().size());
        assertContains(log, b.toString(), "The check procedure failed on 1 node.");

        Exception ex = res.exceptions().values().iterator().next();
        assertTrue(X.hasCause(ex, IgniteDataIntegrityViolationException.class));
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckFailsOnPartitionDataDiffers() throws Exception {
        CacheConfiguration<Integer, Value> ccfg = txCacheConfig(new CacheConfiguration<Integer, Value>(DEFAULT_CACHE_NAME))
            .setAffinity(new RendezvousAffinityFunction(false, 1));

        IgniteEx ignite = startGridsWithoutCache(2);

        ignite.getOrCreateCache(ccfg).put(1, new Value(new byte[2000]));

        forceCheckpoint(ignite);

        GridCacheSharedContext<?, ?> cctx = ignite.context().cache().context();
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cctx.database();

        BinaryContext binCtx = ((CacheObjectBinaryProcessorImpl)ignite.context().cacheObjects()).binaryContext();

        GridCacheAdapter<?, ?> cache = ignite.context().cache().internalCache(dfltCacheCfg.getName());
        long partCtr = cache.context().offheap().lastUpdatedPartitionCounter(PART_ID);
        AtomicBoolean done = new AtomicBoolean();

        db.addCheckpointListener(new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // Change the cache value only at on of the cluster node to get hash conflict when the check command ends.
                if (!done.compareAndSet(false, true))
                    return;

                GridIterator<CacheDataRow> it = cache.context().offheap().partitionIterator(PART_ID);

                assertTrue(it.hasNext());

                CacheDataRow row0 = it.nextX();

                AffinityTopologyVersion topVer = cctx.exchange().readyAffinityVersion();
                GridCacheEntryEx cached = cache.entryEx(row0.key(), topVer);

                byte[] bytes = new byte[2000];
                new Random().nextBytes(bytes);

                try {
                    BinaryObjectImpl newVal = new BinaryObjectImpl(binCtx, binCtx.marshaller().marshal(new Value(bytes)), 0);

                    boolean success = cached.initialValue(
                        newVal,
                        new GridCacheVersion(row0.version().topologyVersion(),
                            row0.version().nodeOrder(),
                            row0.version().order() + 1),
                        null,
                        null,
                        TxState.NA,
                        TxState.NA,
                        TTL_ETERNAL,
                        row0.expireTime(),
                        true,
                        topVer,
                        DR_NONE,
                        false,
                        null);

                    assertTrue(success);

                    long newPartCtr = cache.context().offheap().lastUpdatedPartitionCounter(PART_ID);

                    assertEquals(newPartCtr, partCtr);
                }
                catch (Exception e) {
                    throw new IgniteCheckedException(e);
                }
            }

            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }

            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }
        });

        db.waitForCheckpoint("test-checkpoint");

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        Path part0 = U.searchFileRecursively(snp(ignite).snapshotLocalDir(SNAPSHOT_NAME).toPath(),
            getPartitionFileName(PART_ID));

        assertNotNull(part0);
        assertTrue(part0.toString(), part0.toFile().exists());

        IdleVerifyResultV2 res = snp(ignite).checkSnapshot(SNAPSHOT_NAME).get();

        StringBuilder b = new StringBuilder();
        res.print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertContains(log, b.toString(),
            "The check procedure has finished, found 1 conflict partitions: [counterConflicts=0, hashConflicts=1]");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckHashesSameAsIdleVerifyHashes() throws Exception {
        Random rnd = new Random();
        CacheConfiguration<Integer, Value> ccfg = txCacheConfig(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        IgniteEx ignite = startGridsWithCache(1, CACHE_KEYS_RANGE, k -> new Value(new byte[rnd.nextInt(32768)]), ccfg);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        IdleVerifyResultV2 idleVerifyRes = ignite.compute().execute(new TestVisorBackupPartitionsTask(),
            new VisorIdleVerifyTaskArg(new HashSet<>(singletonList(ccfg.getName())),
            new HashSet<>(),
            false,
            CacheFilterEnum.USER,
            true));

        IdleVerifyResultV2 snpVerifyRes = ignite.compute().execute(new TestSnapshotPartitionsVerifyTask(),
            new SnapshotPartitionsVerifyTaskArg(new HashSet<>(), Collections.singletonMap(ignite.cluster().localNode(),
                Collections.singletonList(snp(ignite).readSnapshotMetadata(SNAPSHOT_NAME,
                    (String)ignite.configuration().getConsistentId())))))
            .idleVerifyResult();

        Map<PartitionKeyV2, List<PartitionHashRecordV2>> idleVerifyHashes = jobResults.get(TestVisorBackupPartitionsTask.class);
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> snpCheckHashes = jobResults.get(TestVisorBackupPartitionsTask.class);

        assertFalse(F.isEmpty(idleVerifyHashes));
        assertFalse(F.isEmpty(snpCheckHashes));

        assertEquals(idleVerifyHashes, snpCheckHashes);
        assertEquals(idleVerifyRes, snpVerifyRes);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithTwoCachesCheckNullInput() throws Exception {
        SnapshotPartitionsVerifyTaskResult res = checkSnapshotWithTwoCachesWhenOneIsCorrupted(null);

        StringBuilder b = new StringBuilder();
        res.idleVerifyResult().print(b::append, true);

        assertFalse(F.isEmpty(res.exceptions()));
        assertNotNull(res.metas());
        assertContains(log, b.toString(), "The check procedure failed on 1 node.");
        assertContains(log, b.toString(), "Failed to read page (CRC validation failed)");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithTwoCachesCheckNotCorrupted() throws Exception {
        SnapshotPartitionsVerifyTaskResult res = checkSnapshotWithTwoCachesWhenOneIsCorrupted(Collections.singletonList(
            OPTIONAL_CACHE_NAME));

        StringBuilder b = new StringBuilder();
        res.idleVerifyResult().print(b::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertNotNull(res.metas());
        assertContains(log, b.toString(), "The check procedure has finished, no conflicts have been found");
        assertNotContains(log, b.toString(), "Failed to read page (CRC validation failed)");
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotCheckWithTwoCachesCheckTwoCaches() throws Exception {
        SnapshotPartitionsVerifyTaskResult res = checkSnapshotWithTwoCachesWhenOneIsCorrupted(Arrays.asList(
            OPTIONAL_CACHE_NAME, DEFAULT_CACHE_NAME));

        StringBuilder b = new StringBuilder();
        res.idleVerifyResult().print(b::append, true);

        assertFalse(F.isEmpty(res.exceptions()));
        assertNotNull(res.metas());
        assertContains(log, b.toString(), "The check procedure failed on 1 node.");
        assertContains(log, b.toString(), "Failed to read page (CRC validation failed)");
    }

    /**
     * @param cls Class of running task.
     * @param results Results of compute.
     */
    private void saveHashes(Class<?> cls, List<ComputeJobResult> results) {
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashes = new HashMap<>();

        for (ComputeJobResult job : results) {
            if (job.getException() != null)
                continue;

            job.<Map<PartitionKeyV2, PartitionHashRecordV2>>getData().forEach((k, v) ->
                hashes.computeIfAbsent(k, k0 -> new ArrayList<>()).add(v));
        }

        Object mustBeNull = jobResults.putIfAbsent(cls, hashes);

        assertNull(mustBeNull);
    }

    /**
     * @param cachesToCheck Cache names to check.
     * @return Check result.
     * @throws Exception If fails.
     */
    private SnapshotPartitionsVerifyTaskResult checkSnapshotWithTwoCachesWhenOneIsCorrupted(
        Collection<String> cachesToCheck
    ) throws Exception {
        Random rnd = new Random();
        CacheConfiguration<Integer, Value> ccfg1 = txCacheConfig(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
        CacheConfiguration<Integer, Value> ccfg2 = txCacheConfig(new CacheConfiguration<>(OPTIONAL_CACHE_NAME));

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, k -> new Value(new byte[rnd.nextInt(32768)]),
            ccfg1, ccfg2);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        corruptPartitionFile(ignite, SNAPSHOT_NAME, ccfg1, PART_ID);

        return snp(ignite).checkSnapshot(SNAPSHOT_NAME, cachesToCheck).get(TIMEOUT);
    }

    /**
     * @param ignite Ignite instance.
     * @param snpName Snapshot name.
     * @param ccfg Cache configuration.
     * @param partId Partition id to corrupt.
     * @throws IgniteCheckedException If fails.
     * @throws IOException If partition file failed to be changed.
     */
    private static void corruptPartitionFile(
        IgniteEx ignite,
        String snpName,
        CacheConfiguration<?, ?> ccfg,
        int partId
    ) throws IgniteCheckedException, IOException {
        Path cachePath = Paths.get(snp(ignite).snapshotLocalDir(snpName).getAbsolutePath(),
            databaseRelativePath(ignite.context().pdsFolderResolver().resolveFolders().folderName()),
            cacheDirName(ccfg));

        Path part0 = U.searchFileRecursively(cachePath, getPartitionFileName(partId));

        try (FilePageStore pageStore = (FilePageStore)((FilePageStoreManager)ignite.context().cache().context().pageStore())
            .getPageStoreFactory(CU.cacheId(ccfg.getName()), false)
            .createPageStore(getTypeByPartId(partId),
                () -> part0,
                val -> {
                })
        ) {
            ByteBuffer buff = ByteBuffer.allocateDirect(ignite.configuration().getDataStorageConfiguration().getPageSize())
                .order(ByteOrder.nativeOrder());
            pageStore.read(0, buff, false);

            pageStore.beginRecover();

            PageIO.setCrc(buff, 1);

            buff.flip();
            pageStore.write(PageIO.getPageId(buff), buff, 0, false);
            pageStore.finishRecover();
        }
    }

    /** */
    private class TestVisorBackupPartitionsTask extends VerifyBackupPartitionsTaskV2 {
        /** {@inheritDoc} */
        @Override public @Nullable IdleVerifyResultV2 reduce(List<ComputeJobResult> results) throws IgniteException {
            IdleVerifyResultV2 res = super.reduce(results);

            saveHashes(TestVisorBackupPartitionsTask.class, results);

            return res;
        }
    }

    /** Test compute task to collect partition data hashes when the snapshot check procedure ends. */
    private class TestSnapshotPartitionsVerifyTask extends SnapshotPartitionsVerifyTask {
        /** {@inheritDoc} */
        @Override public @Nullable SnapshotPartitionsVerifyTaskResult reduce(List<ComputeJobResult> results) throws IgniteException {
            SnapshotPartitionsVerifyTaskResult res = super.reduce(results);

            saveHashes(TestSnapshotPartitionsVerifyTask.class, results);

            return res;
        }
    }
}
