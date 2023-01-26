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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.WalTestUtils;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTask;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyRepairTaskArg;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyTaskResult;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.cache.Cache;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER;

/** */
public class IncrementalSnapshotRestoreTest extends AbstractIncrementalSnapshotTest {
    /** */
    private static final Random RND = new Random();

    /** Bound max key value put in cache (to make intersections of data between snapshots). */
    private static final int BOUND = 1_000;

    /** */
    private static final int PARTS = 10;

    /** */
    // TODO: separate test for that.
    private static final String CACHE2 = CACHE + "2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setIncludeEventTypes(EVT_CONSISTENCY_VIOLATION);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE), cacheConfiguration(CACHE2));

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(nodes());

        grid(0).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Override protected CacheConfiguration<Integer, Integer> cacheConfiguration(String name) {
        return super.cacheConfiguration(name)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(PARTS));
    }

    /** */
    @Test
    public void testRecoverySnapshotNoData() throws Exception {
        snp(grid(0)).createSnapshot(SNP).get();

        for (int i = 0; i < 2; i++)
            snp(grid(0)).createIncrementalSnapshot(SNP).get();

        for (int i = 1; i <= 2 ; i++) {
            restartWithCleanPersistence();

            snp(grid(0)).restoreIncrementalSnapshot(SNP, null, i).get(getTestTimeout());

            checkData(Collections.emptyMap(), CACHE);
        }
    }

    /** */
    @Test
    public void testRecoveryOnClusterSnapshotOnly() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(expSnpData, 1_000, true);

        restartWithCleanPersistence();

        snp(grid(0)).restoreSnapshot(SNP, null).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testIllegalIncrementalSnapshotIndex() throws Exception {
        loadAndCreateSnapshot();

        restartWithCleanPersistence();

        GridTestUtils.assertThrows(
            log,
            () -> snp(grid(0)).restoreIncrementalSnapshot(SNP, null, -1),
            IllegalArgumentException.class,
            "Incremental snapshot index must be greater than 0.");

        GridTestUtils.assertThrows(
            log,
            () -> snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 0),
            IllegalArgumentException.class,
            "Incremental snapshot index must be greater than 0.");
    }

    /** */
    @Test
    public void testRecoveryOnIncrementalSnapshot() throws Exception {
        Map<Integer, Integer> expSnpData = loadAndCreateSnapshot();

        restartWithCleanPersistence();

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

//    /** */
//    @Test
//    public void testRecoverySingleCacheGroup() throws Exception {
//        Map<Integer, Integer> expSnpData = loadAndCreateSnapshot();
//
//        for (int i = 1; i <= 3; i++) {
//            restartWithCleanPersistence();
//
//            snp(grid(0)).restoreIncrementalSnapshot(SNP, F.asSet(CACHE), i).get(getTestTimeout());
//
//            checkData(3_000, CACHE);
//            assertNull(grid(0).cache(CACHE2));
//        }
//    }

    /** */
    @Test
    public void testRecoverySingleKey() throws Exception {
        Map<Integer, Integer> expSnpData = loadAndCreateSnapshot(null, 1, true);

        restartWithCleanPersistence();

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testNonExistentSnapshotFailed() throws Exception {
        loadAndCreateSnapshot();

        restartWithCleanPersistence();

        GridTestUtils.assertThrows(log, () ->
            snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 2).get(getTestTimeout()),
            IgniteException.class,
            "Incremental snapshot doesn't exists");
    }

    /** */
    @Test
    public void testRecoveryOnClusterSnapshotIfNoWalsOnSingleNode() throws Exception {
        loadAndCreateSnapshot();

        restartWithCleanPersistence();

        File rm = Paths.get(U.defaultWorkDirectory())
            .resolve(DFLT_SNAPSHOT_DIRECTORY)
            .resolve(SNP)
            .resolve(IgniteSnapshotManager.INC_SNP_DIR)
            .resolve(U.maskForFileName(getTestIgniteInstanceName(1)))
            .resolve("0000000000000001")
            .resolve("0000000000000000.wal.zip")
            .toFile();

        assertTrue(U.delete(rm));

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(0).context().cache().context().snapshotMgr()
                 .restoreSnapshot(SNP, null, null, 1)
                 .get(),
            IgniteCheckedException.class, "No WAL segments found for incremental snapshot");

        assertNull(grid(0).cache(CACHE));
        assertNull(grid(0).cache(CACHE2));
    }

    /** */
    @Test
    public void testFailedOnCorruptedWalSegment() throws Exception {
        loadAndCreateSnapshot();

        restartWithCleanPersistence();

        corruptIncrementalSnapshot(1, 1);

        GridTestUtils.assertThrows(log,
            () -> grid(0).context().cache().context().snapshotMgr()
                .restoreSnapshot(SNP, null, null, 1)
                .get(),
            IgniteException.class, null);

        assertNull(grid(0).cache(CACHE));
        assertNull(grid(0).cache(CACHE2));
    }

    /** */
    @Test
    public void testIgnoresInconsistentSnapshot() throws Exception {
        Ignite cln = startClientGrid(nodes());

        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(expSnpData, 1_000, false);

        loadData(CACHE, expSnpData, 1_000);

        TestRecordingCommunicationSpi.spi(cln).blockMessages((n, msg) -> msg instanceof GridNearTxFinishRequest);

        // Transaction data will be part of next incremental snapshot.
        runTxAsync(cln, expSnpData);

        TestRecordingCommunicationSpi.spi(cln).waitForBlocked();

        IgniteFuture<Void> incSnpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

        // Wait for incremental snapshot started.
        assertTrue(GridTestUtils
            .waitForCondition(() -> snp(grid(0)).incrementalSnapshotId() != null, getTestTimeout(), 10));

        stopGrid(nodes());

        GridTestUtils.assertThrows(log, () -> incSnpFut.get(), IgniteException.class, "Incremental snapshot is inconsistent");

        loadData(CACHE, expSnpData, 1);

        snp(grid(0)).createIncrementalSnapshot(SNP).get();

        restartWithCleanPersistence();

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testTransactionInclude() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(expSnpData, 1_000, false);

        loadData(CACHE, expSnpData, 1_000);

        TestRecordingCommunicationSpi.spi(grid(0)).blockMessages((n, msg) -> msg instanceof GridNearTxFinishRequest);

        // Transaction will be included into incremental snapshot.
        runTxAsync(grid(0), expSnpData);

        TestRecordingCommunicationSpi.spi(grid(0)).waitForBlocked();

        IgniteFuture<Void> incSnpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

        // Wait for incremental snapshot started.
        assertTrue(GridTestUtils
            .waitForCondition(() -> snp(grid(0)).incrementalSnapshotId() != null, getTestTimeout(), 10));

        TestRecordingCommunicationSpi.spi(grid(0)).stopBlock();

        incSnpFut.get(getTestTimeout());

        for (int i = 0; i < nodes(); i++) {
            try (WALIterator it = walIter(i)) {
                while (it.hasNext()) {
                    WALRecord rec = it.next().getValue();

                    if (rec.type() == WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD)
                        assertFalse(((IncrementalSnapshotFinishRecord)rec).included().isEmpty());
                }
            }
        }

        restartWithCleanPersistence();

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testTransactionExclude() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(expSnpData, 1_000, false);

        loadData(CACHE, expSnpData, 1_000);

        for (int n = 1; n < nodes(); n++) {
            TestRecordingCommunicationSpi.spi(grid(n))
                .blockMessages((node, msg) -> msg instanceof GridNearTxPrepareResponse);
        }

        // Transaction will be excluded from incremental snapshot.
        runTxAsync(grid(0), null);

        for (int n = 1; n < nodes(); n++)
            TestRecordingCommunicationSpi.spi(grid(n)).waitForBlocked();

        IgniteFuture<Void> incSnpFut = snp(grid(0)).createIncrementalSnapshot(SNP);

        // Wait for incremental snapshot started.
        assertTrue(GridTestUtils
            .waitForCondition(() -> snp(grid(0)).incrementalSnapshotId() != null, getTestTimeout(), 10));

        for (int n = 1; n < nodes(); n++)
            TestRecordingCommunicationSpi.spi(grid(n)).stopBlock();

        incSnpFut.get(getTestTimeout());

        for (int i = 0; i < nodes(); i++) {
            try (WALIterator it = walIter(i)) {
                while (it.hasNext()) {
                    WALRecord rec = it.next().getValue();

                    if (rec.type() == WALRecord.RecordType.INCREMENTAL_SNAPSHOT_FINISH_RECORD)
                        assertFalse(((IncrementalSnapshotFinishRecord)rec).excluded().isEmpty());
                }
            }
        }

        restartWithCleanPersistence();

        snp(grid(0)).restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /**
     * Load and create full and incremental snapshots.
     *
     * @return Cache entries for every snapshot.
     */
    private Map<Integer, Integer> loadAndCreateSnapshot() {
        return loadAndCreateSnapshot(null, 1_000, true);
    }

    /**
     * Load and create full and incremental snapshots.
     *
     * @param fullSnpDataHld Holds full snapshot cache entries.
     * @param opsPerSnp Count of operations per single snapshot.
     * @return Cache entries for every snapshot.
     */
    private Map<Integer, Integer> loadAndCreateSnapshot(
        @Nullable Map<Integer, Integer> fullSnpDataHld,
        int opsPerSnp,
        boolean createIncSnp
    ) {
        Map<Integer, Integer> cache = new HashMap<>();

        loadData(CACHE, cache, opsPerSnp);

        if (fullSnpDataHld != null)
            fullSnpDataHld.putAll(cache);

        snp(grid(0)).createSnapshot(SNP).get();

        if (createIncSnp) {
            loadData(CACHE, cache, opsPerSnp);

            snp(grid(0)).createIncrementalSnapshot(SNP).get();
        }

        return cache;
    }

    /** */
    private void checkData(Map<Integer, Integer> expData, String cacheName) {
        List<Cache.Entry<Integer, Integer>> actData = grid(0).cache(cacheName).query(new ScanQuery<Integer, Integer>()).getAll();

        for (Cache.Entry<Integer, Integer> e: actData)
            assertTrue(expData.remove(e.getKey(), e.getValue()));

        assertTrue(expData.isEmpty());

        // Idle verify - OK.
        for (int i = 0; i < nodes(); i++)
            idleVerify(grid(i));

        // Read repair check - OK.
        AtomicBoolean readRepairCheckFailed = new AtomicBoolean(false);

        grid(0).events().remoteListen(null, (IgnitePredicate<Event>)e -> {
            assert e instanceof CacheConsistencyViolationEvent;

            readRepairCheckFailed.set(true);

            return true;
        }, EVT_CONSISTENCY_VIOLATION);

        Set<Integer> parts = IntStream.range(0, PARTS).boxed().collect(Collectors.toSet());

        VisorConsistencyTaskResult res = grid(0).compute().execute(
            VisorConsistencyRepairTask.class,
            new VisorTaskArgument<>(
                G.allGrids().stream().map(ign -> ign.cluster().localNode().id()).collect(Collectors.toList()),
                new VisorConsistencyRepairTaskArg(cacheName, parts, ReadRepairStrategy.CHECK_ONLY),
                false));

        assertFalse(res.message(), res.cancelled());
        assertFalse(res.message(), res.failed());

        assertFalse(readRepairCheckFailed.get());
    }

    /** Prepare for snapshot restoring - restart grids, with clean persistence. */
    private void restartWithCleanPersistence() throws Exception {
        stopAllGrids();

        assertTrue(U.delete(Paths.get(U.defaultWorkDirectory(), "cp").toFile()));

        deleteNodesDirs(DFLT_STORE_DIR, DFLT_BINARY_METADATA_PATH, DFLT_WAL_PATH, DFLT_WAL_ARCHIVE_PATH);

        startGrids(nodes());

        grid(0).cluster().state(ClusterState.ACTIVE);

        // Caches are configured with IgniteConiguration, need to destroy them before restoring snapshot.
        grid(0).destroyCaches(F.asList(CACHE, CACHE2));

        for (int i = 0; i < nodes(); i++) {
            String nodeFolder = U.maskForFileName(getTestIgniteInstanceName(i));

            Path path = Paths.get(U.defaultWorkDirectory(), DFLT_STORE_DIR, nodeFolder);

            GridTestUtils.waitForCondition(() -> !path.toFile().exists(), 1_000, 10);
        }
    }

    /** */
    private void deleteNodesDirs(String... dirs) throws IgniteCheckedException {
        for (int i = 0; i < nodes(); i++) {
            String nodeFolder = U.maskForFileName(getTestIgniteInstanceName(i));

            for (String dir: dirs)
                assertTrue(U.delete(Paths.get(U.defaultWorkDirectory(), dir, nodeFolder).toFile()));
        }
    }

    /** */
    private void runTxAsync(Ignite txCrdNode, @Nullable Map<Integer, Integer> data) throws Exception {
        multithreadedAsync(() -> {
            try (Transaction tx = txCrdNode.transactions().txStart()) {
                for (int i = 0; i < 10; i++) {
                    while (true) {
                        int key = RND.nextInt(BOUND);
                        int val = RND.nextInt();

                        if (data != null) {
                            if (!data.containsKey(key))
                                continue;

                            data.put(key, val);
                        }

                        txCrdNode.cache(CACHE).put(key, val);

                        break;
                    }
                }

                tx.commit();
            }
        }, 1);
    }

    /**
     * @param cacheName Cache name to load.
     * @param data Map of inserted entries.
     * @param opsCnt Count of operations to load.
     */
    private void loadData(String cacheName, Map<Integer, Integer> data, int opsCnt) {
        IgniteCache<Integer, Integer> cache = grid(0).cache(cacheName);

        int bound = 1000;

        for (int i = 0; i < opsCnt; i++) {
            try (Transaction tx = grid(0).transactions().txStart()) {
                Operation op = Operation.values()[RND.nextInt(Operation.values().length)];

                switch (op) {
                    case PUT:
                        int putKey = RND.nextInt(bound);
                        int putVal = RND.nextInt();

                        data.put(putKey, putVal);

                        cache.put(putKey, putVal);

                        break;

                    case PUT_ALL:
                        int putKey1 = RND.nextInt(bound);
                        int putVal1 = RND.nextInt();

                        int putKey2 = RND.nextInt(bound);
                        int putVal2 = RND.nextInt();

                        data.putAll(F.asMap(putKey1, putVal1, putKey2, putVal2));

                        cache.putAll(F.asMap(putKey1, putVal1, putKey2, putVal2));

                        break;

                    case REMOVE:
                        int rmKey = RND.nextInt(bound);

                        data.remove(rmKey);

                        cache.remove(rmKey);

                        break;

                    case REMOVE_ALL:
                        int rmKey1 = RND.nextInt(bound);
                        int rmKey2 = RND.nextInt(bound);

                        data.remove(rmKey1);
                        data.remove(rmKey2);

                        cache.removeAll(F.asSet(rmKey1, rmKey2));

                        break;
                }

                tx.commit();
            }
        }
    }

    /** Corrupts WAL segment in incremental snapshot. */
    private void corruptIncrementalSnapshot(int nodeIdx, int incIdx) throws Exception {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        File incSnpDir = grid(nodeIdx).context().cache().context().snapshotMgr()
                .incrementalSnapshotLocalDir(SNP, null, incIdx);

        File[] incSegs = incSnpDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs(incSegs[0]);

        try (WALIterator it = factory.iterator(params)) {
            for (int i = 0; i < 400; i++)
                it.next();

            WALPointer corruptPtr = it.next().getKey();

            WalTestUtils.corruptWalSegmentFile(new FileDescriptor(incSegs[0]), corruptPtr);
        }
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 2;
    }

    private enum Operation {
        PUT, PUT_ALL, REMOVE, REMOVE_ALL
    }
}
