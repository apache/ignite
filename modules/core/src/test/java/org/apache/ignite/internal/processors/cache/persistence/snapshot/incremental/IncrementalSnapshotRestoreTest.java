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
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.WalTestUtils;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotMetadata;
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
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
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
    private static volatile Runnable fail;

    /** */
    private static final String CACHE2 = CACHE + "2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setIncludeEventTypes(EVT_CONSISTENCY_VIOLATION);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE), cacheConfiguration(CACHE2));

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (getTestIgniteInstanceIndex(instanceName) == 1)
            cfg.setPluginProviders(new FailedIgniteSnapshotManagerProvider());

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
        grid(0).snapshot().createSnapshot(SNP).get();

        for (int i = 0; i < 2; i++)
            grid(0).snapshot().createIncrementalSnapshot(SNP).get();

        for (int i = 1; i <= 2; i++) {
            restartWithCleanPersistence();

            grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, i).get(getTestTimeout());

            checkData(Collections.emptyMap(), CACHE);
        }
    }

    /** */
    @Test
    public void testRecoveryOnClusterSnapshotOnly() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(true, (incSnp) -> {
            Map<Integer, Integer> data = incSnp ? new HashMap<>() : expSnpData;

            loadData(CACHE, data, 1_000);
        });

        restartWithCleanPersistence();

        grid(0).snapshot().restoreSnapshot(SNP, null).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testIllegalIncrementalSnapshotIndex() throws Exception {
        loadAndCreateSnapshot(true, (incSnp) -> loadData(CACHE, new HashMap<>(), 1));

        restartWithCleanPersistence();

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, -1),
            IllegalArgumentException.class,
            "Incremental snapshot index must be greater than 0.");

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 0),
            IllegalArgumentException.class,
            "Incremental snapshot index must be greater than 0.");
    }

    /** */
    @Test
    public void testRecoveryOnIncrementalSnapshot() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(true, (incSnp) -> loadData(CACHE, expSnpData, 1_000));

        restartWithCleanPersistence();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testRecoveryOnIncrementalSnapshotWithMultipleSegments() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(true, (incSnp) -> {
            loadData(CACHE, expSnpData, 1_000);

            if (incSnp) {
                for (int i = 0; i < 3; i++) {
                    loadData(CACHE, expSnpData, 1_000);

                    rollWalSegment(grid(RND.nextInt(nodes())));
                }
            }
        });

        restartWithCleanPersistence();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testRecoveryOnLastIncrementalSnapshot() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(true, (incSnp) -> loadData(CACHE, expSnpData, 1_000));

        loadData(CACHE, expSnpData, 1_000);

        grid(0).snapshot().createIncrementalSnapshot(SNP).get(getTestTimeout());

        restartWithCleanPersistence();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 2).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testRecoverySingleCacheGroup() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(true, (incSnp) -> {
            for (int i = 0; i < 1_000; i++) {
                try (Transaction tx = grid(0).transactions().txStart()) {
                    int key = (incSnp ? 1_000 : 0) + i;

                    grid(0).cache(CACHE).put(key, i);
                    grid(0).cache(CACHE2).put(key, i);

                    expSnpData.put(key, i);

                    tx.commit();
                }
            }
        });

        grid(0).snapshot().createIncrementalSnapshot(SNP).get();

        restartWithCleanPersistence();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, Collections.singleton(CACHE), 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);

        assertNoCaches(Collections.singleton(CACHE2));

        restartWithCleanPersistence();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, Collections.singleton(CACHE2), 1).get(getTestTimeout());

        checkData(expSnpData, CACHE2);

        assertNoCaches(Collections.singleton(CACHE));
    }

    /** */
    @Test
    public void testRecoverySingleKey() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(true, (incSnp) -> loadData(CACHE, expSnpData, 1));

        restartWithCleanPersistence();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testNonExistentSnapshotFailed() throws Exception {
        loadAndCreateSnapshot(true, (incSnp) -> loadData(CACHE, new HashMap<>(), 1));

        restartWithCleanPersistence();

        GridTestUtils.assertThrowsAnyCause(log, () ->
            grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 2).get(getTestTimeout()),
            IgniteException.class,
            "Incremental snapshot doesn't exists");
    }

    /** */
    @Test
    public void testRecoveryOnClusterSnapshotIfNoWalsOnSingleNode() throws Exception {
        loadAndCreateSnapshot(true, (incSnp) -> loadData(CACHE, new HashMap<>(), 1_000));

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
            () -> grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(),
            IgniteCheckedException.class,
            "No WAL segments found for incremental snapshot");

        awaitPartitionMapExchange();

        assertNoCaches(F.asList(CACHE, CACHE2));
    }

    /** */
    @Test
    public void testFailedOnCorruptedWalSegment() throws Exception {
        loadAndCreateSnapshot(true, (incSnp) -> loadData(CACHE, new HashMap<>(), 1_000));

        restartWithCleanPersistence();

        corruptIncrementalSnapshot(1, 1, 0);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(),
            IgniteException.class, "System WAL record for incremental snapshot wasn't found");

        awaitPartitionMapExchange();

        assertNoCaches(F.asList(CACHE, CACHE2));
    }

    /** */
    @Test
    public void testFailedOnCorruptedIntermediateWalSegment() throws Exception {
        int crptNodeIdx = 1;

        loadAndCreateSnapshot(true, (incSnp) -> {
            loadData(CACHE, new HashMap<>(), 1_000);

            if (incSnp) {
                // Prepare incremental snapshot of 3 segments.
                for (int i = 0; i < 3; i++) {
                    // Load data after ClusterSnapshotRecord.
                    loadData(CACHE, new HashMap<>(), 1_000);

                    rollWalSegment(grid(crptNodeIdx));
                }

                loadData(CACHE, new HashMap<>(), 1_000);
            }
        });

        restartWithCleanPersistence();

        corruptIncrementalSnapshot(crptNodeIdx, 1, 1);

        Throwable ex = GridTestUtils.assertThrows(log,
            () -> grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(),
            Throwable.class, null);

        boolean expExc = false;

        // Corrupted WAL segment leads to different errors.
        if (ex instanceof IgniteException) {
            if (ex.getMessage().contains("Failed to read WAL record at position")
                || ex.getMessage().contains("WAL tail reached not in the last available segment")) {
                expExc = true;
            }
        }
        else if (ex instanceof AssertionError) {
            expExc = true;
        }

        assertTrue(ex.getMessage(), expExc);

        awaitPartitionMapExchange();

        assertNoCaches(F.asList(CACHE, CACHE2));
    }

    /** */
    @Test
    public void testIgnoresInconsistentSnapshot() throws Exception {
        Ignite cln = startClientGrid(nodes());

        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(false, (incSnp) -> loadData(CACHE, expSnpData, 1_000));

        loadData(CACHE, expSnpData, 1_000);

        TestRecordingCommunicationSpi.spi(cln).blockMessages((n, msg) -> msg instanceof GridNearTxFinishRequest);

        // Transaction data will be part of next incremental snapshot.
        runTxAsync(cln, expSnpData);

        TestRecordingCommunicationSpi.spi(cln).waitForBlocked();

        IgniteFuture<Void> incSnpFut = grid(0).snapshot().createIncrementalSnapshot(SNP);

        // Wait for incremental snapshot started.
        assertTrue(GridTestUtils
            .waitForCondition(() -> snp(grid(0)).incrementalSnapshotId() != null, getTestTimeout(), 10));

        stopGrid(nodes());

        GridTestUtils.assertThrowsAnyCause(log, incSnpFut::get, IgniteException.class, "Incremental snapshot is inconsistent");

        loadData(CACHE, expSnpData, 1);

        grid(0).snapshot().createIncrementalSnapshot(SNP).get();

        restartWithCleanPersistence();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testTransactionInclude() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(false, (incSnp) -> loadData(CACHE, expSnpData, 1_000));

        loadData(CACHE, expSnpData, 1_000);

        TestRecordingCommunicationSpi.spi(grid(0)).blockMessages((n, msg) -> msg instanceof GridNearTxFinishRequest);

        // Transaction will be included into incremental snapshot.
        runTxAsync(grid(0), expSnpData);

        TestRecordingCommunicationSpi.spi(grid(0)).waitForBlocked();

        IgniteFuture<Void> incSnpFut = grid(0).snapshot().createIncrementalSnapshot(SNP);

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

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testTransactionExclude() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(false, (incSnp) -> loadData(CACHE, expSnpData, 1_000));

        loadData(CACHE, expSnpData, 1_000);

        for (int n = 1; n < nodes(); n++) {
            TestRecordingCommunicationSpi.spi(grid(n))
                .blockMessages((node, msg) -> msg instanceof GridNearTxPrepareResponse);
        }

        // Transaction will be excluded from incremental snapshot.
        runTxAsync(grid(0), null);

        for (int n = 1; n < nodes(); n++)
            TestRecordingCommunicationSpi.spi(grid(n)).waitForBlocked();

        IgniteFuture<Void> incSnpFut = grid(0).snapshot().createIncrementalSnapshot(SNP);

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

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testRestoreBinaryObjects() throws Exception {
        Map<BinaryObject, BinaryObject> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(true, (incSnp) -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                BinaryObject key = grid(0).binary().builder("TestKey")
                    .setField("key", incSnp ? 123 : 122)
                    .build();

                BinaryObject val = grid(0).binary().builder("TestVal")
                    .setField("val", 0)
                    .build();

                grid(0).cache(CACHE).put(key, val);

                expSnpData.put(key, val);

                tx.commit();
            }
        });

        restartWithCleanPersistence();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testRestoreFromSecondAttempt() throws Exception {
        fail = () -> {
            throw new RuntimeException("Force to fail snapshot restore.");
        };

        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(true, (incSnp) -> loadData(CACHE, expSnpData, 1_000));

        restartWithCleanPersistence();

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(),
            IgniteException.class, "Force to fail snapshot restore.");

        awaitPartitionMapExchange();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get();

        checkData(expSnpData, CACHE);

        stopAllGrids();

        startGrids(3);

        checkData(expSnpData, CACHE);
    }

    /** */
    @Test
    public void testNoGapsInCountersAfterRestore() throws Exception {
        Map<Integer, Integer> expSnpData = new HashMap<>();

        loadAndCreateSnapshot(false, (incSnp) -> loadData(CACHE, expSnpData, 1_000));

        loadData(CACHE, expSnpData, 1_000);

        final CountDownLatch txIdSetLatch = new CountDownLatch(1);
        final CountDownLatch msgBlkSet = new CountDownLatch(1);

        final AtomicReference<IgniteUuid> exclTxId = new AtomicReference<>();

        multithreadedAsync(() -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                // Use keys out of bound to avoid dead blocking transactions and snapshot while keys are being locked.
                for (int i = 0; i < 10; i++)
                    grid(0).cache(CACHE).put(BOUND + i, 0);

                exclTxId.set(tx.xid());

                txIdSetLatch.countDown();

                U.awaitQuiet(msgBlkSet);

                tx.commit();
            }
        }, 1);

        U.awaitQuiet(txIdSetLatch);

        for (int n = 1; n < nodes(); n++) {
            TestRecordingCommunicationSpi.spi(grid(n)).blockMessages((node, msg) ->
                msg instanceof GridNearTxPrepareResponse
                    && ((GridNearTxPrepareResponse)msg).version().asIgniteUuid().equals(exclTxId.get()));
        }

        msgBlkSet.countDown();

        for (int n = 1; n < nodes(); n++)
            TestRecordingCommunicationSpi.spi(grid(n)).waitForBlocked();

        loadData(CACHE, expSnpData, 100);

        IgniteFuture<Void> incSnpFut = grid(0).snapshot().createIncrementalSnapshot(SNP);

        // Wait for incremental snapshot started.
        assertTrue(GridTestUtils
            .waitForCondition(() -> snp(grid(0)).incrementalSnapshotId() != null, getTestTimeout(), 10));

        for (int n = 1; n < nodes(); n++)
            TestRecordingCommunicationSpi.spi(grid(n)).stopBlock();

        incSnpFut.get(getTestTimeout());

        restartWithCleanPersistence();

        grid(0).snapshot().restoreIncrementalSnapshot(SNP, null, 1).get(getTestTimeout());

        checkData(expSnpData, CACHE);

        for (int i = 0; i < nodes(); i++) {
            for (GridDhtLocalPartition locPart: grid(i).cachex(CACHE).context().topology().localPartitions())
                assertNull(locPart.finalizeUpdateCounters());
        }
    }

    /**
     * Load and create full and incremental snapshots.
     *
     * @param createIncSnp Whether to create incremental snapshot.
     * @param loadData Loads data, consumes stage (for base snapshot {@code false}, for incremental snapshot {@code true}).
     */
    private void loadAndCreateSnapshot(boolean createIncSnp, Consumer<Boolean> loadData) {
        loadData.accept(false);

        grid(0).snapshot().createSnapshot(SNP).get();

        if (createIncSnp) {
            loadData.accept(true);

            grid(0).snapshot().createIncrementalSnapshot(SNP).get();
        }
    }

    /** */
    private void checkData(Map<?, ?> expData, String cacheName) {
        List<Cache.Entry<Object, Object>> actData = grid(0).cache(cacheName).withKeepBinary().query(new ScanQuery<>()).getAll();

        assertEquals(actData.size(), expData.size());

        for (Cache.Entry<Object, Object> e: actData) {
            assertTrue("Missed: " + e, expData.containsKey(e.getKey()));
            assertEquals(e.getValue(), expData.get(e.getKey()));
        }

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

    /** */
    private void runTxAsync(Ignite txCrdNode, @Nullable Map<Integer, Integer> data) throws Exception {
        multithreadedAsync(() -> {
            try (Transaction tx = txCrdNode.transactions().txStart()) {
                for (int i = 0; i < 50; i++) {
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

                        cache.removeAll(new HashSet<>(Arrays.asList(rmKey1, rmKey2)));

                        break;
                }

                tx.commit();
            }
        }
    }

    /** Corrupts WAL segment in incremental snapshot. */
    private void corruptIncrementalSnapshot(int nodeIdx, int incIdx, int segIdx) throws Exception {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        File incSnpDir = grid(nodeIdx).context().cache().context().snapshotMgr()
            .incrementalSnapshotLocalDir(SNP, null, incIdx);

        File[] incSegs = incSnpDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

        Arrays.sort(incSegs);

        File crptSeg = incSegs[segIdx];

        IgniteWalIteratorFactory.IteratorParametersBuilder params = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .filesOrDirs(crptSeg);

        try (WALIterator it = factory.iterator(params)) {
            for (int i = 0; i < 400; i++)
                it.next();

            WALPointer corruptPtr = it.next().getKey();

            WalTestUtils.corruptWalSegmentFile(new FileDescriptor(incSegs[segIdx]), corruptPtr);
        }
    }

    /** */
    private void restartWithCleanPersistence() throws Exception {
        restartWithCleanPersistence(F.asList(CACHE, CACHE2));
    }

    /** */
    private void assertNoCaches(Collection<String> caches) {
        for (int i = 0; i < nodes(); i++) {
            for (String cache: caches)
                assertNull("[node=" + i + ", cache=" + cache + ']', grid(i).cache(cache));
        }
    }

    /** Rolls WAL segment for specified grid. */
    private void rollWalSegment(IgniteEx g) {
        g.context().cache().context().database().checkpointReadLock();

        try {
            g.context().cache().context().wal().log(new ClusterSnapshotRecord("dummy"), RolloverType.CURRENT_SEGMENT);
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
        finally {
            g.context().cache().context().database().checkpointReadUnlock();
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

    /** */
    private static class FailedIgniteSnapshotManagerProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "FailedIgniteSnapshotManagerProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (IgniteSnapshotManager.class.equals(cls))
                return (T)new FailedIgniteSnapshotManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** */
    private static class FailedIgniteSnapshotManager extends IgniteSnapshotManager {
        /** */
        public FailedIgniteSnapshotManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public IncrementalSnapshotMetadata readIncrementalSnapshotMetadata(
            String snpName,
            @Nullable String snpPath,
            int incIdx
        ) throws IgniteCheckedException, IOException {
            if (fail != null) {
                Runnable f = fail;

                fail = null;

                f.run();
            }

            return super.readIncrementalSnapshotMetadata(snpName, snpPath, incIdx);
        }
    }

    /** */
    private enum Operation {
        /** */
        PUT,

        /** */
        PUT_ALL,

        /** */
        REMOVE,

        /** */
        REMOVE_ALL
    }
}
