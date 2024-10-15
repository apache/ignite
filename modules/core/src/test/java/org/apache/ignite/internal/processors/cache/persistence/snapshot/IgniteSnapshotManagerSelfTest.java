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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static java.util.Objects.nonNull;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.CP_SNAPSHOT_REASON;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_RUNNER_THREAD_PREFIX;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;
import static org.junit.Assume.assumeFalse;

/**
 * Default snapshot manager test.
 */
public class IgniteSnapshotManagerSelfTest extends AbstractSnapshotSelfTest {
    /** The size of value array to fit 3 pages. */
    private static final int SIZE_FOR_FIT_3_PAGES = PAGE_SIZE * 2 + PAGE_SIZE / 2;

    /** Listenning logger. */
    private ListeningTestLogger listenLog;

    /** Number of threads being used to perform snapshot operation. */
    private Integer snapshotThreadPoolSize;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Disable implicit checkpoints for this test to avoid a race if an implicit cp has been scheduled before
        // listener registration and calling snpFutTask.start().
        cfg.getDataStorageConfiguration().setCheckpointFrequency(TimeUnit.DAYS.toMillis(365));

        if (listenLog != null)
            cfg.setGridLogger(listenLog);

        if (nonNull(snapshotThreadPoolSize))
            cfg.setSnapshotThreadPoolSize(snapshotThreadPoolSize);

        return cfg;
    }

    /**
     * Test that all partitions are copied successfully even after multiple checkpoints occur during
     * the long copy of cache partition files.
     *
     * Data consistency checked through a test node started right from snapshot directory and all values
     * read successes.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testSnapshotLocalPartitionMultiCpWithLoad() throws Exception {
        int valMultiplier = 2;
        int afterSnpValMultiplier = 3;

        CountDownLatch slowCopy = new CountDownLatch(1);

        // Start grid node with data before each test.
        IgniteEx ig = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        GridCacheSharedContext<?, ?> cctx = ig.context().cache().context();
        AtomicInteger cntr = new AtomicInteger();
        CountDownLatch ldrLatch = new CountDownLatch(1);
        IgniteSnapshotManager mgr = snp(ig);
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cctx.database();

        IgniteInternalFuture<?> loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                U.await(ldrLatch);

                while (!Thread.currentThread().isInterrupted())
                    ig.cache(DEFAULT_CACHE_NAME).put(cntr.incrementAndGet(),
                        new Account(cntr.incrementAndGet(), cntr.incrementAndGet()));
            }
            catch (IgniteInterruptedCheckedException e) {
                log.warning("Loader has been interrupted", e);
            }
        }, 5, "cache-loader-");

        // Register task but not schedule it on the checkpoint.
        SnapshotFutureTask snpFutTask = (SnapshotFutureTask)mgr.registerSnapshotTask(SNAPSHOT_NAME,
            null,
            cctx.localNodeId(),
            null,
            F.asMap(CU.cacheId(DEFAULT_CACHE_NAME), null),
            encryption,
            false,
            false,
            false,
            new DelegateSnapshotSender(log, mgr.snapshotExecutorService(), mgr.localSnapshotSenderFactory().apply(SNAPSHOT_NAME, null)) {
                @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    try {
                        U.await(slowCopy);

                        delegate.sendPart0(part, cacheDirName, pair, length);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            });

        db.addCheckpointListener(new CheckpointListener() {
            /** {@inheritDoc} */
            @Override public void beforeCheckpointBegin(Context ctx) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void onMarkCheckpointBegin(Context ctx) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void onCheckpointBegin(Context ctx) {
                Map<Integer, Set<Integer>> processed = GridTestUtils.getFieldValue(snpFutTask,
                    SnapshotFutureTask.class,
                    "processed");

                if (!processed.isEmpty())
                    ldrLatch.countDown();
            }
        });

        try {
            snpFutTask.start();

            // Change data before snapshot creation which must be included into it with correct value multiplier.
            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ig.cache(DEFAULT_CACHE_NAME).put(i, new Account(i, valMultiplier * i));

            // Snapshot is still in the INIT state. beforeCheckpoint has been skipped
            // due to checkpoint already running and we need to schedule the next one
            // right after current will be completed.
            cctx.database().forceCheckpoint(String.format(CP_SNAPSHOT_REASON, SNAPSHOT_NAME));

            snpFutTask.started().get();

            db.forceCheckpoint("snapshot is ready to be created")
                .futureFor(CheckpointState.MARKER_STORED_TO_DISK)
                .get();

            // Change data after snapshot.
            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ig.cache(DEFAULT_CACHE_NAME).put(i, new Account(i, afterSnpValMultiplier * i));

            // Snapshot on the next checkpoint must copy page to delta file before write it to a partition.
            forceCheckpoint(ig);

            slowCopy.countDown();

            snpFutTask.get();
        }
        finally {
            loadFut.cancel();
        }

        // Now can stop the node and check created snapshots.
        stopGrid(0);

        cleanPersistenceDir(ig.name());

        // Start Ignite instance from snapshot directory.
        IgniteEx ig2 = startGridsFromSnapshot(1, SNAPSHOT_NAME);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++) {
            assertEquals("snapshot data consistency violation [key=" + i + ']',
                i * valMultiplier, ((Account)ig2.cache(DEFAULT_CACHE_NAME).get(i)).balance);
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotLocalPartitionNotEnoughSpace() throws Exception {
        String err_msg = "Test exception. Not enough space.";
        AtomicInteger throwCntr = new AtomicInteger();
        RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();

        IgniteEx ig = startGridWithCache(dfltCacheCfg.setAffinity(new ZeroPartitionAffinityFunction()),
            CACHE_KEYS_RANGE);

        // Change data after backup.
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, 2 * i);

        GridCacheSharedContext<?, ?> cctx0 = ig.context().cache().context();

        IgniteSnapshotManager mgr = snp(ig);

        mgr.ioFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = ioFactory.create(file, modes);

                if (file.getName().equals(IgniteSnapshotManager.partDeltaFileName(0)))
                    return new FileIODecorator(fileIo) {
                        @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
                            if (throwCntr.incrementAndGet() == 3)
                                throw new IOException(err_msg);

                            return super.writeFully(srcBuf);
                        }
                    };

                return fileIo;
            }
        });

        IgniteInternalFuture<?> snpFut = startLocalSnapshotTask(cctx0,
            SNAPSHOT_NAME,
            F.asMap(CU.cacheId(DEFAULT_CACHE_NAME), null),
            encryption,
            mgr.localSnapshotSenderFactory().apply(SNAPSHOT_NAME, null));

        // Check the right exception thrown.
        assertThrowsAnyCause(log,
            snpFut::get,
            IOException.class,
            err_msg);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotCreateLocalCopyPartitionFail() throws Exception {
        String err_msg = "Test. Fail to copy partition: ";
        IgniteEx ig = startGridWithCache(dfltCacheCfg, CACHE_KEYS_RANGE);

        Map<Integer, Set<Integer>> parts = new HashMap<>();
        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), new HashSet<>(Collections.singletonList(0)));

        IgniteSnapshotManager mgr0 = snp(ig);

        IgniteInternalFuture<?> fut = startLocalSnapshotTask(ig.context().cache().context(),
            SNAPSHOT_NAME,
            parts,
            encryption,
            new DelegateSnapshotSender(log, mgr0.snapshotExecutorService(),
                mgr0.localSnapshotSenderFactory().apply(SNAPSHOT_NAME, null)) {
                @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    if (pair.getPartitionId() == 0)
                        throw new IgniteException(err_msg + pair);

                    delegate.sendPart0(part, cacheDirName, pair, length);
                }
            });

        assertThrowsAnyCause(log,
            fut::get,
            IgniteException.class,
            err_msg);
    }

    /** @throws Exception If fails. */
    @Test(expected = IgniteCheckedException.class)
    public void testLocalSnapshotOnCacheStopped() throws Exception {
        IgniteEx ig = startGridWithCache(dfltCacheCfg, CACHE_KEYS_RANGE);

        startGrid(1);

        ig.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        GridCacheSharedContext<?, ?> cctx0 = ig.context().cache().context();
        IgniteSnapshotManager mgr = snp(ig);

        CountDownLatch cpLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> snpFut = startLocalSnapshotTask(cctx0,
            SNAPSHOT_NAME,
            F.asMap(CU.cacheId(DEFAULT_CACHE_NAME), null),
            encryption,
            new DelegateSnapshotSender(log, mgr.snapshotExecutorService(), mgr.localSnapshotSenderFactory().apply(SNAPSHOT_NAME, null)) {
                @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    try {
                        U.await(cpLatch);

                        delegate.sendPart0(part, cacheDirName, pair, length);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            });

        IgniteCache<?, ?> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.destroy();

        cpLatch.countDown();

        snpFut.get(5_000, TimeUnit.MILLISECONDS);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotIteratorRandomizedLoader() throws Exception {
        Random rnd = new Random();
        int maxKey = 15_000;
        int maxValSize = 32_768;
        int loadingTimeMs = 30_000;

        CacheConfiguration<Integer, Value> ccfg = txCacheConfig(new CacheConfiguration<Integer, Value>("tx1"))
            .setAffinity(new RendezvousAffinityFunction(false, 1));

        IgniteEx ignite = startGridsWithCache(1, CACHE_KEYS_RANGE, k -> new Value(new byte[1024]), ccfg);

        IgniteCache<Integer, Value> cache = ignite.cache(ccfg.getName());

        long startTime = U.currentTimeMillis();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!Thread.currentThread().isInterrupted() && startTime + loadingTimeMs > U.currentTimeMillis()) {
                if (rnd.nextBoolean())
                    cache.put(rnd.nextInt(maxKey), new Value(new byte[rnd.nextInt(maxValSize)]));
                else
                    cache.remove(rnd.nextInt(maxKey));
            }

        }, 10, "change-loader-");

        fut.get();

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        Map<Integer, Value> iterated = new HashMap<>();

        try (GridCloseableIterator<CacheDataRow> iter = snp(ignite).partitionRowIterator(SNAPSHOT_NAME,
            ignite.context().pdsFolderResolver().resolveFolders().folderName(),
            ccfg.getName(),
            0,
            ignite.context().encryption())
        ) {
            CacheObjectContext coctx = ignite.cachex(ccfg.getName()).context().cacheObjectContext();

            while (iter.hasNext()) {
                CacheDataRow row = iter.next();

                iterated.put(row.key().value(coctx, true), row.value().value(coctx, true));
            }
        }

        stopAllGrids();

        IgniteEx snpIgnite = startGridsFromSnapshot(1, SNAPSHOT_NAME);

        IgniteCache<Integer, Value> snpCache = snpIgnite.cache(ccfg.getName());

        assertEquals(snpCache.size(CachePeekMode.PRIMARY), iterated.size());
        snpCache.forEach(e -> {
            Value val = iterated.remove(e.getKey());

            assertNotNull(val);
            assertEquals(val.arr().length, e.getValue().arr().length);
        });

        assertTrue(iterated.isEmpty());
    }

    /** @throws Exception If fails */
    @Test
    public void testSnapshotIterator() throws Exception {
        int keys = 127;

        IgniteEx ignite = startGridsWithCache(2,
            dfltCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 1)), keys);

        ignite = grid(ignite.affinity(dfltCacheCfg.getName()).mapPartitionToNode(0));

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        int rows = 0;

        try (GridCloseableIterator<CacheDataRow> iter = snp(ignite).partitionRowIterator(SNAPSHOT_NAME,
            ignite.context().pdsFolderResolver().resolveFolders().folderName(),
            dfltCacheCfg.getName(),
            0,
            ignite.context().encryption())
        ) {
            CacheObjectContext coctx = ignite.cachex(dfltCacheCfg.getName()).context().cacheObjectContext();

            while (iter.hasNext()) {
                CacheDataRow row = iter.next();

                // Invariant for cache: cache key always equals to cache value.
                assertEquals("Invalid key/value pair [key=" + row.key() + ", val=" + row.value() + ']',
                    row.key().value(coctx, false, U.resolveClassLoader(ignite.configuration())),
                    (Integer)row.value().value(coctx, false));

                rows++;
            }
        }

        assertEquals("Invalid number of rows: " + rows, keys, rows);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotIteratorLargeRows() throws Exception {
        int keys = 2;
        CacheConfiguration<Integer, Value> ccfg = txCacheConfig(new CacheConfiguration<Integer, Value>(DEFAULT_CACHE_NAME))
            .setAffinity(new RendezvousAffinityFunction(false, 1));

        IgniteEx ignite = startGridsWithoutCache(2);

        assertEquals(PAGE_SIZE, ignite.configuration().getDataStorageConfiguration().getPageSize());

        for (int i = 0; i < keys; i++)
            ignite.getOrCreateCache(ccfg).put(i, new Value(new byte[SIZE_FOR_FIT_3_PAGES]));

        int part = 0;

        ignite = grid(ignite.affinity(ccfg.getName()).mapPartitionToNode(part));

        forceCheckpoint();

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME);

        int rows = 0;

        try (GridCloseableIterator<CacheDataRow> iter = snp(ignite).partitionRowIterator(SNAPSHOT_NAME,
            ignite.context().pdsFolderResolver().resolveFolders().folderName(),
            dfltCacheCfg.getName(),
            part,
            ignite.context().encryption())
        ) {
            CacheObjectContext coctx = ignite.cachex(dfltCacheCfg.getName()).context().cacheObjectContext();

            while (iter.hasNext()) {
                CacheDataRow row = iter.next();

                assertEquals(SIZE_FOR_FIT_3_PAGES, ((Value)row.value().value(coctx, false)).arr().length);
                assertTrue((Integer)row.key().value(coctx, false, null) < 2);

                rows++;
            }
        }

        assertEquals("Invalid number of rows: " + rows, keys, rows);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotAlwaysStartsNewCheckpoint() throws Exception {
        long testTimeout = 30_000;

        listenLog = new ListeningTestLogger(log);

        LogListener lsnr = LogListener.matches("Snapshot operation is scheduled on local node").times(1).build();

        listenLog.registerListener(lsnr);

        IgniteEx ignite = startGridsWithCache(1, 4096, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        assertTrue("Test requires that only forced checkpoints were allowed.",
            ignite.configuration().getDataStorageConfiguration().getCheckpointFrequency() >= TimeUnit.DAYS.toMillis(365));

        GridCacheDatabaseSharedManager dbMgr =
            ((GridCacheDatabaseSharedManager)ignite.context().cache().context().database());

        // Ensure that previous checkpoint finished.
        dbMgr.getCheckpointer().currentProgress().futureFor(CheckpointState.FINISHED).get(testTimeout);

        CountDownLatch beforeCpEnter = new CountDownLatch(1);
        CountDownLatch beforeCpExit = new CountDownLatch(1);

        // Block checkpointer on start.
        dbMgr.addCheckpointListener(new CheckpointListener() {
            @Override public void beforeCheckpointBegin(CheckpointListener.Context ctx) throws IgniteCheckedException {
                beforeCpEnter.countDown();

                U.await(beforeCpExit, testTimeout, TimeUnit.MILLISECONDS);
            }

            @Override public void onMarkCheckpointBegin(CheckpointListener.Context ctx) {
                // No-op.
            }

            @Override public void onCheckpointBegin(CheckpointListener.Context ctx) {
                // No-op.
            }
        });

        dbMgr.forceCheckpoint("snapshot-task-hang-test");

        beforeCpEnter.await(testTimeout, TimeUnit.MILLISECONDS);

        IgniteFuture<Void> snpFut = snp(ignite).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

        // Wait until the snapshot task checkpoint listener is registered.
        assertTrue(GridTestUtils.waitForCondition(lsnr::check, testTimeout));

        // Unblock checkpointer.
        beforeCpExit.countDown();

        // Make sure the snapshot has been taken.
        snpFut.get(testTimeout);

        checkSnapshot(SNAPSHOT_NAME, null);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotThreadPoolSizeUsage() throws Exception {
        snapshotThreadPoolSize = 6;

        IgniteEx ig = startGridWithCache(dfltCacheCfg, CACHE_KEYS_RANGE);

        createAndCheckSnapshot(ig, SNAPSHOT_NAME, null, TIMEOUT);

        ThreadMXBean tMb = ManagementFactory.getThreadMXBean();

        long snpRunningThreads = LongStream.of(tMb.getAllThreadIds()).mapToObj(tMb::getThreadInfo)
            .filter(info -> info.getThreadName().startsWith(SNAPSHOT_RUNNER_THREAD_PREFIX)).count();

        assertEquals(snapshotThreadPoolSize.longValue(), snpRunningThreads);
    }

    /**
     * Tests that full-copy and incremental snapshots log correctly.
     *
     * @throws Exception If fails.
     * */
    @Test
    public void testFullSnapshotCreationLog() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        listenLog = new ListeningTestLogger(log);

        final int entriesCnt = 4;

        LogListener matchStart = LogListener.matches("Cluster-wide snapshot operation started: ").times(entriesCnt).build();
        listenLog.registerListener(matchStart);

        LogListener matchFinish = LogListener.matches("Cluster-wide snapshot operation finished successfully: ").times(entriesCnt).build();
        listenLog.registerListener(matchFinish);

        LogListener matchFullParams = LogListener.matches("incremental=false, incIdx=-1").times(2).build();
        listenLog.registerListener(matchFullParams);

        LogListener matchIncParams = LogListener.matches("incremental=true").times(2 * (entriesCnt - 1)).build();
        listenLog.registerListener(matchIncParams);

        LogListener noMatchParams = LogListener.matches("incremental=true, incIdx=-1").build();
        listenLog.registerListener(noMatchParams);

        IgniteEx ignite = startGrid(getConfiguration().setConsistentId(null));
        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        cache.put(0, 0);
        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get();
        for (int i = 1; i < entriesCnt; i++) {
            cache.put(i, i);
            ignite.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get();
        }

        ignite.close();

        assertTrue(matchStart.check());
        assertTrue(matchFinish.check());
        assertTrue(matchFullParams.check());
        assertTrue(matchIncParams.check());
        assertFalse(noMatchParams.check());
    }

    /**
     * @param ignite Ignite instance to set factory.
     * @param factory New factory to use.
     */
    private static void snapshotStoreFactory(IgniteEx ignite, BiFunction<Integer, Boolean, FileVersionCheckingFactory> factory) {
        setFieldValue(snp(ignite), "storeFactory", factory);
    }

    /** */
    private static class ZeroPartitionAffinityFunction extends RendezvousAffinityFunction {
        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return 0;
        }
    }
}
