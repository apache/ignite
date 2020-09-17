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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.LongJVMPauseDetector;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperation;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.LongJVMPauseDetector.DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.MARKER_STORED_TO_DISK;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.PAGE_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP;

/**
 * Checkpointer object is used for notification on checkpoint begin, predicate is {@link #scheduledCp}<code>.nextCpTs -
 * now > 0 </code>. Method {@link #wakeupForCheckpoint} uses notify, {@link #waitCheckpointEvent} uses wait.
 *
 * Checkpointer is one threaded which means that only one checkpoint at the one moment possible.
 *
 * Checkpoint steps:
 * <p> Take checkpoint write lock(all node load is stopped). </p>
 * <p> Collect all dirty pages from page memory under write lock. </p>
 * <p> Release write lock and write start marker to disk. </p>
 * <p> Start to write dirty pages to disk. </p>
 * <p> Finish the checkpoint and write end marker to disk. </p>
 */
@SuppressWarnings("NakedNotify")
public class Checkpointer extends GridWorker {
    /** Checkpoint started log message format. */
    private static final String CHECKPOINT_STARTED_LOG_FORMAT = "Checkpoint started [" +
        "checkpointId=%s, " +
        "startPtr=%s, " +
        "checkpointBeforeLockTime=%dms, " +
        "checkpointLockWait=%dms, " +
        "checkpointListenersExecuteTime=%dms, " +
        "checkpointLockHoldTime=%dms, " +
        "walCpRecordFsyncDuration=%dms, " +
        "writeCheckpointEntryDuration=%dms, " +
        "splitAndSortCpPagesDuration=%dms, " +
        "%s pages=%d, " +
        "reason='%s']";

    /** Timeout between partition file destroy and checkpoint to handle it. */
    private static final long PARTITION_DESTROY_CHECKPOINT_TIMEOUT = 30 * 1000; // 30 Seconds.

    /** This number of threads will be created and used for parallel sorting. */
    private static final int PARALLEL_SORT_THREADS = Math.min(Runtime.getRuntime().availableProcessors(), 8);

    /** @see IgniteSystemProperties#CHECKPOINT_PARALLEL_SORT_THRESHOLD */
    public static final int DFLT_CHECKPOINT_PARALLEL_SORT_THRESHOLD = 512 * 1024;

    /**
     * Starting from this number of dirty pages in checkpoint, array will be sorted with {@link
     * Arrays#parallelSort(Comparable[])} in case of {@link CheckpointWriteOrder#SEQUENTIAL}.
     */
    private final int parallelSortThreshold = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.CHECKPOINT_PARALLEL_SORT_THRESHOLD, DFLT_CHECKPOINT_PARALLEL_SORT_THRESHOLD);

    /** Assertion enabled. */
    private static final boolean ASSERTION_ENABLED = GridCacheDatabaseSharedManager.class.desiredAssertionStatus();

    /** Checkpoint lock hold count. */
    public static final ThreadLocal<Integer> CHECKPOINT_LOCK_HOLD_COUNT = ThreadLocal.withInitial(() -> 0);

    /** Skip sync. */
    private final boolean skipSync = getBoolean(IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);

    /** */
    private final boolean skipCheckpointOnNodeStop = getBoolean(IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, false);

    /** Long JVM pause threshold. */
    private final int longJvmPauseThreshold =
        getInteger(IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD, DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD);

    /** */
    private final boolean printCheckpointStats = true;

    /** Temporary write buffer. */
    private final ByteBuffer tmpWriteBuf;

    /** Pause detector. */
    private final LongJVMPauseDetector pauseDetector;

    /** */
    private final long checkpointFreq;

    /** */
    private final FailureProcessor failureProcessor;

    /** Database configuration. */
    private final DataStorageConfiguration persistenceCfg;

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private final IgniteThreadPoolExecutor asyncRunner;

    /** Snapshot manager. */
    private final IgniteCacheSnapshotManager snapshotMgr;

    /** */
    private final Collection<DbCheckpointListener> lsnrs = new CopyOnWriteArrayList<>();

    /** Checkpoint lock. */
    private final ReentrantReadWriteLock checkpointLock;

    /** */
    private final IgniteWriteAheadLogManager wal;

    /** */
    private final CheckpointHistory cpHistory;

    /** */
    private final DataStorageMetricsImpl persStoreMetrics;

    /** */
    private final Supplier<Collection<DataRegion>> dataRegions;

    /** File I/O factory for writing checkpoint markers. */
    private final FileIOFactory ioFactory;

    /** Checkpoint metadata directory ("cp"), contains files with checkpoint start and end */
    private final File cpDir;

    /** Page size */
    private final int pageSize;

    /** */
    private final GridCacheProcessor cacheProcessor;

    /** */
    private final FilePageStoreManager storeMgr;

    /** */
    private final boolean truncateWalOnCpFinish;

    /** Throttling policy according to the settings. */
    private final PageMemoryImpl.ThrottlingPolicy throttlingPolicy;

    /** Resolver of page memory by group id. */
    private final IgniteThrowableFunction<Integer, PageMemoryEx> pageMemoryGroupResolver;

    /** Thread local with buffers for the checkpoint threads. Each buffer represent one page for durable memory. */
    private volatile ThreadLocal<ByteBuffer> threadBuf;

    /** Next scheduled checkpoint progress. */
    private volatile CheckpointProgressImpl scheduledCp;

    /** Current checkpoint. This field is updated only by checkpoint thread. */
    private volatile CheckpointProgressImpl curCpProgress;

    /** Shutdown now. */
    private volatile boolean shutdownNow;

    /** Last checkpoint timestamp. */
    private long lastCpTs;

    /** Pointer to a memory recovery record that should be included into the next checkpoint record. */
    private volatile WALPointer memoryRecoveryRecordPtr;

    /** For testing only. */
    private GridFutureAdapter<Void> enableChangeApplied;

    /** For testing only. */
    private volatile boolean checkpointsEnabled = true;

    /**
     * @param gridName Grid name.
     * @param name Thread name.
     * @param workersRegistry Worker registry.
     * @param log Logger.
     * @param pageSize Configured page size.
     * @param detector Long JVM pause detector.
     * @param failureProcessor Failure processor.
     * @param dsCfg Data storage configuration.
     * @param checkpointPoolExecutor Checkpoint async runner.
     * @param snapshotManager Snapshot manager.
     * @param checkpointLock Checkpoint lock.
     * @param wal Write ahead log manager.
     * @param cpHistory Checkpoint history.
     * @param dsMetrics Data storage metrics.
     * @param regionsSupplier Checkpoint regions resolver.
     * @param factory Configured IO factory.
     * @param dir Checkpoint directory.
     * @param cacheProcessor Cache processor.
     * @param storeMgr File page store manager.
     * @param truncateWalOnCpFinish Truncate wal on checkpoint finish.
     * @param throttlingPolicy Throttling policy.
     * @param pageMemoryGroupResolver Resolver of page memory by group id.
     * @param buf Thread local with buffers for the checkpoint threads.
     */
    public Checkpointer(
        @Nullable String gridName,
        String name,
        WorkersRegistry workersRegistry,
        IgniteLogger log,
        int pageSize,
        LongJVMPauseDetector detector,
        FailureProcessor failureProcessor,
        DataStorageConfiguration dsCfg,
        @Nullable IgniteThreadPoolExecutor checkpointPoolExecutor,
        IgniteCacheSnapshotManager snapshotManager,
        ReentrantReadWriteLock checkpointLock,
        IgniteWriteAheadLogManager wal,
        CheckpointHistory cpHistory,
        DataStorageMetricsImpl dsMetrics,
        Supplier<Collection<DataRegion>> regionsSupplier,
        FileIOFactory factory, File dir, GridCacheProcessor cacheProcessor,
        FilePageStoreManager storeMgr,
        boolean truncateWalOnCpFinish,
        PageMemoryImpl.ThrottlingPolicy throttlingPolicy,
        IgniteThrowableFunction<Integer, PageMemoryEx> pageMemoryGroupResolver,
        ThreadLocal<ByteBuffer> buf
    ) {
        super(gridName, name, log, workersRegistry);
        this.pauseDetector = detector;
        this.checkpointFreq = dsCfg.getCheckpointFrequency();
        this.failureProcessor = failureProcessor;
        this.persistenceCfg = dsCfg;
        this.asyncRunner = checkpointPoolExecutor;
        this.snapshotMgr = snapshotManager;
        this.checkpointLock = checkpointLock;
        this.wal = wal;
        this.cpHistory = cpHistory;
        this.persStoreMetrics = dsMetrics;
        this.dataRegions = regionsSupplier;
        this.ioFactory = factory;
        this.cpDir = dir;
        this.cacheProcessor = cacheProcessor;
        this.storeMgr = storeMgr;
        this.truncateWalOnCpFinish = truncateWalOnCpFinish;
        this.throttlingPolicy = throttlingPolicy;
        this.pageMemoryGroupResolver = pageMemoryGroupResolver;
        this.threadBuf = buf;
        this.pageSize = pageSize;

        scheduledCp = new CheckpointProgressImpl(checkpointFreq);

        tmpWriteBuf = ByteBuffer.allocateDirect(pageSize);

        tmpWriteBuf.order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override protected void body() {
        Throwable err = null;

        try {
            while (!isCancelled()) {
                waitCheckpointEvent();

                if (skipCheckpointOnNodeStop && (isCancelled() || shutdownNow)) {
                    if (log.isInfoEnabled())
                        log.warning("Skipping last checkpoint because node is stopping.");

                    return;
                }

                GridFutureAdapter<Void> enableChangeApplied = this.enableChangeApplied;

                if (enableChangeApplied != null) {
                    enableChangeApplied.onDone();

                    this.enableChangeApplied = null;
                }

                if (checkpointsEnabled)
                    doCheckpoint();
                else {
                    synchronized (this) {
                        scheduledCp.nextCpNanos(System.nanoTime() + U.millisToNanos(checkpointFreq));
                    }
                }
            }

            // Final run after the cancellation.
            if (checkpointsEnabled && !shutdownNow)
                doCheckpoint();
        }
        catch (Throwable t) {
            err = t;

            scheduledCp.fail(t);

            throw t;
        }
        finally {
            if (err == null && !(isCancelled))
                err = new IllegalStateException("Thread is terminated unexpectedly: " + name());

            if (err instanceof OutOfMemoryError)
                failureProcessor.process(new FailureContext(CRITICAL_ERROR, err));
            else if (err != null)
                failureProcessor.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));

            scheduledCp.fail(new NodeStoppingException("Node is stopping."));
        }
    }

    /**
     *
     */
    public CheckpointProgress wakeupForCheckpoint(long delayFromNow, String reason) {
        return wakeupForCheckpoint(delayFromNow, reason, null);
    }

    /**
     *
     */
    public <R> CheckpointProgress wakeupForCheckpoint(
        long delayFromNow,
        String reason,
        IgniteInClosure<? super IgniteInternalFuture<R>> lsnr
    ) {
        if (lsnr != null) {
            //To be sure lsnr always will be executed in checkpoint thread.
            synchronized (this) {
                CheckpointProgress sched = scheduledCp;

                sched.futureFor(FINISHED).listen(lsnr);
            }
        }

        CheckpointProgressImpl sched = scheduledCp;

        long nextNanos = System.nanoTime() + U.millisToNanos(delayFromNow);

        if (sched.nextCpNanos() <= nextNanos)
            return sched;

        synchronized (this) {
            sched = scheduledCp;

            if (sched.nextCpNanos() > nextNanos) {
                sched.reason(reason);

                sched.nextCpNanos(nextNanos);
            }

            notifyAll();
        }

        return sched;
    }

    /**
     * @param snapshotOperation Snapshot operation.
     */
    public IgniteInternalFuture wakeupForSnapshotCreation(SnapshotOperation snapshotOperation) {
        GridFutureAdapter<Object> ret;

        synchronized (this) {
            scheduledCp.nextCpNanos(System.nanoTime());

            scheduledCp.reason("snapshot");

            scheduledCp.nextSnapshot(true);

            scheduledCp.snapshotOperation(snapshotOperation);

            ret = scheduledCp.futureFor(LOCK_RELEASED);

            notifyAll();
        }

        return ret;
    }

    /**
     *
     */
    private void doCheckpoint() {
        Checkpoint chp = null;

        try {
            CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

            try {
                chp = markCheckpointBegin(tracker);
            }
            catch (Exception e) {
                if (curCpProgress != null)
                    curCpProgress.fail(e);

                // In case of checkpoint initialization error node should be invalidated and stopped.
                failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));

                throw new IgniteException(e); // Re-throw as unchecked exception to force stopping checkpoint thread.
            }

            updateHeartbeat();

            currentProgress().initCounters(chp.pagesSize);

            boolean success = false;

            int destroyedPartitionsCnt;

            try {
                if (chp.hasDelta()) {
                    // Identity stores set.
                    ConcurrentLinkedHashMap<PageStore, LongAdder> updStores = new ConcurrentLinkedHashMap<>();

                    CountDownFuture doneWriteFut = new CountDownFuture(
                        asyncRunner == null ? 1 : persistenceCfg.getCheckpointThreads());

                    tracker.onPagesWriteStart();

                    final int totalPagesToWriteCnt = chp.pagesSize;

                    if (asyncRunner != null) {
                        for (int i = 0; i < persistenceCfg.getCheckpointThreads(); i++) {
                            Runnable write = new WriteCheckpointPages(
                                tracker,
                                chp.cpPages,
                                updStores,
                                doneWriteFut,
                                totalPagesToWriteCnt,
                                this::updateHeartbeat,
                                snapshotMgr,
                                log,
                                persStoreMetrics,
                                threadBuf,
                                throttlingPolicy,
                                pageMemoryGroupResolver,
                                curCpProgress,
                                storeMgr,
                                this::isShutdownNow
                            );

                            try {
                                asyncRunner.execute(write);
                            }
                            catch (RejectedExecutionException ignore) {
                                // Run the task synchronously.
                                updateHeartbeat();

                                write.run();
                            }
                        }
                    }
                    else {
                        // Single-threaded checkpoint.
                        updateHeartbeat();

                        Runnable write = new WriteCheckpointPages(
                            tracker,
                            chp.cpPages,
                            updStores,
                            doneWriteFut,
                            totalPagesToWriteCnt,
                            this::updateHeartbeat,
                            snapshotMgr,
                            log,
                            persStoreMetrics,
                            threadBuf,
                            throttlingPolicy,
                            pageMemoryGroupResolver,
                            curCpProgress,
                            storeMgr,
                            this::isShutdownNow
                        );

                        write.run();
                    }

                    updateHeartbeat();

                    // Wait and check for errors.
                    doneWriteFut.get();

                    // Must re-check shutdown flag here because threads may have skipped some pages.
                    // If so, we should not put finish checkpoint mark.
                    if (shutdownNow) {
                        chp.progress.fail(new NodeStoppingException("Node is stopping."));

                        return;
                    }

                    tracker.onFsyncStart();

                    if (!skipSync) {
                        for (Map.Entry<PageStore, LongAdder> updStoreEntry : updStores.entrySet()) {
                            if (shutdownNow) {
                                chp.progress.fail(new NodeStoppingException("Node is stopping."));

                                return;
                            }

                            blockingSectionBegin();

                            try {
                                updStoreEntry.getKey().sync();
                            }
                            finally {
                                blockingSectionEnd();
                            }

                            currentProgress().updateSyncedPages(updStoreEntry.getValue().intValue());
                        }
                    }
                }
                else {
                    tracker.onPagesWriteStart();
                    tracker.onFsyncStart();
                }

                snapshotMgr.afterCheckpointPageWritten();

                destroyedPartitionsCnt = destroyEvictedPartitions();

                // Must mark successful checkpoint only if there are no exceptions or interrupts.
                success = true;
            }
            finally {
                if (success)
                    markCheckpointEnd(chp);
            }

            tracker.onEnd();

            if (chp.hasDelta() || destroyedPartitionsCnt > 0) {
                if (printCheckpointStats) {
                    if (log.isInfoEnabled()) {
                        String walSegsCoveredMsg = prepareWalSegsCoveredMsg(chp.walSegsCoveredRange);

                        log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                                "walSegmentsCleared=%d, walSegmentsCovered=%s, markDuration=%dms, pagesWrite=%dms, fsync=%dms, " +
                                "total=%dms]",
                            chp.cpEntry != null ? chp.cpEntry.checkpointId() : "",
                            chp.pagesSize,
                            chp.cpEntry != null ? chp.cpEntry.checkpointMark() : "",
                            chp.walFilesDeleted,
                            walSegsCoveredMsg,
                            tracker.markDuration(),
                            tracker.pagesWriteDuration(),
                            tracker.fsyncDuration(),
                            tracker.totalDuration()));
                    }
                }
            }

            updateMetrics(chp, tracker);
        }
        catch (IgniteCheckedException e) {
            if (chp != null)
                chp.progress.fail(e);

            failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }
    }

    /**
     *
     */
    @SuppressWarnings("TooBroadScope")
    private Checkpoint markCheckpointBegin(CheckpointMetricsTracker tracker) throws IgniteCheckedException {
        long cpTs = updateLastCheckpointTime();

        CheckpointProgressImpl curr = scheduledCp;

        List<DbCheckpointListener> dbLsnrs = new ArrayList<>(lsnrs);

        CheckpointRecord cpRec = new CheckpointRecord(memoryRecoveryRecordPtr);

        memoryRecoveryRecordPtr = null;

        CheckpointEntry cp = null;

        IgniteFuture snapFut = null;

        CheckpointPagesInfoHolder cpPagesHolder;

        int dirtyPagesCount;

        boolean hasUserPages, hasPartitionsToDestroy;

        DbCheckpointContextImpl ctx0 = new DbCheckpointContextImpl(curr, new PartitionAllocationMap(), asyncRunner, this::updateHeartbeat);

        internalReadLock();

        try {
            for (DbCheckpointListener lsnr : dbLsnrs)
                lsnr.beforeCheckpointBegin(ctx0);

            ctx0.awaitPendingTasksFinished();
        }
        finally {
            internalReadUnlock();
        }

        tracker.onLockWaitStart();

        checkpointLock.writeLock().lock();

        try {
            updateCurrentCheckpointProgress();

            assert curCpProgress == curr : "Concurrent checkpoint begin should not be happened";

            tracker.onMarkStart();

            // Listeners must be invoked before we write checkpoint record to WAL.
            for (DbCheckpointListener lsnr : dbLsnrs)
                lsnr.onMarkCheckpointBegin(ctx0);

            ctx0.awaitPendingTasksFinished();

            tracker.onListenersExecuteEnd();

            if (curr.nextSnapshot())
                snapFut = snapshotMgr.onMarkCheckPointBegin(curr.snapshotOperation(), ctx0.partitionStatMap());

            fillCacheGroupState(cpRec);

            //There are allowable to replace pages only after checkpoint entry was stored to disk.
            cpPagesHolder = beginAllCheckpoints(curr.futureFor(MARKER_STORED_TO_DISK));

            dirtyPagesCount = cpPagesHolder.pagesNum();

            hasPartitionsToDestroy = !curr.getDestroyQueue().pendingReqs().isEmpty();

            WALPointer cpPtr = null;

            if (dirtyPagesCount > 0 || curr.nextSnapshot() || hasPartitionsToDestroy) {
                // No page updates for this checkpoint are allowed from now on.
                cpPtr = wal.log(cpRec);

                if (cpPtr == null)
                    cpPtr = GridCacheDatabaseSharedManager.CheckpointStatus.NULL_PTR;
            }

            if (dirtyPagesCount > 0 || hasPartitionsToDestroy) {
                cp = prepareCheckpointEntry(
                    tmpWriteBuf,
                    cpTs,
                    cpRec.checkpointId(),
                    cpPtr,
                    cpRec,
                    CheckpointEntryType.START);

                cpHistory.addCheckpoint(cp);
            }

            curr.transitTo(PAGE_SNAPSHOT_TAKEN);
        }
        finally {
            checkpointLock.writeLock().unlock();

            tracker.onLockRelease();
        }

        curr.transitTo(LOCK_RELEASED);

        for (DbCheckpointListener lsnr : dbLsnrs)
            lsnr.onCheckpointBegin(ctx0);

        if (snapFut != null) {
            try {
                snapFut.get();
            }
            catch (IgniteException e) {
                U.error(log, "Failed to wait for snapshot operation initialization: " +
                    curr.snapshotOperation(), e);
            }
        }

        if (dirtyPagesCount > 0 || hasPartitionsToDestroy) {
            assert cp != null;
            assert cp.checkpointMark() != null;

            tracker.onWalCpRecordFsyncStart();

            // Sync log outside the checkpoint write lock.
            wal.flush(cp.checkpointMark(), true);

            tracker.onWalCpRecordFsyncEnd();

            writeCheckpointEntry(tmpWriteBuf, cp, CheckpointEntryType.START);

            curr.transitTo(MARKER_STORED_TO_DISK);

            tracker.onSplitAndSortCpPagesStart();

            GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> cpPages =
                splitAndSortCpPagesIfNeeded(cpPagesHolder);

            tracker.onSplitAndSortCpPagesEnd();

            if (printCheckpointStats && log.isInfoEnabled()) {
                long possibleJvmPauseDur = possibleLongJvmPauseDuration(tracker);

                if (log.isInfoEnabled())
                    log.info(
                        String.format(
                            CHECKPOINT_STARTED_LOG_FORMAT,
                            cpRec.checkpointId(),
                            cp.checkpointMark(),
                            tracker.beforeLockDuration(),
                            tracker.lockWaitDuration(),
                            tracker.listenersExecuteDuration(),
                            tracker.lockHoldDuration(),
                            tracker.walCpRecordFsyncDuration(),
                            tracker.writeCheckpointEntryDuration(),
                            tracker.splitAndSortCpPagesDuration(),
                            possibleJvmPauseDur > 0 ? "possibleJvmPauseDuration=" + possibleJvmPauseDur + "ms," : "",
                            dirtyPagesCount,
                            curr.reason()
                        )
                    );
            }

            return new Checkpoint(cp, cpPages, curr);
        }
        else {
            if (curr.nextSnapshot())
                wal.flush(null, true);

            if (printCheckpointStats) {
                if (log.isInfoEnabled())
                    LT.info(log, String.format("Skipping checkpoint (no pages were modified) [" +
                            "checkpointBeforeLockTime=%dms, checkpointLockWait=%dms, " +
                            "checkpointListenersExecuteTime=%dms, checkpointLockHoldTime=%dms, reason='%s']",
                        tracker.beforeLockDuration(),
                        tracker.lockWaitDuration(),
                        tracker.listenersExecuteDuration(),
                        tracker.lockHoldDuration(),
                        curr.reason()));
            }

            return new Checkpoint(null, GridConcurrentMultiPairQueue.EMPTY, curr);
        }
    }

    /**
     * Reorders list of checkpoint pages and splits them into appropriate number of sublists according to {@link
     * DataStorageConfiguration#getCheckpointThreads()} and {@link DataStorageConfiguration#getCheckpointWriteOrder()}.
     *
     * @param cpPages Checkpoint pages with overall count and user pages info.
     */
    private GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> splitAndSortCpPagesIfNeeded(
        CheckpointPagesInfoHolder cpPages
    ) throws IgniteCheckedException {
        Set<T2<PageMemoryEx, FullPageId[]>> cpPagesPerRegion = new HashSet<>();

        int realPagesArrSize = 0;

        int totalPagesCnt = cpPages.pagesNum();

        for (Map.Entry<PageMemoryEx, GridMultiCollectionWrapper<FullPageId>> regPages : cpPages.cpPages()) {
            FullPageId[] pages = new FullPageId[regPages.getValue().size()];

            int pagePos = 0;

            for (int i = 0; i < regPages.getValue().collectionsSize(); i++) {
                for (FullPageId page : regPages.getValue().innerCollection(i)) {
                    if (realPagesArrSize++ == totalPagesCnt)
                        throw new AssertionError("Incorrect estimated dirty pages number: " + totalPagesCnt);

                    pages[pagePos++] = page;
                }
            }

            // Some pages may have been already replaced.
            if (pagePos != pages.length)
                cpPagesPerRegion.add(new T2<>(regPages.getKey(), Arrays.copyOf(pages, pagePos)));
            else
                cpPagesPerRegion.add(new T2<>(regPages.getKey(), pages));
        }

        if (persistenceCfg.getCheckpointWriteOrder() == CheckpointWriteOrder.SEQUENTIAL) {
            Comparator<FullPageId> cmp = Comparator.comparingInt(FullPageId::groupId)
                .thenComparingLong(FullPageId::effectivePageId);

            ForkJoinPool pool = null;

            for (T2<PageMemoryEx, FullPageId[]> pagesPerReg : cpPagesPerRegion) {
                if (pagesPerReg.getValue().length >= parallelSortThreshold)
                    pool = parallelSortInIsolatedPool(pagesPerReg.get2(), cmp, pool);
                else
                    Arrays.sort(pagesPerReg.get2(), cmp);
            }

            if (pool != null)
                pool.shutdown();
        }

        return new GridConcurrentMultiPairQueue<>(cpPagesPerRegion);
    }

    /**
     * Performs parallel sort in isolated fork join pool.
     *
     * @param pagesArr Pages array.
     * @param cmp Cmp.
     * @return ForkJoinPool instance, check {@link ForkJoinTask#fork()} realization.
     */
    private static ForkJoinPool parallelSortInIsolatedPool(
        FullPageId[] pagesArr,
        Comparator<FullPageId> cmp,
        ForkJoinPool pool
    ) throws IgniteCheckedException {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = new ForkJoinPool.ForkJoinWorkerThreadFactory() {
            @Override public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);

                worker.setName("checkpoint-pages-sorter-" + worker.getPoolIndex());

                return worker;
            }
        };

        ForkJoinPool execPool = pool == null ?
            new ForkJoinPool(PARALLEL_SORT_THREADS + 1, factory, null, false) : pool;

        Future<?> sortTask = execPool.submit(() -> Arrays.parallelSort(pagesArr, cmp));

        try {
            sortTask.get();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }
        catch (ExecutionException e) {
            throw new IgniteCheckedException("Failed to perform pages array parallel sort", e.getCause());
        }

        return execPool;
    }

    /**
     * Writes checkpoint entry buffer {@code entryBuf} to specified checkpoint file with 2-phase protocol.
     *
     * @param entryBuf Checkpoint entry buffer to write.
     * @param cp Checkpoint entry.
     * @param type Checkpoint entry type.
     * @throws StorageException If failed to write checkpoint entry.
     */
    public void writeCheckpointEntry(ByteBuffer entryBuf, CheckpointEntry cp,
        CheckpointEntryType type) throws StorageException {
        String fileName = checkpointFileName(cp, type);
        String tmpFileName = fileName + FilePageStoreManager.TMP_SUFFIX;

        try {
            try (FileIO io = ioFactory.create(Paths.get(cpDir.getAbsolutePath(), skipSync ? fileName : tmpFileName).toFile(),
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {

                io.writeFully(entryBuf);

                entryBuf.clear();

                if (!skipSync)
                    io.force(true);
            }

            if (!skipSync)
                Files.move(Paths.get(cpDir.getAbsolutePath(), tmpFileName), Paths.get(cpDir.getAbsolutePath(), fileName));
        }
        catch (IOException e) {
            throw new StorageException("Failed to write checkpoint entry [ptr=" + cp.checkpointMark()
                + ", cpTs=" + cp.timestamp()
                + ", cpId=" + cp.checkpointId()
                + ", type=" + type + "]", e);
        }
    }

    /**
     * Prepares checkpoint entry containing WAL pointer to checkpoint record. Writes into given {@code ptrBuf} WAL
     * pointer content.
     *
     * @param entryBuf Buffer to fill
     * @param cpTs Checkpoint timestamp.
     * @param cpId Checkpoint id.
     * @param ptr WAL pointer containing record.
     * @param rec Checkpoint WAL record.
     * @param type Checkpoint type.
     * @return Checkpoint entry.
     */
    private CheckpointEntry prepareCheckpointEntry(
        ByteBuffer entryBuf,
        long cpTs,
        UUID cpId,
        WALPointer ptr,
        @Nullable CheckpointRecord rec,
        CheckpointEntryType type
    ) {
        assert ptr instanceof FileWALPointer;

        FileWALPointer filePtr = (FileWALPointer)ptr;

        entryBuf.rewind();

        entryBuf.putLong(filePtr.index());

        entryBuf.putInt(filePtr.fileOffset());

        entryBuf.putInt(filePtr.length());

        entryBuf.flip();

        return createCheckPointEntry(cpTs, ptr, cpId, rec, type);
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @param ptr Wal pointer of checkpoint.
     * @param cpId Checkpoint ID.
     * @param rec Checkpoint record.
     * @param type Checkpoint type.
     * @return Checkpoint entry.
     */
    public CheckpointEntry createCheckPointEntry(
        long cpTs,
        WALPointer ptr,
        UUID cpId,
        @Nullable CheckpointRecord rec,
        CheckpointEntryType type
    ) {
        assert cpTs > 0;
        assert ptr != null;
        assert cpId != null;
        assert type != null;

        Map<Integer, CacheState> cacheGrpStates = null;

        // Do not hold groups state in-memory if there is no space in the checkpoint history to prevent possible OOM.
        // In this case the actual group states will be readed from WAL by demand.
        if (rec != null && cpHistory.hasSpace())
            cacheGrpStates = rec.cacheGroupStates();

        return new CheckpointEntry(cpTs, ptr, cpId, cacheGrpStates);
    }

    /**
     * @param allowToReplace The sign which allows to replace pages from a checkpoint by page replacer.
     * @return holder of FullPageIds obtained from each PageMemory, overall number of dirty pages, and flag defines at
     * least one user page became a dirty since last checkpoint.
     */
    private CheckpointPagesInfoHolder beginAllCheckpoints(IgniteInternalFuture<?> allowToReplace) {
        Collection<DataRegion> regions = dataRegions.get();

        Collection<Map.Entry<PageMemoryEx, GridMultiCollectionWrapper<FullPageId>>> res =
            new ArrayList<>(regions.size());

        int pagesNum = 0;

        for (DataRegion reg : regions) {
            if (!reg.config().isPersistenceEnabled())
                continue;

            GridMultiCollectionWrapper<FullPageId> nextCpPages = ((PageMemoryEx)reg.pageMemory()).beginCheckpoint(allowToReplace);

            pagesNum += nextCpPages.size();

            res.add(new T2<>((PageMemoryEx)reg.pageMemory(), nextCpPages));
        }

        CheckpointProgress progress = curCpProgress;

        if (progress != null)
            progress.currentCheckpointPagesCount(pagesNum);

        return new CheckpointPagesInfoHolder(res, pagesNum);
    }

    /**
     * @param chp Checkpoint.
     * @param tracker Tracker.
     */
    private void updateMetrics(Checkpoint chp, CheckpointMetricsTracker tracker) {
        if (persStoreMetrics.metricsEnabled()) {
            persStoreMetrics.onCheckpoint(
                tracker.lockWaitDuration(),
                tracker.markDuration(),
                tracker.pagesWriteDuration(),
                tracker.fsyncDuration(),
                tracker.totalDuration(),
                chp.pagesSize,
                tracker.dataPagesWritten(),
                tracker.cowPagesWritten(),
                forAllPageStores(PageStore::size),
                forAllPageStores(PageStore::getSparseSize));
        }
    }

    /**
     * @param f Consumer.
     * @return Accumulated result for all page stores.
     */
    public long forAllPageStores(ToLongFunction<PageStore> f) {
        long res = 0;

        for (CacheGroupContext gctx : cacheProcessor.cacheGroups())
            res += forGroupPageStores(gctx, f);

        return res;
    }

    /**
     * @param gctx Group context.
     * @param f Consumer.
     * @return Accumulated result for all page stores.
     */
    public long forGroupPageStores(CacheGroupContext gctx, ToLongFunction<PageStore> f) {
        int groupId = gctx.groupId();

        long res = 0;

        try {
            Collection<PageStore> stores = storeMgr.getStores(groupId);

            if (stores != null) {
                for (PageStore store : stores)
                    res += f.applyAsLong(store);
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return res;
    }

    /** */
    private String prepareWalSegsCoveredMsg(IgniteBiTuple<Long, Long> walRange) {
        String res;

        long startIdx = walRange.get1();
        long endIdx = walRange.get2();

        if (endIdx < 0 || endIdx < startIdx)
            res = "[]";
        else if (endIdx == startIdx)
            res = "[" + endIdx + "]";
        else
            res = "[" + startIdx + " - " + endIdx + "]";

        return res;
    }

    /**
     * Processes all evicted partitions scheduled for destroy.
     *
     * @return The number of destroyed partition files.
     * @throws IgniteCheckedException If failed.
     */
    private int destroyEvictedPartitions() throws IgniteCheckedException {
        PartitionDestroyQueue destroyQueue = curCpProgress.getDestroyQueue();

        if (destroyQueue.pendingReqs().isEmpty())
            return 0;

        List<PartitionDestroyRequest> reqs = null;

        for (final PartitionDestroyRequest req : destroyQueue.pendingReqs().values()) {
            if (!req.beginDestroy())
                continue;

            final int grpId = req.groupId();
            final int partId = req.partitionId();

            CacheGroupContext grp = cacheProcessor.cacheGroup(grpId);

            assert grp != null
                : "Cache group is not initialized [grpId=" + grpId + "]";
            assert grp.offheap() instanceof GridCacheOffheapManager
                : "Destroying partition files when persistence is off " + grp.offheap();

            final GridCacheOffheapManager offheap = (GridCacheOffheapManager)grp.offheap();

            Runnable destroyPartTask = () -> {
                try {
                    offheap.destroyPartitionStore(grpId, partId);

                    req.onDone(null);

                    grp.metrics().decrementInitializedLocalPartitions();

                    if (log.isDebugEnabled())
                        log.debug("Partition file has destroyed [grpId=" + grpId + ", partId=" + partId + "]");
                }
                catch (Exception e) {
                    req.onDone(new IgniteCheckedException(
                        "Partition file destroy has failed [grpId=" + grpId + ", partId=" + partId + "]", e));
                }
            };

            if (asyncRunner != null) {
                try {
                    asyncRunner.execute(destroyPartTask);
                }
                catch (RejectedExecutionException ignore) {
                    // Run the task synchronously.
                    destroyPartTask.run();
                }
            }
            else
                destroyPartTask.run();

            if (reqs == null)
                reqs = new ArrayList<>();

            reqs.add(req);
        }

        if (reqs != null)
            for (PartitionDestroyRequest req : reqs)
                req.waitCompleted();

        destroyQueue.pendingReqs().clear();

        return reqs != null ? reqs.size() : 0;
    }

    /**
     * @param grpCtx Group context. Can be {@code null} in case of crash recovery.
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public void schedulePartitionDestroy(@Nullable CacheGroupContext grpCtx, int grpId, int partId) {
        synchronized (this) {
            scheduledCp.getDestroyQueue().addDestroyRequest(grpCtx, grpId, partId);
        }

        if (log.isDebugEnabled())
            log.debug("Partition file has been scheduled to destroy [grpId=" + grpId + ", partId=" + partId + "]");

        if (grpCtx != null)
            wakeupForCheckpoint(PARTITION_DESTROY_CHECKPOINT_TIMEOUT, "partition destroy");
    }

    /**
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public void cancelOrWaitPartitionDestroy(int grpId, int partId) throws IgniteCheckedException {
        PartitionDestroyRequest req;

        synchronized (this) {
            req = scheduledCp.getDestroyQueue().cancelDestroy(grpId, partId);
        }

        if (req != null)
            req.waitCompleted();

        CheckpointProgressImpl cur;

        synchronized (this) {
            cur = curCpProgress;

            if (cur != null)
                req = cur.getDestroyQueue().cancelDestroy(grpId, partId);
        }

        if (req != null)
            req.waitCompleted();

        if (req != null && log.isDebugEnabled())
            log.debug("Partition file destroy has cancelled [grpId=" + grpId + ", partId=" + partId + "]");
    }

    /**
     *
     */
    private void waitCheckpointEvent() {
        boolean cancel = false;

        try {
            synchronized (this) {
                long remaining = U.nanosToMillis(scheduledCp.nextCpNanos() - System.nanoTime());

                while (remaining > 0 && !isCancelled()) {
                    blockingSectionBegin();

                    try {
                        wait(remaining);

                        remaining = U.nanosToMillis(scheduledCp.nextCpNanos() - System.nanoTime());
                    }
                    finally {
                        blockingSectionEnd();
                    }
                }
            }
        }
        catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();

            cancel = true;
        }

        if (cancel)
            isCancelled = true;
    }

    /**
     * @param tracker Checkpoint metrics tracker.
     * @return Duration of possible JVM pause, if it was detected, or {@code -1} otherwise.
     */
    private long possibleLongJvmPauseDuration(CheckpointMetricsTracker tracker) {
        if (LongJVMPauseDetector.enabled()) {
            if (tracker.lockWaitDuration() + tracker.lockHoldDuration() > longJvmPauseThreshold) {
                long now = System.currentTimeMillis();

                // We must get last wake up time before search possible pause in events map.
                long wakeUpTime = pauseDetector.getLastWakeUpTime();

                IgniteBiTuple<Long, Long> lastLongPause = pauseDetector.getLastLongPause();

                if (lastLongPause != null && tracker.checkpointStartTime() < lastLongPause.get1())
                    return lastLongPause.get2();

                if (now - wakeUpTime > longJvmPauseThreshold)
                    return now - wakeUpTime;
            }
        }

        return -1L;
    }

    /**
     * Take read lock for internal use.
     */
    private void internalReadUnlock() {
        checkpointLock.readLock().unlock();

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() - 1);
    }

    /**
     * Release read lock.
     */
    private void internalReadLock() {
        checkpointLock.readLock().lock();

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);
    }

    /**
     * Fill cache group state in checkpoint record.
     *
     * @param cpRec Checkpoint record for filling.
     * @throws IgniteCheckedException if fail.
     */
    private void fillCacheGroupState(CheckpointRecord cpRec) throws IgniteCheckedException {
        GridCompoundFuture grpHandleFut = asyncRunner == null ? null : new GridCompoundFuture();

        for (CacheGroupContext grp : cacheProcessor.cacheGroups()) {
            if (grp.isLocal() || !grp.walEnabled())
                continue;

            Runnable r = () -> {
                ArrayList<GridDhtLocalPartition> parts = new ArrayList<>(grp.topology().localPartitions().size());

                for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
                    parts.add(part);

                CacheState state = new CacheState(parts.size());

                for (GridDhtLocalPartition part : parts) {
                    GridDhtPartitionState partState = part.state();

                    if (partState == LOST)
                        partState = OWNING;

                    state.addPartitionState(
                        part.id(),
                        part.dataStore().fullSize(),
                        part.updateCounter(),
                        (byte)partState.ordinal()
                    );
                }

                synchronized (cpRec) {
                    cpRec.addCacheGroupState(grp.groupId(), state);
                }
            };

            if (asyncRunner == null)
                r.run();
            else
                try {
                    GridFutureAdapter<?> res = new GridFutureAdapter<>();

                    asyncRunner.execute(U.wrapIgniteFuture(r, res));

                    grpHandleFut.add(res);
                }
                catch (RejectedExecutionException e) {
                    assert false : "Task should never be rejected by async runner";

                    throw new IgniteException(e); //to protect from disabled asserts and call to failure handler
                }
        }

        if (grpHandleFut != null) {
            grpHandleFut.markInitialized();

            grpHandleFut.get();
        }
    }

    /**
     * @return Last checkpoint time.
     */
    private long updateLastCheckpointTime() {
        long cpTs = System.currentTimeMillis();

        // This can happen in an unlikely event of two checkpoints happening
        // within a currentTimeMillis() granularity window.
        if (cpTs == lastCpTs)
            cpTs++;

        lastCpTs = cpTs;

        return cpTs;
    }

    /**
     * Update current checkpoint progress by scheduled.
     *
     * @return Current checkpoint progress.
     */
    @NotNull private CheckpointProgress updateCurrentCheckpointProgress() {
        final CheckpointProgressImpl curr;

        synchronized (this) {
            curr = scheduledCp;

            curr.transitTo(LOCK_TAKEN);

            if (curr.reason() == null)
                curr.reason("timeout");

            // It is important that we assign a new progress object before checkpoint mark in page memory.
            scheduledCp = new CheckpointProgressImpl(checkpointFreq);

            curCpProgress = curr;
        }
        return curr;
    }

    /**
     * @param chp Checkpoint snapshot.
     */
    private void markCheckpointEnd(Checkpoint chp) throws IgniteCheckedException {
        synchronized (this) {
            chp.progress.clearCounters();

            for (DataRegion memPlc : dataRegions.get()) {
                if (!memPlc.config().isPersistenceEnabled())
                    continue;

                ((PageMemoryEx)memPlc.pageMemory()).finishCheckpoint();
            }
        }

        if (chp.hasDelta()) {
            CheckpointEntry cp = prepareCheckpointEntry(
                tmpWriteBuf,
                chp.cpEntry.timestamp(),
                chp.cpEntry.checkpointId(),
                chp.cpEntry.checkpointMark(),
                null,
                CheckpointEntryType.END);

            writeCheckpointEntry(tmpWriteBuf, cp, CheckpointEntryType.END);

            wal.notchLastCheckpointPtr(chp.cpEntry.checkpointMark());
        }

        List<CheckpointEntry> removedFromHistory = cpHistory.onCheckpointFinished(chp, truncateWalOnCpFinish);

        for (CheckpointEntry cp : removedFromHistory)
            removeCheckpointFiles(cp);

        if (chp.progress != null)
            chp.progress.transitTo(FINISHED);
    }

    /**
     * Removes checkpoint start/end files belongs to given {@code cpEntry}.
     *
     * @param cpEntry Checkpoint entry.
     * @throws IgniteCheckedException If failed to delete.
     */
    private void removeCheckpointFiles(CheckpointEntry cpEntry) throws IgniteCheckedException {
        Path startFile = new File(cpDir.getAbsolutePath(), checkpointFileName(cpEntry, CheckpointEntryType.START)).toPath();
        Path endFile = new File(cpDir.getAbsolutePath(), checkpointFileName(cpEntry, CheckpointEntryType.END)).toPath();

        try {
            if (Files.exists(startFile))
                Files.delete(startFile);

            if (Files.exists(endFile))
                Files.delete(endFile);
        }
        catch (IOException e) {
            throw new StorageException("Failed to delete stale checkpoint files: " + cpEntry, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        if (log.isDebugEnabled())
            log.debug("Cancelling grid runnable: " + this);

        // Do not interrupt runner thread.
        isCancelled = true;

        synchronized (this) {
            notifyAll();
        }
    }

    /**
     * For test use only.
     */
    public IgniteInternalFuture<Void> enableCheckpoints(boolean enable) {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        enableChangeApplied = fut;

        checkpointsEnabled = enable;

        return fut;
    }

    /**
     *
     */
    public void shutdownNow() {
        shutdownNow = true;

        if (!isCancelled)
            cancel();
    }

    /**
     * @param memoryRecoveryRecordPtr Memory recovery record pointer.
     */
    public void memoryRecoveryRecordPtr(WALPointer memoryRecoveryRecordPtr) {
        this.memoryRecoveryRecordPtr = memoryRecoveryRecordPtr;
    }

    /**
     * @param cancel Cancel flag.
     */
    @SuppressWarnings("unused")
    public void shutdownCheckpointer(boolean cancel) {
        if (cancel)
            shutdownNow();
        else
            cancel();

        try {
            U.join(this);
        }
        catch (IgniteInterruptedCheckedException ignore) {
            U.warn(log, "Was interrupted while waiting for checkpointer shutdown, " +
                "will not wait for checkpoint to finish.");

            shutdownNow();

            while (true) {
                try {
                    U.join(this);

                    scheduledCp.fail(new NodeStoppingException("Checkpointer is stopped during node stop."));

                    break;
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    //Ignore
                }
            }

            Thread.currentThread().interrupt();
        }

        if (asyncRunner != null) {
            asyncRunner.shutdownNow();

            try {
                asyncRunner.awaitTermination(2, TimeUnit.MINUTES);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }

        lsnrs.clear();
    }

    /**
     * @param lsnr Listener.
     */
    public void addCheckpointListener(DbCheckpointListener lsnr) {
        lsnrs.add(lsnr);
    }

    /**
     * @param lsnr Listener.
     */
    public void removeCheckpointListener(DbCheckpointListener lsnr) {
        lsnrs.remove(lsnr);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void finalizeCheckpointOnRecovery(
        long cpTs,
        UUID cpId,
        WALPointer walPtr,
        StripedExecutor exec
    ) throws IgniteCheckedException {
        assert cpTs != 0;

        long start = System.currentTimeMillis();

        Collection<DataRegion> regions = dataRegions.get();

        CheckpointPagesInfoHolder cpPagesHolder = beginAllCheckpoints(new GridFinishedFuture<>());

        // Sort and split all dirty pages set to several stripes.
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> pages =
            splitAndSortCpPagesIfNeeded(cpPagesHolder);

        // Identity stores set for future fsync.
        Collection<PageStore> updStores = new GridConcurrentHashSet<>();

        AtomicInteger cpPagesCnt = new AtomicInteger();

        // Shared refernce for tracking exception during write pages.
        AtomicReference<Throwable> writePagesError = new AtomicReference<>();

        for (int stripeIdx = 0; stripeIdx < exec.stripesCount(); stripeIdx++) {
            exec.execute(stripeIdx, () -> {
                PageStoreWriter pageStoreWriter = (fullPageId, buf, tag) -> {
                    assert tag != PageMemoryImpl.TRY_AGAIN_TAG : "Lock is held by other thread for page " + fullPageId;

                    int groupId = fullPageId.groupId();
                    long pageId = fullPageId.pageId();

                    // Write buf to page store.
                    PageStore store = storeMgr.writeInternal(groupId, pageId, buf, tag, true);

                    // Save store for future fsync.
                    updStores.add(store);
                };

                // Local buffer for write pages.
                ByteBuffer writePageBuf = ByteBuffer.allocateDirect(pageSize);

                writePageBuf.order(ByteOrder.nativeOrder());

                GridConcurrentMultiPairQueue.Result<PageMemoryEx, FullPageId> res =
                    new GridConcurrentMultiPairQueue.Result<>();

                int pagesWritten = 0;

                try {
                    while (pages.next(res)) {
                        // Fail-fast break if some exception occurred.
                        if (writePagesError.get() != null)
                            break;

                        PageMemoryEx pageMem = res.getKey();

                        // Write page content to page store via pageStoreWriter.
                        // Tracker is null, because no need to track checkpoint metrics on recovery.
                        pageMem.checkpointWritePage(res.getValue(), writePageBuf, pageStoreWriter, null);

                        // Add number of handled pages.
                        pagesWritten++;
                    }
                }
                catch (Throwable e) {
                    U.error(log, "Failed to write page to pageStore: " + res);

                    writePagesError.compareAndSet(null, e);

                    if (e instanceof Error)
                        throw (Error)e;
                }

                cpPagesCnt.addAndGet(pagesWritten);
            });
        }

        // Await completion all write tasks.
        awaitApplyComplete(exec, writePagesError);

        long written = U.currentTimeMillis();

        // Fsync all touched stores.
        for (PageStore updStore : updStores)
            updStore.sync();

        long fsync = U.currentTimeMillis();

        for (DataRegion memPlc : regions) {
            if (memPlc.config().isPersistenceEnabled())
                ((PageMemoryEx)memPlc.pageMemory()).finishCheckpoint();
        }

        ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(pageSize);

        tmpWriteBuf.order(ByteOrder.nativeOrder());

        CheckpointEntry cp = prepareCheckpointEntry(
            tmpWriteBuf,
            cpTs,
            cpId,
            walPtr,
            null,
            CheckpointEntryType.END);

        writeCheckpointEntry(tmpWriteBuf, cp, CheckpointEntryType.END);

        if (log.isInfoEnabled())
            log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                    "pagesWrite=%dms, fsync=%dms, total=%dms]",
                cpId,
                cpPagesCnt.get(),
                walPtr,
                written - start,
                fsync - written,
                fsync - start));
    }

    /**
     * @param exec Striped executor.
     * @param applyError Check error reference.
     */
    private void awaitApplyComplete(
        StripedExecutor exec,
        AtomicReference<Throwable> applyError
    ) throws IgniteCheckedException {
        try {
            // Await completion apply tasks in all stripes.
            exec.awaitComplete();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }

        // Checking error after all task applied.
        Throwable error = applyError.get();

        if (error != null)
            throw error instanceof IgniteCheckedException
                ? (IgniteCheckedException)error : new IgniteCheckedException(error);
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @param cpId Checkpoint ID.
     * @param type Checkpoint type.
     * @return Checkpoint file name.
     */
    private static String checkpointFileName(long cpTs, UUID cpId, CheckpointEntryType type) {
        return cpTs + "-" + cpId + "-" + type + ".bin";
    }

    /**
     * @param cp Checkpoint entry.
     * @param type Checkpoint type.
     * @return Checkpoint file name.
     */
    public static String checkpointFileName(CheckpointEntry cp, CheckpointEntryType type) {
        return checkpointFileName(cp.timestamp(), cp.checkpointId(), type);
    }

    /**
     * @param threadBuf Thread local byte buffer.
     */
    public void threadBuf(ThreadLocal<ByteBuffer> threadBuf) {
        this.threadBuf = threadBuf;
    }

    /**
     * @return Progress of current chekpoint or {@code null}, if isn't checkpoint at this moment.
     */
    public CheckpointProgress currentProgress() {
        return curCpProgress;
    }

    /**
     * @return {@code True} if checkpoint should be stopped immediately.
     */
    private boolean isShutdownNow() {
        return shutdownNow;
    }
}
