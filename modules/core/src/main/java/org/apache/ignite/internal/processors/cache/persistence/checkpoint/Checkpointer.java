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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.LongJVMPauseDetector;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperation;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor;
import org.apache.ignite.internal.util.GridConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.WorkProgressDispatcher;
import org.apache.ignite.internal.worker.WorkersRegistry;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.LongJVMPauseDetector.DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP;
import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointReadWriteLock.CHECKPOINT_RUNNER_THREAD_PREFIX;

/**
 * Checkpointer object is used for notification on checkpoint begin, predicate is {@link #scheduledCp}<code>.nextCpTs -
 * now > 0 </code>. Method {@link #scheduleCheckpoint} uses notify, {@link #waitCheckpointEvent} uses wait.
 *
 * Checkpointer is one threaded which means that only one checkpoint at the one moment possible.
 *
 * Responsiblity: Provide the API for schedule/trigger the checkpoint. Schedule new checkpoint after current one
 * according to checkpoint frequency. Failure handling. Managing of page write threads - ? Logging and metrics of
 * checkpoint. *
 *
 * Checkpointer steps:
 * <p> Awaiting checkpoint event. </p>
 * <p> Collect all dirty pages from page memory under write lock. </p>
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
        "%s" +
        "pages=%d, " +
        "reason='%s']";

    /** Skip sync. */
    private final boolean skipSync = getBoolean(IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);

    /** Timeout between partition file destroy and checkpoint to handle it. */
    private static final long PARTITION_DESTROY_CHECKPOINT_TIMEOUT = 30 * 1000; // 30 Seconds.

    /** Avoid the start checkpoint if checkpointer was canceled. */
    private volatile boolean skipCheckpointOnNodeStop = getBoolean(IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, false);

    /** Long JVM pause threshold. */
    private final int longJvmPauseThreshold =
        getInteger(IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD, DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD);

    /** Pause detector. */
    private final LongJVMPauseDetector pauseDetector;

    /** The interval in ms after which the checkpoint is triggered if there are no other events. */
    private final long checkpointFreq;

    /** Failure processor. */
    private final FailureProcessor failureProcessor;

    /** Snapshot manager. */
    private final IgniteCacheSnapshotManager snapshotMgr;

    /** Metrics. */
    private final DataStorageMetricsImpl persStoreMetrics;

    /**
     * Cache processor.
     *
     * @deprecated Checkpointer should not know about the cache at all.
     */
    private final GridCacheProcessor cacheProcessor;

    /** Strategy of where and how to get the pages. */
    private final CheckpointWorkflow checkpointWorkflow;

    /** Factory for the creation of page-write workers. */
    private final CheckpointPagesWriterFactory checkpointPagesWriterFactory;

    /** The number of IO-bound threads which will write pages to disk. */
    private final int checkpointWritePageThreads;

    /** Checkpoint frequency deviation. */
    private final Supplier<Integer> cpFreqDeviation;

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private volatile IgniteThreadPoolExecutor checkpointWritePagesPool;

    /** Next scheduled checkpoint progress. */
    private volatile CheckpointProgressImpl scheduledCp;

    /** Current checkpoint. This field is updated only by checkpoint thread. */
    private volatile CheckpointProgressImpl curCpProgress;

    /** Shutdown now. */
    private volatile boolean shutdownNow;

    /** Last checkpoint timestamp. */
    private long lastCpTs;

    /** Performance statistics processor. */
    private final PerformanceStatisticsProcessor psproc;

    /** For testing only. */
    private GridFutureAdapter<Void> enableChangeApplied;

    /** For testing only. */
    private volatile boolean checkpointsEnabled = true;

    /**
     * @param gridName Grid name.
     * @param name Thread name.
     * @param workersRegistry Worker registry.
     * @param logger Logger.
     * @param detector Long JVM pause detector.
     * @param failureProcessor Failure processor.
     * @param snapshotManager Snapshot manager.
     * @param dsMetrics Data storage metrics.
     * @param cacheProcessor Cache processor.
     * @param checkpoint Implementation of checkpoint.
     * @param factory Page writer factory.
     * @param checkpointFrequency Checkpoint frequency.
     * @param checkpointWritePageThreads The number of IO-bound threads which will write pages to disk.
     * @param cpFreqDeviation Deviation of checkpoint frequency.
     */
    Checkpointer(
        @Nullable String gridName,
        String name,
        WorkersRegistry workersRegistry,
        Function<Class<?>, IgniteLogger> logger,
        LongJVMPauseDetector detector,
        FailureProcessor failureProcessor,
        IgniteCacheSnapshotManager snapshotManager,
        DataStorageMetricsImpl dsMetrics,
        GridCacheProcessor cacheProcessor,
        CheckpointWorkflow checkpoint,
        CheckpointPagesWriterFactory factory,
        long checkpointFrequency,
        int checkpointWritePageThreads,
        Supplier<Integer> cpFreqDeviation
    ) {
        super(gridName, name, logger.apply(Checkpointer.class), workersRegistry);
        this.pauseDetector = detector;
        this.checkpointFreq = checkpointFrequency;
        this.failureProcessor = failureProcessor;
        this.snapshotMgr = snapshotManager;
        this.checkpointWorkflow = checkpoint;
        this.checkpointPagesWriterFactory = factory;
        this.persStoreMetrics = dsMetrics;
        this.cacheProcessor = cacheProcessor;
        this.checkpointWritePageThreads = Math.max(checkpointWritePageThreads, 1);
        this.checkpointWritePagesPool = initializeCheckpointPool();
        this.cpFreqDeviation = cpFreqDeviation;
        this.psproc = cacheProcessor.context().kernalContext().performanceStatistics();

        scheduledCp = new CheckpointProgressImpl(nextCheckpointInterval());
    }

    /**
     * @return Initialized checkpoint page write pool;
     */
    private IgniteThreadPoolExecutor initializeCheckpointPool() {
        if (checkpointWritePageThreads > 1)
            return new IgniteThreadPoolExecutor(
                CHECKPOINT_RUNNER_THREAD_PREFIX + "-IO",
                igniteInstanceName(),
                checkpointWritePageThreads,
                checkpointWritePageThreads,
                30_000,
                new LinkedBlockingQueue<>()
            );

        return null;
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
                        scheduledCp.nextCpNanos(System.nanoTime() + U.millisToNanos(nextCheckpointInterval()));
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
    public CheckpointProgress scheduleCheckpoint(long delayFromNow, String reason) {
        return scheduleCheckpoint(delayFromNow, reason, null);
    }

    /**
     * Gets a checkpoint interval with a randomized delay.
     * It helps when the cluster makes a checkpoint in the same time in every node.
     *
     * @return Next checkpoint interval.
     */
    private long nextCheckpointInterval() {
        Integer deviation = cpFreqDeviation.get();

        if (deviation == null || deviation == 0)
            return checkpointFreq;

        long startDelay = ThreadLocalRandom.current().nextLong(U.ensurePositive(U.safeAbs(checkpointFreq * deviation) / 100, 1))
            - U.ensurePositive(U.safeAbs(checkpointFreq * deviation) / 200, 1);

        return U.safeAbs(checkpointFreq + startDelay);
    }

    /**
     * Change the information for a scheduled checkpoint if it was scheduled further than {@code delayFromNow}, or do
     * nothing otherwise.
     *
     * If lsnr is not null, new checkpoint is always triggered.
     *
     * @return Nearest scheduled checkpoint which is not started yet(Dirty pages weren't collected yet).
     */
    public <R> CheckpointProgress scheduleCheckpoint(
        long delayFromNow,
        String reason,
        IgniteInClosure<? super IgniteInternalFuture<R>> lsnr
    ) {
        CheckpointProgressImpl sched = curCpProgress;

        //If checkpoint haven't taken the write lock yet it shouldn't trigger a new checkpoint but should return current one.
        if (lsnr == null && sched != null && !sched.greaterOrEqualTo(CheckpointState.LOCK_TAKEN))
            return sched;

        if (lsnr != null) {
            //To be sure lsnr always will be executed in checkpoint thread.
            synchronized (this) {
                sched = scheduledCp;

                sched.futureFor(FINISHED).listen(lsnr);
            }
        }

        sched = scheduledCp;

        long nextNanos = System.nanoTime() + U.millisToNanos(delayFromNow);

        if (sched.nextCpNanos() - nextNanos <= 0)
            return sched;

        synchronized (this) {
            sched = scheduledCp;

            if (sched.nextCpNanos() - nextNanos > 0) {
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

            startCheckpointProgress();

            try {
                chp = checkpointWorkflow.markCheckpointBegin(lastCpTs, curCpProgress, tracker, this);
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

            if (chp.hasDelta()) {
                if (log.isInfoEnabled()) {
                    long possibleJvmPauseDur = possibleLongJvmPauseDuration(tracker);

                    if (log.isInfoEnabled())
                        log.info(
                            String.format(
                                CHECKPOINT_STARTED_LOG_FORMAT,
                                chp.cpEntry == null ? "" : chp.cpEntry.checkpointId(),
                                chp.cpEntry == null ? "" : chp.cpEntry.checkpointMark(),
                                tracker.beforeLockDuration(),
                                tracker.lockWaitDuration(),
                                tracker.listenersExecuteDuration(),
                                tracker.lockHoldDuration(),
                                tracker.walCpRecordFsyncDuration(),
                                tracker.writeCheckpointEntryDuration(),
                                tracker.splitAndSortCpPagesDuration(),
                                possibleJvmPauseDur > 0 ? "possibleJvmPauseDuration=" + possibleJvmPauseDur + "ms, " : "",
                                chp.pagesSize,
                                chp.progress.reason()
                            )
                        );
                }

                if (!writePages(tracker, chp.cpPages, chp.progress, this, this::isShutdownNow))
                    return;
            }
            else {
                if (log.isInfoEnabled())
                    LT.info(log, String.format(
                        "Skipping checkpoint (no pages were modified) [" +
                            "checkpointBeforeLockTime=%dms, checkpointLockWait=%dms, " +
                            "checkpointListenersExecuteTime=%dms, checkpointLockHoldTime=%dms, reason='%s']",
                        tracker.beforeLockDuration(),
                        tracker.lockWaitDuration(),
                        tracker.listenersExecuteDuration(),
                        tracker.lockHoldDuration(),
                        chp.progress.reason())
                    );

                tracker.onPagesWriteStart();
                tracker.onFsyncStart();
            }

            snapshotMgr.afterCheckpointPageWritten();

            int destroyedPartitionsCnt = destroyEvictedPartitions();

            // Must mark successful checkpoint only if there are no exceptions or interrupts.
            checkpointWorkflow.markCheckpointEnd(chp);

            tracker.onEnd();

            if (chp.hasDelta() || destroyedPartitionsCnt > 0) {
                if (log.isInfoEnabled()) {
                    log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                            "walSegmentsCovered=%s, markDuration=%dms, pagesWrite=%dms, fsync=%dms, total=%dms]",
                        chp.cpEntry != null ? chp.cpEntry.checkpointId() : "",
                        chp.pagesSize,
                        chp.cpEntry != null ? chp.cpEntry.checkpointMark() : "",
                        walRangeStr(chp.walSegsCoveredRange),
                        tracker.markDuration(),
                        tracker.pagesWriteDuration(),
                        tracker.fsyncDuration(),
                        tracker.totalDuration()));
                }
            }

            updateMetrics(chp, tracker);
        }
        catch (IgniteCheckedException e) {
            chp.progress.fail(e);

            failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, e));
        }
    }

    /**
     * @param workProgressDispatcher Work progress dispatcher.
     * @param tracker Checkpoint metrics tracker.
     * @param cpPages List of pages to write.
     * @param curCpProgress Current checkpoint data.
     * @param shutdownNow Checker of stop operation.
     */
    boolean writePages(
        CheckpointMetricsTracker tracker,
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> cpPages,
        CheckpointProgressImpl curCpProgress,
        WorkProgressDispatcher workProgressDispatcher,
        BooleanSupplier shutdownNow
    ) throws IgniteCheckedException {
        IgniteThreadPoolExecutor pageWritePool = checkpointWritePagesPool;

        int checkpointWritePageThreads = pageWritePool == null ? 1 : pageWritePool.getMaximumPoolSize();

        // Identity stores set.
        ConcurrentLinkedHashMap<PageStore, LongAdder> updStores = new ConcurrentLinkedHashMap<>();

        CountDownFuture doneWriteFut = new CountDownFuture(checkpointWritePageThreads);

        tracker.onPagesWriteStart();

        for (int i = 0; i < checkpointWritePageThreads; i++) {
            Runnable write = checkpointPagesWriterFactory.build(
                tracker,
                cpPages,
                updStores,
                doneWriteFut,
                workProgressDispatcher::updateHeartbeat,
                curCpProgress,
                shutdownNow
            );

            if (pageWritePool == null)
                write.run();
            else {
                try {
                    pageWritePool.execute(write);
                }
                catch (RejectedExecutionException ignore) {
                    // Run the task synchronously.
                    write.run();
                }
            }
        }

        workProgressDispatcher.updateHeartbeat();

        // Wait and check for errors.
        doneWriteFut.get();

        // Must re-check shutdown flag here because threads may have skipped some pages.
        // If so, we should not put finish checkpoint mark.
        if (shutdownNow.getAsBoolean()) {
            curCpProgress.fail(new NodeStoppingException("Node is stopping."));

            return false;
        }

        tracker.onFsyncStart();

        if (!skipSync) {
            for (Map.Entry<PageStore, LongAdder> updStoreEntry : updStores.entrySet()) {
                if (shutdownNow.getAsBoolean()) {
                    curCpProgress.fail(new NodeStoppingException("Node is stopping."));

                    return false;
                }

                workProgressDispatcher.blockingSectionBegin();

                try {
                    updStoreEntry.getKey().sync();
                }
                finally {
                    workProgressDispatcher.blockingSectionEnd();
                }

                curCpProgress.updateSyncedPages(updStoreEntry.getValue().intValue());
            }
        }

        return true;
    }

    /**
     * @param chp Checkpoint.
     * @param tracker Tracker.
     */
    private void updateMetrics(Checkpoint chp, CheckpointMetricsTracker tracker) {
        if (psproc.enabled()) {
            psproc.checkpoint(
                tracker.beforeLockDuration(),
                tracker.lockWaitDuration(),
                tracker.listenersExecuteDuration(),
                tracker.markDuration(),
                tracker.lockHoldDuration(),
                tracker.pagesWriteDuration(),
                tracker.fsyncDuration(),
                tracker.walCpRecordFsyncDuration(),
                tracker.writeCheckpointEntryDuration(),
                tracker.splitAndSortCpPagesDuration(),
                tracker.totalDuration(),
                tracker.checkpointStartTime(),
                chp.pagesSize,
                tracker.dataPagesWritten(),
                tracker.cowPagesWritten());
        }

        if (persStoreMetrics.metricsEnabled()) {
            persStoreMetrics.onCheckpoint(
                tracker.beforeLockDuration(),
                tracker.lockWaitDuration(),
                tracker.listenersExecuteDuration(),
                tracker.markDuration(),
                tracker.lockHoldDuration(),
                tracker.pagesWriteDuration(),
                tracker.fsyncDuration(),
                tracker.walCpRecordFsyncDuration(),
                tracker.writeCheckpointEntryDuration(),
                tracker.splitAndSortCpPagesDuration(),
                tracker.totalDuration(),
                tracker.checkpointStartTime(),
                chp.pagesSize,
                tracker.dataPagesWritten(),
                tracker.cowPagesWritten()
            );
        }
    }

    /**
     * Creates a string of a range WAL segments.
     *
     * @param walRange Range of WAL segments.
     * @return The message about how many WAL segments was between previous checkpoint and current one.
     */
    private String walRangeStr(@Nullable IgniteBiTuple<Long, Long> walRange) {
        if (walRange == null)
            return "";

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

            IgniteThreadPoolExecutor pool = checkpointWritePagesPool;

            if (pool != null) {
                try {
                    pool.execute(destroyPartTask);
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
            scheduleCheckpoint(PARTITION_DESTROY_CHECKPOINT_TIMEOUT, "partition destroy");
    }

    /**
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return {@code True} if the request to destroy the partition was canceled.
     */
    public boolean cancelOrWaitPartitionDestroy(int grpId, int partId) throws IgniteCheckedException {
        PartitionDestroyRequest req;
        boolean canceled = false;

        synchronized (this) {
            req = scheduledCp.getDestroyQueue().removeRequest(grpId, partId);

            if (req != null) {
                canceled = req.cancel();

                assert canceled;
            }

            CheckpointProgressImpl cur = curCpProgress;

            if (cur != null) {
                req = cur.getDestroyQueue().removeRequest(grpId, partId);

                if (req != null)
                    canceled = req.cancel();
            }
        }

        if (!canceled) {
            if (req != null)
                req.waitCompleted();

            return false;
        }

        if (log.isDebugEnabled())
            log.debug("Partition file destroy has cancelled [grpId=" + grpId + ", partId=" + partId + "]");

        return true;
    }

    /**
     * Waiting until the next checkpoint time.
     */
    private void waitCheckpointEvent() {
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

            isCancelled = true;
        }
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
     * Update the current checkpoint info from the scheduled one.
     */
    private void startCheckpointProgress() {
        long cpTs = System.currentTimeMillis();

        // This can happen in an unlikely event of two checkpoints happening
        // within a currentTimeMillis() granularity window.
        if (cpTs == lastCpTs)
            cpTs++;

        lastCpTs = cpTs;

        synchronized (this) {
            CheckpointProgressImpl curr = scheduledCp;

            if (curr.reason() == null)
                curr.reason("timeout");

            // It is important that we assign a new progress object before checkpoint mark in page memory.
            scheduledCp = new CheckpointProgressImpl(nextCheckpointInterval());

            curCpProgress = curr;
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
     *
     * @deprecated Should be rewritten to public API.
     */
    public IgniteInternalFuture<Void> enableCheckpoints(boolean enable) {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        enableChangeApplied = fut;

        checkpointsEnabled = enable;

        return fut;
    }

    /**
     * Stopping all checkpoint activity immediately even if the current checkpoint is in progress.
     */
    public void shutdownNow() {
        shutdownNow = true;

        if (!isCancelled)
            cancel();
    }

    /**
     * Restart worker in IgniteThread.
     */
    public void start() {
        if (runner() != null)
            return;

        assert runner() == null : "Checkpointer is running.";

        new IgniteThread(this).start();
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

        IgniteThreadPoolExecutor pool = checkpointWritePagesPool;

        if (pool != null) {
            pool.shutdownNow();

            try {
                pool.awaitTermination(2, TimeUnit.MINUTES);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }

            checkpointWritePagesPool = null;
        }
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
        checkpointWorkflow.finalizeCheckpointOnRecovery(cpTs, cpId, walPtr, exec, checkpointPagesWriterFactory);
    }

    /**
     * @return Progress of current chekpoint, last finished one or {@code null}, if checkpoint has never started.
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

    /**
     * Skip checkpoint on node stop.
     *
     * @param skip If {@code true} skips checkpoint on node stop.
     */
    public void skipCheckpointOnNodeStop(boolean skip) {
        skipCheckpointOnNodeStop = skip;
    }
}
