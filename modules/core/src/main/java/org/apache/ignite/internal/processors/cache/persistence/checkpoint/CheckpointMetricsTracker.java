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

import java.util.EnumMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.LongJVMPauseDetector;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteCommonsSystemProperties.getInteger;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.internal.LongJVMPauseDetector.DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD;

/**
 * Tracks various checkpoint phases and stats.
 *
 * Assumed sequence of events:
 * <ol>
 *     <li>Checkpoint start</li>
 *     <li>CP Lock wait start</li>
 *     <li>CP mark start</li>
 *     <li>CP Lock release</li>
 *     <li>Pages write start</li>
 *     <li>fsync start</li>
 *     <li>Checkpoint end</li>
 * </ol>
 */
public class CheckpointMetricsTracker {
    /** Checkpoint started log message format. */
    private static final String CHECKPOINT_STARTED_LOG_FORMAT = "Checkpoint started [" +
            "checkpointId=%s, " +
            "startPtr=%s, " +
            "checkpointBeforeLockTime=%dms, " +
            "checkpointLockWait=%dms, " +
            "checkpointListenersExecuteTime=%dms, " +
            "checkpointLockHoldTime=%dms, " +
            "walCpRecordFsyncDuration=%dms, " +
            "splitAndSortCpPagesDuration=%dms, " +
            "writeRecoveryDataDuration=%dms, " +
            "writeCheckpointEntryDuration=%dms, " +
            "%s" +
            "pages=%d, " +
            "reason='%s']";

    /** Executor service to provide calculations and metrics export */
    public static final ExecutorService CHECK_POINT_METRICS_EXPORT_EXECUTOR =
            Executors.newSingleThreadExecutor((runnable) -> {
                Thread thread = new Thread(runnable);
                try {
                    thread.setDaemon(true);
                    thread.setPriority(Thread.NORM_PRIORITY - 1);
                    thread.setName("check-point-metrics-export-thread");
                }
                catch (SecurityException ignored) {
                    // do nothing
                }
                return thread;
            });

    static {
        Runtime runtime = Runtime.getRuntime();
        Thread shutdownExecutor = new Thread(() -> {
            CHECK_POINT_METRICS_EXPORT_EXECUTOR.shutdown();
            try {
                CHECK_POINT_METRICS_EXPORT_EXECUTOR.awaitTermination(60, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        runtime.addShutdownHook(shutdownExecutor);
    }

    /** */
    private static final AtomicIntegerFieldUpdater<CheckpointMetricsTracker> DATA_PAGES_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(CheckpointMetricsTracker.class, "dataPages");

    /** */
    private static final AtomicIntegerFieldUpdater<CheckpointMetricsTracker> COW_PAGES_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(CheckpointMetricsTracker.class, "cowPages");

    /** Long JVM pause threshold. */
    private final int longJvmPauseThreshold =
            getInteger(IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD, DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD);

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Previously calculated duration storage. */
    private final EnumMap<Duration, Long> durationsStorage = new EnumMap<>(Duration.class);

    /** Pause detector. */
    private final LongJVMPauseDetector pauseDetector;

    /** */
    private volatile int dataPages;

    /** */
    private volatile int cowPages;

    /** */
    private final long cpStart = System.nanoTime();

    /** Epoch time of checkpoint process start */
    private final long cpStartEpochTime = System.currentTimeMillis();

    /** */
    private long cpLockWaitStart;

    /** */
    private long cpMarkStart;

    /** */
    private long cpMarkEnd;

    /** */
    private long cpLockRelease;

    /** */
    private long cpPagesWriteStart;

    /** */
    private long cpFsyncStart;

    /** */
    private long cpEnd;

    /** */
    private long walCpRecordFsyncStart;

    /** */
    private long walCpRecordFsyncEnd;

    /** */
    private long cpMarkerStoreEnd;

    /** */
    private long splitAndSortCpPagesEnd;

    /** */
    private long cpRecoveryDataWriteEnd;

    /** */
    private long cpRecoveryDataSize;

    /** */
    private long listenersExecEnd;

    /**
     * @param log - current ignite logger instance
     */
    public CheckpointMetricsTracker(IgniteLogger log, LongJVMPauseDetector pauseDetector) {
        this.log = log;
        this.pauseDetector = pauseDetector;
    }

    /**
     * Increments counter if copy on write page was written.
     */
    public void onCowPageWritten() {
        COW_PAGES_UPDATER.incrementAndGet(this);
    }

    /** */
    public void onDataPageWritten() {
        DATA_PAGES_UPDATER.incrementAndGet(this);
    }

    /**
     * @return COW pages.
     */
    public int cowPagesWritten() {
        return cowPages;
    }

    /**
     * @return Data pages written.
     */
    public int dataPagesWritten() {
        return dataPages;
    }

    /** */
    public void onLockWaitStart() {
        cpLockWaitStart = System.nanoTime();
    }

    /** */
    public void onMarkStart() {
        cpMarkStart = System.nanoTime();
    }

    /** */
    public void onMarkEnd() {
        cpMarkEnd = System.nanoTime();
    }

    /** */
    public void onLockRelease() {
        cpLockRelease = System.nanoTime();
    }

    /** */
    public void onPagesWriteStart() {
        cpPagesWriteStart = System.nanoTime();
    }

    /** */
    public void onFsyncStart() {
        cpFsyncStart = System.nanoTime();
    }

    /** */
    public void onEnd() {
        cpEnd = System.nanoTime();
    }

    /** */
    public void onListenersExecuteEnd() {
        listenersExecEnd = System.nanoTime();
    }

    /** */
    public void onWalCpRecordFsyncStart() {
        walCpRecordFsyncStart = System.nanoTime();
    }

    /** */
    public void onCpMarkerStoreEnd() {
        cpMarkerStoreEnd = System.nanoTime();
    }

    /** */
    public void onSplitAndSortCpPagesEnd() {
        splitAndSortCpPagesEnd = System.nanoTime();
    }

    /** */
    public void onWalCpRecordFsyncEnd() {
        walCpRecordFsyncEnd = System.nanoTime();
    }

    /** */
    public void onWriteRecoveryDataEnd(long recoveryDataSize) {
        cpRecoveryDataSize = recoveryDataSize;
        cpRecoveryDataWriteEnd = System.nanoTime();
    }

    /**
     * Tries to log message with checkpoint recovery data write start details
     * @param chp Checkpoint (This metrics owner)
     */
    void logCheckPointRecoveryDataWriteStart(Checkpoint chp) {
        int pagesSize = chp.pagesSize;
        Object checkpointId = chp.cpEntry == null ? "" : chp.cpEntry.checkpointId();
        Object checkpointMark = chp.cpEntry == null ? "" : chp.cpEntry.checkpointMark();
        String recoveryDataWriteStartReason = chp.progress.reason();
        invokeLater(() -> {
            if (log.isInfoEnabled()) {
                log.info(String.format("Checkpoint recovery data write started [" +
                                "checkpointId=%s, " +
                                "startPtr=%s, " +
                                "pages=%d, " +
                                "checkpointBeforeLockTime=%dms, " +
                                "checkpointLockWait=%dms, " +
                                "checkpointListenersExecuteTime=%dms, " +
                                "checkpointLockHoldTime=%dms, " +
                                "walCpRecordFsyncDuration=%dms, " +
                                "splitAndSortCpPagesDuration=%dms, " +
                                "reason='%s']",
                        checkpointId,
                        checkpointMark,
                        pagesSize,
                        getDurationMillis(Duration.BEFORE_LOCK),
                        getDurationMillis(Duration.LOCK_WAIT),
                        getDurationMillis(Duration.LISTENERS_EXECUTE),
                        getDurationMillis(Duration.LOCK_HOLD),
                        getDurationMillis(Duration.WAL_CP_RECORD_FSYNC),
                        getDurationMillis(Duration.SPLIT_AND_SORT_CP_PAGES),
                        recoveryDataWriteStartReason
                ));
            }
        });
    }

    /**
     * Tries to log message with checkpoint start details
     * @param chp Checkpoint (This metrics owner)
     */
    void logCheckPointStart(Checkpoint chp) {
        int pagesSize = chp.pagesSize;
        String checkPointStartReason = chp.progress.reason();
        Object checkpointId = chp.cpEntry == null ? "" : chp.cpEntry.checkpointId();
        Object checkpointMark = chp.cpEntry == null ? "" : chp.cpEntry.checkpointMark();
        invokeLater(() -> {
            if (log.isInfoEnabled()) {
                log.info(
                        String.format(
                                CHECKPOINT_STARTED_LOG_FORMAT,
                                checkpointId,
                                checkpointMark,
                                getDurationMillis(Duration.BEFORE_LOCK),
                                getDurationMillis(Duration.LOCK_WAIT),
                                getDurationMillis(Duration.LISTENERS_EXECUTE),
                                getDurationMillis(Duration.LOCK_HOLD),
                                getDurationMillis(Duration.WAL_CP_RECORD_FSYNC),
                                getDurationMillis(Duration.SPLIT_AND_SORT_CP_PAGES),
                                getDurationMillis(Duration.RECOVERY_DATA_WRITE),
                                getDurationMillis(Duration.WRITE_CHECKPOINT_ENTRY),
                                possibleLongJvmPauseExplaination(),
                                pagesSize,
                                checkPointStartReason));
            }
        });
    }

    /**
     * Tries to log message with checkpoint finish details
     * @param chp Checkpoint (This metrics owner)
     */
    void logCheckpointFinish(Checkpoint chp) {
        int pagesSize = chp.pagesSize;
        Object checkpointId = chp.cpEntry != null ? chp.cpEntry.checkpointId() : "";
        Object checkpointMark = chp.cpEntry != null ? chp.cpEntry.checkpointMark() : "";
        IgniteBiTuple<Long, Long> walSegsCoveredRange = chp.walSegsCoveredRange;
        invokeLater(() -> {
            if (log.isInfoEnabled()) {
                log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                                "walSegmentsCovered=%s, markDuration=%dms, recoveryWrite=%dms, pagesWrite=%dms, " +
                                "fsync=%dms, total=%dms]",
                        checkpointId,
                        pagesSize,
                        checkpointMark,
                        walRangeStr(walSegsCoveredRange),
                        getDurationMillis(Duration.MARK),
                        getDurationMillis(Duration.RECOVERY_DATA_WRITE),
                        getDurationMillis(Duration.PAGES_WRITE),
                        getDurationMillis(Duration.FSYNC),
                        getDurationMillis(Duration.TOTAL)));
            }
        });
    }

    /**
     * Tries to log message with checkpoint skip details
     * @param chp Checkpoint (This metrics owner)
     */
    void logCheckpointSkip(Checkpoint chp) {
        String skipReason = chp.progress.reason();
        invokeLater(() -> {
            if (log.isInfoEnabled()) {
                LT.info(log, String.format(
                        "Skipping checkpoint (no pages were modified) [" +
                                "checkpointBeforeLockTime=%dms, checkpointLockWait=%dms, " +
                                "checkpointListenersExecuteTime=%dms, checkpointLockHoldTime=%dms, reason='%s']",
                        getDurationMillis(Duration.BEFORE_LOCK),
                        getDurationMillis(Duration.LOCK_WAIT),
                        getDurationMillis(Duration.LISTENERS_EXECUTE),
                        getDurationMillis(Duration.LOCK_HOLD),
                        skipReason)
                );
            }
        });
    }

    /**
     * Updates performance statistics if feature is enabled
     * @param chp Checkpoint (This metrics owner)
     * @param psproc Performance statistics processor.
     */
    void updatePerformanceStatistics(Checkpoint chp, PerformanceStatisticsProcessor psproc) {
        int pagesSize = chp.pagesSize;
        int dataPagesWritten = dataPagesWritten();
        int cowPagesWritten = cowPagesWritten();
        invokeLater(() -> {
            if (psproc.enabled()) {
                psproc.checkpoint(
                        getDurationMillis(Duration.BEFORE_LOCK),
                        getDurationMillis(Duration.LOCK_WAIT),
                        getDurationMillis(Duration.LISTENERS_EXECUTE),
                        getDurationMillis(Duration.MARK),
                        getDurationMillis(Duration.LOCK_HOLD),
                        getDurationMillis(Duration.PAGES_WRITE),
                        getDurationMillis(Duration.FSYNC),
                        getDurationMillis(Duration.WAL_CP_RECORD_FSYNC),
                        getDurationMillis(Duration.WRITE_CHECKPOINT_ENTRY),
                        getDurationMillis(Duration.SPLIT_AND_SORT_CP_PAGES),
                        getDurationMillis(Duration.RECOVERY_DATA_WRITE),
                        getDurationMillis(Duration.TOTAL),
                        cpStartEpochTime,
                        pagesSize,
                        dataPagesWritten,
                        cowPagesWritten);
            }
        });
    }

    /**
     * Persists metrics of Checkpoint if feature is enabled
     * @param chp Checkpoint (This metrics owner)
     * @param persStoreMetrics Metrics persistence storage.
     * @param cacheProc Cache processor.
     */
    void storeMetrics(Checkpoint chp, DataStorageMetricsImpl persStoreMetrics, GridCacheProcessor cacheProc) {
        boolean metricsEnabled = persStoreMetrics.metricsEnabled();
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cacheProc.context().database();
        int pagesSize = chp.pagesSize;
        long pageStoresSize = dbMgr.forAllPageStores(PageStore::size);
        long pageStoresSparseSize = dbMgr.forAllPageStores(PageStore::getSparseSize);
        int cowPagesWritten = cowPagesWritten();
        int dataPagesWritten = dataPagesWritten();
        invokeLater(() -> {
            if (metricsEnabled) {
                persStoreMetrics.onCheckpoint(
                        getDurationMillis(Duration.BEFORE_LOCK),
                        getDurationMillis(Duration.LOCK_WAIT),
                        getDurationMillis(Duration.LISTENERS_EXECUTE),
                        getDurationMillis(Duration.MARK),
                        getDurationMillis(Duration.LOCK_HOLD),
                        getDurationMillis(Duration.PAGES_WRITE),
                        getDurationMillis(Duration.FSYNC),
                        getDurationMillis(Duration.WAL_CP_RECORD_FSYNC),
                        getDurationMillis(Duration.WRITE_CHECKPOINT_ENTRY),
                        getDurationMillis(Duration.SPLIT_AND_SORT_CP_PAGES),
                        getDurationMillis(Duration.RECOVERY_DATA_WRITE),
                        getDurationMillis(Duration.TOTAL),
                        cpStartEpochTime,
                        pagesSize,
                        dataPagesWritten,
                        cowPagesWritten,
                        cpRecoveryDataSize,
                        pageStoresSize,
                        pageStoresSparseSize
                );
            }
        });
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
     * @return Explain possible JVM pause.
     */
    private String possibleLongJvmPauseExplaination() {
        long lockDuration = getDurationMillis(Duration.TOTAL_LOCK);
        if (LongJVMPauseDetector.enabled() && lockDuration > longJvmPauseThreshold) {
            StringBuilder explainBuilder = new StringBuilder("Checkpoint lock took ")
                    .append(lockDuration).append(" ms, ");
            Optional<String> totalSpottedPausesExplain = pauseDetector.getTotalSpottedPausesExplain(cpStart);
            totalSpottedPausesExplain.ifPresent(explainBuilder::append);
            totalSpottedPausesExplain.ifPresent(ignored -> explainBuilder.append(", "));
            return explainBuilder.toString();
        }
        return "";
    }

    /**
     * Tries to execute task, will not throw {@link java.util.concurrent.RejectedExecutionException}
     * @param runnable Runnable.
     */
    private void invokeLater(Runnable runnable) {
        try {
            CHECK_POINT_METRICS_EXPORT_EXECUTOR.submit(runnable);
        }
        catch (RejectedExecutionException ignored) {
            // do nothing, looks like it is the end
        }
    }

    /**
     * Tries to get previously calculated result
     * or calculates duration
     * @param durationEntry Duration entry.
     */
    private long getDurationMillis(Duration durationEntry) {
        return durationsStorage.computeIfAbsent(durationEntry, duration -> duration.getMillis(this));
    }

    /** Enum contains all tracked durations, it's calculation and comment */
    private enum Duration {
        /** Total checkpoint duration. */
        TOTAL(tracker -> tracker.cpEnd - tracker.cpStart),
        /** Since start due to write lock acquisition */
        BEFORE_LOCK(tracker -> tracker.cpLockWaitStart - tracker.cpStart),
        /** Checkpoint lock wait duration. */
        LOCK_WAIT(tracker -> tracker.cpMarkStart - tracker.cpLockWaitStart),
        /** Checkpoint lock hold duration */
        LOCK_HOLD(tracker -> tracker.cpLockRelease - tracker.cpMarkStart),
        /** Lock wait and hold total duration */
        TOTAL_LOCK(tracker -> tracker.cpLockRelease - tracker.cpLockWaitStart),
        /** Fire listeners under write lock duration */
        LISTENERS_EXECUTE(tracker -> tracker.listenersExecEnd - tracker.cpMarkStart),
        /** Checkpoint mark duration */
        MARK(tracker -> tracker.cpMarkEnd - tracker.cpMarkStart),
        /** Pages write duration */
        PAGES_WRITE(tracker -> tracker.cpFsyncStart - tracker.cpPagesWriteStart),
        /** Checkpoint fsync duration */
        FSYNC(tracker -> tracker.cpEnd - tracker.cpFsyncStart),
        /** Duration of WAL fsync after logging {@link CheckpointRecord} on checkpoint begin. */
        WAL_CP_RECORD_FSYNC(tracker -> tracker.walCpRecordFsyncEnd - tracker.walCpRecordFsyncStart),
        /** Duration of splitting and sorting checkpoint pages */
        SPLIT_AND_SORT_CP_PAGES(tracker -> tracker.splitAndSortCpPagesEnd - tracker.walCpRecordFsyncEnd),
        /** Duration of writing recovery data */
        RECOVERY_DATA_WRITE(tracker -> tracker.cpRecoveryDataWriteEnd - tracker.cpMarkEnd),
        /**
         * Duration of checkpoint entry buffer writing to file.
         *
         * @see CheckpointMarkersStorage#writeCheckpointEntry(long, UUID, WALPointer, CheckpointRecord, CheckpointEntryType, boolean)
         */
        WRITE_CHECKPOINT_ENTRY(tracker -> tracker.cpMarkerStoreEnd - tracker.cpRecoveryDataWriteEnd);

        /** Duration nano time calculation function */
        private final Function<CheckpointMetricsTracker, Long> durationNanosFunction;

        /**
         * @param durationNanosFunction Duration nano time calculation function.
         */
        Duration(Function<CheckpointMetricsTracker, Long> durationNanosFunction) {
            this.durationNanosFunction = durationNanosFunction;
        }

        /**
         * @param tracker Tracker.
         */
        private Long getMillis(CheckpointMetricsTracker tracker) {
            Long nanos = durationNanosFunction.apply(tracker);
            return TimeUnit.NANOSECONDS.toMillis(nanos);
        }
    }
}
