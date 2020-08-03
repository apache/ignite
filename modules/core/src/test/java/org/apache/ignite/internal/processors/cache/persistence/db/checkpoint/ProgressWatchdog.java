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
package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteSpeedBasedThrottle;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Process watchdog for ignite instance. Detects gaps in progress, prints and saves summary of execution metrics to
 * file. Client implementation should use {@link ProgressWatchdog#reportProgress(int)} to show how much data was
 * processed at client size. Long absence of this calls will cause thread dumps to be generated.
 */
class ProgressWatchdog {
    public static final int CHECK_PERIOD_MSEC = 1000;

    /** Progress counter, Overall records processed. */
    private final LongAdder overallRecordsProcessed = new LongAdder();

    /** Metrics log Txt file writer. */
    private final FileWriter txtWriter;

    /** Client threads name, included into thread dumps. */
    private String clientThreadsName;

    /** Service for scheduled execution of watchdog. */
    private ScheduledExecutorService svc = Executors.newScheduledThreadPool(1);

    /** Operation name for messages and log. */
    private final String operation;

    /** Ignite instance. */
    private Ignite ignite;

    /** Stopping flag, indicates ignite close was called but stop is not finished. */
    private volatile boolean stopping;

    /** Value of records count at previous tick, see {@link #overallRecordsProcessed} */
    private final AtomicLong prevRecordsCnt = new AtomicLong();

    /** Milliseconds elapsed at previous tick. */
    private final AtomicLong prevMsElapsed = new AtomicLong();

    /** Checkpoint written pages at previous tick. */
    private final AtomicLong prevCpWrittenPages = new AtomicLong();

    /** Checkpoint fsync()'ed pages at previous tick. */
    private final AtomicLong prevCpSyncedPages = new AtomicLong();

    /** WAL pointer at previous tick reference. */
    private final AtomicReference<FileWALPointer> prevWalPtrRef = new AtomicReference<>();

    /** Milliseconds at start of watchdog execution. */
    private long msStart;

    /**
     * Creates watchdog.
     *
     * @param ignite Ignite.
     * @param operation Operation name for log.
     * @param clientThreadsName Client threads name.
     */
    ProgressWatchdog(Ignite ignite, String operation,
        String clientThreadsName) throws IgniteCheckedException, IOException {
        this.ignite = ignite;
        this.operation = operation;
        txtWriter = new FileWriter(new File(getTempDirFile(), "watchdog-" + operation + ".txt"));
        this.clientThreadsName = clientThreadsName;
        line("sec",
            "cur." + operation + "/sec",
            "WAL speed, MB/s.",
            "cp. speed, MB/sec",
            "cp. sync., MB/sec",
            "WAL work seg.",
            "Throttle part time",
            "curDirtyRatio",
            "targetDirtyRatio",
            "throttleWeigth",
            "markDirtySpeed",
            "cpWriteSpeed",
            "estMarkAllSpeed",
            "avg." + operation + "/sec",
            "dirtyPages",
            "cpWrittenPages",
            "cpSyncedPages",
            "cpEvictedPages",
            "WAL idx",
            "Arch. idx",
            "WAL Archive seg.");
    }

    /**
     * @return temp dir to place watchdog report.
     * @throws IgniteCheckedException if failed.
     */
    @NotNull private static File getTempDirFile() throws IgniteCheckedException {
        File tempDir = new File(U.defaultWorkDirectory(), "temp");

        if (!tempDir.exists())
            tempDir.mkdirs();

        return tempDir;
    }

    /**
     * Generates limited thread dump with only threads involved into persistence.
     *
     * @return string to log.
     */
    private String generateThreadDump() {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        int depth = 100;
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), depth);
        final StringBuilder dump = new StringBuilder();

        for (ThreadInfo threadInfo : threadInfos) {
            String name = threadInfo.getThreadName();

            if (name.contains("checkpoint-runner")
                || name.contains("db-checkpoint-thread")
                || name.contains("wal-file-archiver")
                || name.contains("data-streamer")
                || (clientThreadsName != null && name.contains(clientThreadsName))) {
                String str = threadInfo.toString();

                if (name.contains("db-checkpoint-thread")) {
                    dump.append(str);
                    dump.append("(Full stacktrace)");

                    StackTraceElement[] stackTrace = threadInfo.getStackTrace();
                    int i = 0;

                    for (; i < stackTrace.length && i < depth; i++) {
                        StackTraceElement ste = stackTrace[i];
                        dump.append("\tat ").append(ste.toString());
                        dump.append('\n');
                    }

                    if (i < stackTrace.length) {
                        dump.append("\t...");
                        dump.append('\n');
                    }

                    dump.append('\n');
                }
                else
                    dump.append(str);
            }
            else {
                String s = threadInfo.toString();

                if (s.contains(FileWriteAheadLogManager.class.getSimpleName())
                    || s.contains(FilePageStoreManager.class.getSimpleName()))
                    dump.append(s);
            }
        }
        return dump.toString();
    }

    /**
     * Adds line to txt log {@link #txtWriter}.
     * @param parms values to log
     */
    private void line(Object... parms) {
        try {
            for (int i = 0; i < parms.length; i++) {
                Object parm = parms[i];
                String delim = (i < parms.length - 1) ? "\t" : "\n";

                txtWriter.write(parm + delim);
            }

            txtWriter.flush();
        }
        catch (IOException ignored) {
        }
    }

    /**
     * Starts watchdog execution.
     */
    public void start() {
        msStart = U.currentTimeMillis();
        prevMsElapsed.set(0);
        prevRecordsCnt.set(0);
        prevCpWrittenPages.set(0);
        prevCpSyncedPages.set(0);
        prevWalPtrRef.set(null);

        svc.scheduleAtFixedRate(
            this::tick,
            CHECK_PERIOD_MSEC, CHECK_PERIOD_MSEC, TimeUnit.MILLISECONDS);
    }

    /**
     * Regular method printing statistics to out and to log. Checks gaps in progress.
     */
    private void tick() {
        long elapsedMs = U.currentTimeMillis() - msStart;
        final long totalCnt = overallRecordsProcessed.longValue();
        long elapsedMsFromPrevTick = elapsedMs - prevMsElapsed.getAndSet(elapsedMs);
        if (elapsedMsFromPrevTick == 0)
            return;

        final long currPutPerSec = ((totalCnt - prevRecordsCnt.getAndSet(totalCnt)) * 1000) / elapsedMsFromPrevTick;
        final long averagePutPerSec = totalCnt * 1000 / elapsedMs;
        boolean slowProgress = currPutPerSec < averagePutPerSec / 10 && !stopping;
        final String fileNameWithDump = slowProgress ? reactNoProgress() : "";

        DataStorageConfiguration dsCfg = ignite.configuration().getDataStorageConfiguration();

        String defRegName = dsCfg.getDefaultDataRegionConfiguration().getName();
        long dirtyPages = -1;
        for (DataRegionMetrics m : ignite.dataRegionMetrics())
            if (m.getName().equals(defRegName))
                dirtyPages = m.getDirtyPages();

        GridCacheSharedContext<Object, Object> cacheSctx = null;
        PageMemoryImpl pageMemory = null;
        try {
            cacheSctx = ((IgniteEx)ignite).context().cache().context();
            pageMemory = (PageMemoryImpl)cacheSctx.database().dataRegion(defRegName).pageMemory();
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }

        long cpBufPages = 0;

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)(cacheSctx.database());
        AtomicInteger wrPageCntr = db.getCheckpointer().currentProgress().writtenPagesCounter();
        long cpWrittenPages = wrPageCntr == null ? 0 : wrPageCntr.get();

        AtomicInteger syncedPagesCntr = db.getCheckpointer().currentProgress().syncedPagesCounter();
        int cpSyncedPages = syncedPagesCntr == null ? 0 : syncedPagesCntr.get();

        AtomicInteger evictedPagesCntr = db.getCheckpointer().currentProgress().evictedPagesCounter();
        int cpEvictedPages = evictedPagesCntr == null ? 0 : evictedPagesCntr.get();

        int pageSize = pageMemory == null ? 0 : pageMemory.pageSize();

        String cpWriteSpeed = getMBytesPrintable(
            detectDelta(elapsedMsFromPrevTick, cpWrittenPages, prevCpWrittenPages) * pageSize);

        String cpSyncSpeed = getMBytesPrintable(
            detectDelta(elapsedMsFromPrevTick, cpSyncedPages, prevCpSyncedPages) * pageSize);

        String walSpeed = "";
        long throttleParkTimeNanos = 0;
        double curDirtyRatio = 0.0;
        String targetDirtyRatioStr = "";
        double closeToThrottle = 0.0;
        long idx = -1;
        long lastArchIdx = -1;
        int walArchiveSegments = 0;
        long walWorkSegments = 0;
        long markDirtySpeed = 0;
        long cpWriteSpeedInPages = 0;
        long estWrAllSpeed = 0;

        try {
            if (pageMemory != null) {
                cpBufPages = pageMemory.checkpointBufferPagesCount();

                PagesWriteSpeedBasedThrottle throttle = U.field(pageMemory, "writeThrottle");

                if (throttle != null) {
                    curDirtyRatio = throttle.getCurrDirtyRatio();

                    double targetDirtyRatio = throttle.getTargetDirtyRatio();

                    targetDirtyRatioStr = targetDirtyRatio < 0 ? "" : formatDbl(targetDirtyRatio);

                    closeToThrottle = throttle.throttleWeight();
                    throttleParkTimeNanos = throttle.throttleParkTime();
                    markDirtySpeed = throttle.getMarkDirtySpeed();
                    cpWriteSpeedInPages = throttle.getCpWriteSpeed();
                    estWrAllSpeed = throttle.getLastEstimatedSpeedForMarkAll();
                    if (estWrAllSpeed > 99_999)
                        estWrAllSpeed = 99_999;
                }
            }

            FileWriteAheadLogManager wal = (FileWriteAheadLogManager)cacheSctx.wal();

            idx = 0;
            lastArchIdx = 0;

            walArchiveSegments = wal.walArchiveSegments();
            walWorkSegments = idx - lastArchIdx;

            /* // uncomment when currentWritePointer is available
             FileWALPointer ptr = wal.currentWritePointer();
               FileWALPointer prevWalPtr = this.prevWalPtrRef.getAndSet(ptr);

               if (prevWalPtr != null) {
                   long idxDiff = ptr.index() - prevWalPtr.index();
                   long offDiff = ptr.fileOffset() - prevWalPtr.fileOffset();
                   long bytesDiff = idxDiff * maxWalSegmentSize + offDiff;

                   long bytesPerSec = (bytesDiff * 1000) / elapsedMsFromPrevTick;

                   walSpeed = getMBytesPrintable(bytesPerSec);
               } else
                   walSpeed = "0";
             */

            walSpeed = "0";
        }
        catch (Exception e) {
            X.error(e.getClass().getSimpleName() + ":" + e.getMessage());
        }

        long elapsedSecs = elapsedMs / 1000;
        X.println(" >> " +
            operation +
            " done: " + totalCnt + "/" + elapsedSecs + "s, " +
            "Cur. " + operation + " " + currPutPerSec + " recs/sec " +
            "cpWriteSpeed=" + cpWriteSpeed + " " +
            "cpSyncSpeed=" + cpSyncSpeed + " " +
            "walSpeed= " + walSpeed + " " +
            "walWorkSeg.=" + walWorkSegments + " " +
            "markDirtySpeed=" + markDirtySpeed + " " +
            "Avg. " + operation + " " + averagePutPerSec + " recs/sec, " +
            "dirtyP=" + dirtyPages + ", " +
            "cpWrittenP.=" + cpWrittenPages + ", " +
            "cpBufP.=" + cpBufPages + " " +
            "threshold=" + targetDirtyRatioStr + " " +
            "walIdx=" + idx + " " +
            "archWalIdx=" + lastArchIdx + " " +
            "walArchiveSegments=" + walArchiveSegments + " " +
            fileNameWithDump);

        line(elapsedSecs,
            currPutPerSec,
            walSpeed,
            cpWriteSpeed,
            cpSyncSpeed,
            walWorkSegments,
            throttleParkTimeNanos,
            formatDbl(curDirtyRatio),
            targetDirtyRatioStr,
            formatDbl(closeToThrottle),
            markDirtySpeed,
            cpWriteSpeedInPages,
            estWrAllSpeed,
            averagePutPerSec,
            dirtyPages,
            cpWrittenPages,
            cpSyncedPages,
            cpEvictedPages,
            idx,
            lastArchIdx,
            walArchiveSegments
        );
    }

    /**
     * @param val value to log.
     * @return formatted value for txt log.
     */
    private String formatDbl(double val) {
        return String.format("%.2f", val).replace(",", ".");
    }

    /**
     * Converts bytes counter to MegaBytes
     * @param currBytesWritten bytes.
     * @return string with megabytes as printable string.
     */
    private String getMBytesPrintable(long currBytesWritten) {
        double cpMbPs = 1.0 * currBytesWritten / (1024 * 1024);

        return formatDbl(cpMbPs);
    }

    /**
     * @param elapsedMsFromPrevTick time from previous tick, millis.
     * @param absVal current value
     * @param cnt counter stores previous value.
     * @return value change from previous tick.
     */
    private long detectDelta(long elapsedMsFromPrevTick, long absVal, AtomicLong cnt) {
        long cpPagesChange = absVal - cnt.getAndSet(absVal);

        if (cpPagesChange < 0)
            cpPagesChange = 0;

        return (cpPagesChange * 1000) / elapsedMsFromPrevTick;
    }

    /**
     * @return file name with dump created.
     */
    private String reactNoProgress() {
        try {
            String threadDump = generateThreadDump();

            long sec = TimeUnit.MILLISECONDS.toSeconds(U.currentTimeMillis() - msStart);

            String fileName = "dumpAt" + sec + "second.txt";

            if (threadDump.contains(IgniteCacheDatabaseSharedManager.class.getName() + ".checkpointLock"))
                fileName = "checkpoint_" + fileName;

            fileName = operation + fileName;

            try (FileWriter writer = new FileWriter(new File(getTempDirFile(), fileName))) {
                writer.write(threadDump);
            }

            return fileName;
        }
        catch (IOException | IgniteCheckedException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * @param cnt counter of entries/operations processed since last call.
     */
    public void reportProgress(int cnt) {
        overallRecordsProcessed.add(cnt);
    }

    /**
     * Call this method to indicate client operation is done, and ignite is stopping.
     */
    public void stopping() {
        stopping = true;
    }

    /**
     * Stops watchdog threads.
     */
    public void stop() {
        U.closeQuiet(txtWriter);

        ScheduledExecutorService pool = this.svc;
        stopPool(pool);
    }

    public static void stopPool(ExecutorService pool) {
        pool.shutdown();
        try {
            pool.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

}
