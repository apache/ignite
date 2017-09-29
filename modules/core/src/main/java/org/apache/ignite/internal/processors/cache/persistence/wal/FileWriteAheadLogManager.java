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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.sql.Time;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.SwitchSegmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.PersistenceMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordDataV1Serializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordDataV2Serializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV2Serializer;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * File WAL manager.
 */
public class FileWriteAheadLogManager extends GridCacheSharedManagerAdapter implements IgniteWriteAheadLogManager {
    /** */
    public static final FileDescriptor[] EMPTY_DESCRIPTORS = new FileDescriptor[0];

    /** */
    public static final String WAL_SEGMENT_FILE_EXT = ".wal";

    /** */
    private static final byte[] FILL_BUF = new byte[1024 * 1024];

    /** Pattern for segment file names */
    private static final Pattern WAL_NAME_PATTERN = Pattern.compile("\\d{16}\\.wal");

    /** */
    private static final Pattern WAL_TEMP_NAME_PATTERN = Pattern.compile("\\d{16}\\.wal\\.tmp");

    /** WAL segment file filter, see {@link #WAL_NAME_PATTERN} */
    public static final FileFilter WAL_SEGMENT_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File file) {
            return !file.isDirectory() && WAL_NAME_PATTERN.matcher(file.getName()).matches();
        }
    };

    /** */
    private static final FileFilter WAL_SEGMENT_TEMP_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File file) {
            return !file.isDirectory() && WAL_TEMP_NAME_PATTERN.matcher(file.getName()).matches();
        }
    };

    /** Latest serializer version to use. */
    public static final int LATEST_SERIALIZER_VERSION = 1;

    /** */
    private final boolean alwaysWriteFullPages;

    /** WAL segment size in bytes */
    private final long maxWalSegmentSize;

    /** */
    private final WALMode mode;

    /** Thread local byte buffer size, see {@link #tlb} */
    private final int tlbSize;

    /** WAL flush frequency. Makes sense only for {@link WALMode#BACKGROUND} log WALMode. */
    private final long flushFreq;

    /** Fsync delay. */
    private final long fsyncDelay;

    /** */
    private final PersistentStoreConfiguration psCfg;

    /** Events service */
    private final GridEventStorageManager evt;

    /** */
    private IgniteConfiguration igCfg;

    /** Persistence metrics tracker. */
    private PersistenceMetricsImpl metrics;

    /** */
    private File walWorkDir;

    /** WAL archive directory (including consistent ID as subfolder) */
    private File walArchiveDir;

    /** Serializer of latest version. */
    private RecordSerializer serializer;

    /** Serializer latest version to use. */
    private int serializerVersion = LATEST_SERIALIZER_VERSION;

    /** */
    private volatile long oldestArchiveSegmentIdx;

    /** Factory to provide I/O interfaces for read/write operations with files */
    private final FileIOFactory ioFactory;

    /** Updater for {@link #currentHnd}, used for verify there are no concurrent update for current log segment handle */
    private static final AtomicReferenceFieldUpdater<FileWriteAheadLogManager, FileWriteHandle> currentHndUpd =
        AtomicReferenceFieldUpdater.newUpdater(FileWriteAheadLogManager.class, FileWriteHandle.class, "currentHnd");

    /**
     * Thread local byte buffer for saving serialized WAL records chain, see {@link FileWriteHandle#head}.
     * Introduced to decrease number of buffers allocation.
     * Used only for record itself is shorter than {@link #tlbSize}.
     */
    private final ThreadLocal<ByteBuffer> tlb = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            ByteBuffer buf = ByteBuffer.allocateDirect(tlbSize);

            buf.order(GridUnsafe.NATIVE_BYTE_ORDER);

            return buf;
        }
    };

    /** */
    private volatile FileArchiver archiver;

    /** */
    private final ThreadLocal<WALPointer> lastWALPtr = new ThreadLocal<>();

    /** Current log segment handle */
    private volatile FileWriteHandle currentHnd;

    /** Environment failure. */
    private volatile Throwable envFailed;

    /**
     * Positive (non-0) value indicates WAL can be archived even if not complete<br>
     * See {@link PersistentStoreConfiguration#setWalAutoArchiveAfterInactivity(long)}<br>
     */
    private final long walAutoArchiveAfterInactivity;

    /**
     * Container with last WAL record logged timestamp.<br>
     * Zero value means there was no records logged to current segment, skip possible archiving for this case<br>
     * Value is filled only for case {@link #walAutoArchiveAfterInactivity} > 0<br>
     */
    private AtomicLong lastRecordLoggedMs = new AtomicLong();

    /**
     * Cancellable task for {@link WALMode#BACKGROUND}, should be cancelled at shutdown
     * Null for non background modes
     */
    @Nullable private volatile GridTimeoutProcessor.CancelableTask backgroundFlushSchedule;

    /**
     * Reference to the last added next archive timeout check object.
     * Null if mode is not enabled.
     * Should be cancelled at shutdown
     */
    @Nullable private volatile GridTimeoutObject nextAutoArchiveTimeoutObj;

    /**
     * @param ctx Kernal context.
     */
    public FileWriteAheadLogManager(@NotNull final GridKernalContext ctx) {
        igCfg = ctx.config();

        PersistentStoreConfiguration psCfg = igCfg.getPersistentStoreConfiguration();

        assert psCfg != null : "WAL should not be created if persistence is disabled.";

        this.psCfg = psCfg;

        maxWalSegmentSize = psCfg.getWalSegmentSize();
        mode = psCfg.getWalMode();
        tlbSize = psCfg.getTlbSize();
        flushFreq = psCfg.getWalFlushFrequency();
        fsyncDelay = psCfg.getWalFsyncDelayNanos();
        alwaysWriteFullPages = psCfg.isAlwaysWriteFullPages();
        ioFactory = psCfg.getFileIOFactory();
        walAutoArchiveAfterInactivity = psCfg.getWalAutoArchiveAfterInactivity();
        evt = ctx.event();
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        if (!cctx.kernalContext().clientNode()) {
            String consId = consistentId();

            A.notNullOrEmpty(consId, "consistentId");

            consId = U.maskForFileName(consId);

            checkWalConfiguration();

            walWorkDir = initDirectory(
                psCfg.getWalStorePath(),
                PersistentStoreConfiguration.DFLT_WAL_STORE_PATH,
                consId,
                "write ahead log work directory"
            );

            walArchiveDir = initDirectory(
                psCfg.getWalArchivePath(),
                PersistentStoreConfiguration.DFLT_WAL_ARCHIVE_PATH,
                consId,
                "write ahead log archive directory"
            );

            serializer = forVersion(cctx, serializerVersion);

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

            metrics = dbMgr.persistentStoreMetricsImpl();

            checkOrPrepareFiles();

            IgniteBiTuple<Long, Long> tup = scanMinMaxArchiveIndices();

            oldestArchiveSegmentIdx = tup == null ? 0 : tup.get1();

            archiver = new FileArchiver(tup == null ? -1 : tup.get2());

            if (mode != WALMode.DEFAULT) {
                if (log.isInfoEnabled())
                    log.info("Started write-ahead log manager [mode=" + mode + ']');
            }
        }
    }

    /**
     * @throws IgniteCheckedException if WAL store path is configured and archive path isn't (or vice versa)
     */
    private void checkWalConfiguration() throws IgniteCheckedException {
        if (psCfg.getWalStorePath() == null ^ psCfg.getWalArchivePath() == null) {
            throw new IgniteCheckedException(
                "Properties should be either both specified or both null " +
                    "[walStorePath = " + psCfg.getWalStorePath() +
                    ", walArchivePath = " + psCfg.getWalArchivePath() + "]"
            );
        }
    }

    /**
     * @return Consistent ID.
     */
    protected String consistentId() {
        return cctx.discovery().consistentId().toString();
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        final GridTimeoutProcessor.CancelableTask schedule = backgroundFlushSchedule;

        if (schedule != null)
            schedule.close();

        final GridTimeoutObject timeoutObj = nextAutoArchiveTimeoutObj;

        if (timeoutObj != null)
            cctx.time().removeTimeoutObject(timeoutObj);

        final FileWriteHandle currHnd = currentHandle();

        try {
            if (mode == WALMode.BACKGROUND) {
                if (currHnd != null)
                    currHnd.flush((FileWALPointer)null, true);
            }

            if (currHnd != null)
                currHnd.close(false);

            if (archiver != null)
                archiver.shutdown();
        }
        catch (Exception e) {
            U.error(log, "Failed to gracefully close WAL segment: " + currentHnd.fileIO, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activated file write ahead log manager [nodeId=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        start0();

        if (!cctx.kernalContext().clientNode()) {
            assert archiver != null;

            archiver.start();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate file write ahead log [nodeId=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        stop0(true);

        currentHnd = null;
    }

    /** {@inheritDoc} */
    @Override public boolean isAlwaysWriteFullPages() {
        return alwaysWriteFullPages;
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSync() {
        return mode == WALMode.DEFAULT;
    }

    /** {@inheritDoc} */
    @Override public void resumeLogging(WALPointer lastPtr) throws IgniteCheckedException {
        try {
            assert currentHnd == null;
            assert lastPtr == null || lastPtr instanceof FileWALPointer;

            FileWALPointer filePtr = (FileWALPointer)lastPtr;

            currentHnd = restoreWriteHandle(filePtr);

            if (currentHnd.serializer.version() != serializer.version()) {
                if (log.isInfoEnabled())
                    log.info("Record serializer version change detected, will start logging with a new WAL record " +
                        "serializer to a new WAL segment [curFile=" + currentHnd + ", newVer=" + serializer.version() +
                        ", oldVer=" + currentHnd.serializer.version() + ']');

                rollOver(currentHnd);
            }

            if (mode == WALMode.BACKGROUND) {
                backgroundFlushSchedule = cctx.time().schedule(new Runnable() {
                    @Override public void run() {
                        doFlush();
                    }
                }, flushFreq, flushFreq);
            }

            if (walAutoArchiveAfterInactivity > 0)
                scheduleNextInactivityPeriodElapsedCheck();
        }
        catch (StorageException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Schedules next check of inactivity period expired. Based on current record update timestamp.
     * At timeout method does check of inactivity period and schedules new launch.
     */
    private void scheduleNextInactivityPeriodElapsedCheck() {
        final long lastRecMs = lastRecordLoggedMs.get();
        final long nextPossibleAutoArchive = (lastRecMs <= 0 ? U.currentTimeMillis() : lastRecMs) + walAutoArchiveAfterInactivity;

        if (log.isDebugEnabled())
            log.debug("Schedule WAL rollover check at " + new Time(nextPossibleAutoArchive).toString());

        nextAutoArchiveTimeoutObj = new GridTimeoutObject() {
            private final IgniteUuid id = IgniteUuid.randomUuid();

            @Override public IgniteUuid timeoutId() {
                return id;
            }

            @Override public long endTime() {
                return nextPossibleAutoArchive;
            }

            @Override public void onTimeout() {
                if (log.isDebugEnabled())
                    log.debug("Checking if WAL rollover required (" + new Time(U.currentTimeMillis()).toString() + ")");

                checkWalRolloverRequiredDuringInactivityPeriod();

                scheduleNextInactivityPeriodElapsedCheck();
            }
        };
        cctx.time().addTimeoutObject(nextAutoArchiveTimeoutObj);
    }

    /**
     * Checks if there was elapsed significant period of inactivity.
     * If WAL auto-archive is enabled using {@link #walAutoArchiveAfterInactivity} > 0 this method will activate
     * roll over by timeout<br>
     */
    private void checkWalRolloverRequiredDuringInactivityPeriod() {
        if (walAutoArchiveAfterInactivity <= 0)
            return; // feature not configured, nothing to do

        final long lastRecMs = lastRecordLoggedMs.get();

        if (lastRecMs == 0)
            return; //no records were logged to current segment, does not consider inactivity

        final long elapsedMs = U.currentTimeMillis() - lastRecMs;

        if (elapsedMs <= walAutoArchiveAfterInactivity)
            return; // not enough time elapsed since last write

        if (!lastRecordLoggedMs.compareAndSet(lastRecMs, 0))
            return; // record write occurred concurrently

        final FileWriteHandle handle = currentHandle();

        try {
            rollOver(handle);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Unable to perform segment rollover: " + e.getMessage(), e);
            handle.invalidateEnvironment(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("TooBroadScope")
    @Override public WALPointer log(WALRecord record) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == WALMode.NONE)
            return null;

        FileWriteHandle currWrHandle = currentHandle();

        // Logging was not resumed yet.
        if (currWrHandle == null)
            return null;

        // Need to calculate record size first.
        record.size(serializer.size(record));

        for (; ; currWrHandle = rollOver(currWrHandle)) {
            WALPointer ptr = currWrHandle.addRecord(record);

            if (ptr != null) {
                metrics.onWalRecordLogged();

                lastWALPtr.set(ptr);

                if (walAutoArchiveAfterInactivity > 0)
                    lastRecordLoggedMs.set(U.currentTimeMillis());

                return ptr;
            }

            checkEnvironment();

            if (isStopping())
                throw new IgniteCheckedException("Stopping.");
        }
    }

    /** {@inheritDoc} */
    @Override public void fsync(WALPointer ptr) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == WALMode.NONE)
            return;

        FileWriteHandle cur = currentHandle();

        // WAL manager was not started (client node).
        if (cur == null)
            return;

        FileWALPointer filePtr = (FileWALPointer)(ptr == null ? lastWALPtr.get() : ptr);

        boolean forceFlush = filePtr != null && filePtr.forceFlush();

        if (mode == WALMode.BACKGROUND && !forceFlush)
            return;

        if (mode == WALMode.LOG_ONLY || forceFlush) {
            cur.flushOrWait(filePtr, false);

            return;
        }

        // No need to sync if was rolled over.
        if (filePtr != null && !cur.needFsync(filePtr))
            return;

        cur.fsync(filePtr, false);
    }

    /** {@inheritDoc} */
    @Override public WALIterator replay(WALPointer start)
        throws IgniteCheckedException, StorageException {
        assert start == null || start instanceof FileWALPointer : "Invalid start pointer: " + start;

        FileWriteHandle hnd = currentHandle();

        FileWALPointer end = null;

        if (hnd != null)
            end = hnd.position();

        return new RecordsIterator(
            cctx,
            walWorkDir,
            walArchiveDir,
            (FileWALPointer)start,
            end,
            psCfg,
            serializer,
            ioFactory,
            archiver,
            log,
            tlbSize
        );
    }

    /** {@inheritDoc} */
    @Override public boolean reserve(WALPointer start) throws IgniteCheckedException {
        assert start != null && start instanceof FileWALPointer : "Invalid start pointer: " + start;

        if (mode == WALMode.NONE)
            return false;

        FileArchiver archiver0 = archiver;

        if (archiver0 == null)
            throw new IgniteCheckedException("Could not reserve WAL segment: archiver == null");

        archiver0.reserve(((FileWALPointer)start).index());

        if (!hasIndex(((FileWALPointer)start).index())) {
            archiver0.release(((FileWALPointer)start).index());

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void release(WALPointer start) throws IgniteCheckedException {
        assert start != null && start instanceof FileWALPointer : "Invalid start pointer: " + start;

        if (mode == WALMode.NONE)
            return;

        FileArchiver archiver0 = archiver;

        if (archiver0 == null)
            throw new IgniteCheckedException("Could not release WAL segment: archiver == null");

        archiver0.release(((FileWALPointer)start).index());
    }

    /**
     * @param absIdx Absolulte index to check.
     * @return {@code true} if has this index.
     */
    private boolean hasIndex(long absIdx) {
        String name = FileDescriptor.fileName(absIdx);

        boolean inArchive = new File(walArchiveDir, name).exists();

        if (inArchive)
            return true;

        if (absIdx <= lastArchivedIndex())
            return false;

        FileWriteHandle cur = currentHnd;

        return cur != null && cur.idx >= absIdx;
    }

    /** {@inheritDoc} */
    @Override public int truncate(WALPointer ptr) {
        if (ptr == null)
            return 0;

        assert ptr instanceof FileWALPointer : ptr;

        // File pointer bound: older entries will be deleted from archive
        FileWALPointer fPtr = (FileWALPointer)ptr;

        FileDescriptor[] descs = scan(walArchiveDir.listFiles(WAL_SEGMENT_FILE_FILTER));

        int deleted = 0;

        FileArchiver archiver0 = archiver;

        for (FileDescriptor desc : descs) {
            // Do not delete reserved or locked segment and any segment after it.
            if (archiver0 != null && archiver0.reserved(desc.idx))
                return deleted;

            // We need to leave at least one archived segment to correctly determine the archive index.
            if (desc.idx + 1 < fPtr.index()) {
                if (!desc.file.delete())
                    U.warn(log, "Failed to remove obsolete WAL segment (make sure the process has enough rights): " +
                        desc.file.getAbsolutePath());
                else
                    deleted++;

                // Bump up the oldest archive segment index.
                if (oldestArchiveSegmentIdx < desc.idx)
                    oldestArchiveSegmentIdx = desc.idx;
            }
        }

        return deleted;
    }

    /** {@inheritDoc} */
    @Override public int walArchiveSegments() {
        long oldest = oldestArchiveSegmentIdx;

        long lastArchived = archiver.lastArchivedAbsoluteIndex();

        if (lastArchived == -1)
            return 0;

        int res = (int)(lastArchived - oldest);

        return res >= 0 ? res : 0;
    }

    /** {@inheritDoc} */
    @Override public boolean reserved(WALPointer ptr) {
        FileWALPointer fPtr = (FileWALPointer)ptr;

        FileArchiver archiver0 = archiver;

        return archiver0 != null && archiver0.reserved(fPtr.index());
    }

    /**
     * Lists files in archive directory and returns the index of last archived file.
     *
     * @return The absolute index of last archived file.
     */
    private long lastArchivedIndex() {
        long lastIdx = -1;

        for (File file : walArchiveDir.listFiles(WAL_SEGMENT_FILE_FILTER)) {
            try {
                long idx = Long.parseLong(file.getName().substring(0, 16));

                lastIdx = Math.max(lastIdx, idx);
            }
            catch (NumberFormatException | IndexOutOfBoundsException ignore) {

            }
        }

        return lastIdx;
    }

    /**
     * Lists files in archive directory and returns the index of last archived file.
     *
     * @return The absolute index of last archived file.
     */
    private IgniteBiTuple<Long, Long> scanMinMaxArchiveIndices() {
        long minIdx = Integer.MAX_VALUE;
        long maxIdx = -1;

        for (File file : walArchiveDir.listFiles(WAL_SEGMENT_FILE_FILTER)) {
            try {
                long idx = Long.parseLong(file.getName().substring(0, 16));

                minIdx = Math.min(minIdx, idx);
                maxIdx = Math.max(maxIdx, idx);
            }
            catch (NumberFormatException | IndexOutOfBoundsException ignore) {

            }
        }

        return maxIdx == -1 ? null : F.t(minIdx, maxIdx);
    }

    /**
     * Creates a directory specified by the given arguments.
     *
     * @param cfg Configured directory path, may be {@code null}.
     * @param defDir Default directory path, will be used if cfg is {@code null}.
     * @param consId Local node consistent ID.
     * @param msg File description to print out on successful initialization.
     * @return Initialized directory.
     * @throws IgniteCheckedException If failed to initialize directory.
     */
    private File initDirectory(String cfg, String defDir, String consId, String msg) throws IgniteCheckedException {
        File dir;

        if (cfg != null) {
            File workDir0 = new File(cfg);

            dir = workDir0.isAbsolute() ?
                new File(workDir0, consId) :
                new File(U.resolveWorkDirectory(igCfg.getWorkDirectory(), cfg, false), consId);
        }
        else
            dir = new File(U.resolveWorkDirectory(igCfg.getWorkDirectory(), defDir, false), consId);

        U.ensureDirectory(dir, msg, log);

        return dir;
    }

    /**
     * @return Current log segment handle.
     */
    private FileWriteHandle currentHandle() {
        return currentHnd;
    }

    /**
     * @param cur Handle that failed to fit the given entry.
     * @return Handle that will fit the entry.
     */
    private FileWriteHandle rollOver(FileWriteHandle cur) throws StorageException, IgniteCheckedException {
        FileWriteHandle hnd = currentHandle();

        if (hnd != cur)
            return hnd;

        if (hnd.close(true)) {
            FileWriteHandle next = initNextWriteHandle(cur.idx);

            boolean swapped = currentHndUpd.compareAndSet(this, hnd, next);

            assert swapped : "Concurrent updates on rollover are not allowed";

            if (walAutoArchiveAfterInactivity > 0)
                lastRecordLoggedMs.set(0);

            // Let other threads to proceed with new segment.
            hnd.signalNextAvailable();
        }
        else
            hnd.awaitNext();

        return currentHandle();
    }

    /**
     * @param lastReadPtr Last read WAL file pointer.
     * @return Initialized file write handle.
     * @throws IgniteCheckedException If failed to initialize WAL write handle.
     */
    private FileWriteHandle restoreWriteHandle(FileWALPointer lastReadPtr) throws IgniteCheckedException {
        long absIdx = lastReadPtr == null ? 0 : lastReadPtr.index();

        long segNo = absIdx % psCfg.getWalSegments();

        File curFile = new File(walWorkDir, FileDescriptor.fileName(segNo));

        int offset = lastReadPtr == null ? 0 : lastReadPtr.fileOffset();
        int len = lastReadPtr == null ? 0 : lastReadPtr.length();

        try {
            FileIO fileIO = ioFactory.create(curFile);

            try {
                int serVer = serializerVersion;

                // If we have existing segment, try to read version from it.
                if (lastReadPtr != null) {
                    try {
                        serVer = readSerializerVersion(fileIO);
                    }
                    catch (SegmentEofException | EOFException ignore) {
                        serVer = serializerVersion;
                    }
                }

                RecordSerializer ser = forVersion(cctx, serVer);

                if (log.isInfoEnabled())
                    log.info("Resuming logging to WAL segment [file=" + curFile.getAbsolutePath() +
                        ", offset=" + offset + ", ver=" + serVer + ']');

                FileWriteHandle hnd = new FileWriteHandle(
                    fileIO,
                    absIdx,
                    cctx.igniteInstanceName(),
                    offset + len,
                    maxWalSegmentSize,
                    ser);

                // For new handle write serializer version to it.
                if (lastReadPtr == null)
                    hnd.writeSerializerVersion();

                archiver.currentWalIndex(absIdx);

                return hnd;
            }
            catch (IgniteCheckedException | IOException e) {
                fileIO.close();

                throw e;
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to restore WAL write handle: " + curFile.getAbsolutePath(), e);
        }
    }

    /**
     * Fills the file header for a new segment.
     * Calling this method signals we are done with the segment and it can be archived.
     * If we don't have prepared file yet and achiever is busy this method blocks
     *
     * @param curIdx current absolute segment released by WAL writer
     * @return Initialized file handle.
     * @throws StorageException If IO exception occurred.
     * @throws IgniteCheckedException If failed.
     */
    private FileWriteHandle initNextWriteHandle(long curIdx) throws StorageException, IgniteCheckedException {
        try {
            File nextFile = pollNextFile(curIdx);

            if (log.isDebugEnabled())
                log.debug("Switching to a new WAL segment: " + nextFile.getAbsolutePath());

            FileIO fileIO = ioFactory.create(nextFile);

            FileWriteHandle hnd = new FileWriteHandle(
                fileIO,
                curIdx + 1,
                cctx.igniteInstanceName(),
                0,
                maxWalSegmentSize,
                serializer);

            hnd.writeSerializerVersion();

            return hnd;
        }
        catch (IOException e) {
            throw new StorageException(e);
        }
    }

    /**
     * Deletes temp files, creates and prepares new; Creates first segment if necessary
     */
    private void checkOrPrepareFiles() throws IgniteCheckedException {
        // Clean temp files.
        {
            File[] tmpFiles = walWorkDir.listFiles(WAL_SEGMENT_TEMP_FILE_FILTER);

            if (!F.isEmpty(tmpFiles)) {
                for (File tmp : tmpFiles) {
                    boolean deleted = tmp.delete();

                    if (!deleted)
                        throw new IgniteCheckedException("Failed to delete previously created temp file " +
                            "(make sure Ignite process has enough rights): " + tmp.getAbsolutePath());
                }
            }
        }

        File[] allFiles = walWorkDir.listFiles(WAL_SEGMENT_FILE_FILTER);

        if (allFiles.length != 0 && allFiles.length > psCfg.getWalSegments())
            throw new IgniteCheckedException("Failed to initialize wal (work directory contains " +
                "incorrect number of segments) [cur=" + allFiles.length + ", expected=" + psCfg.getWalSegments() + ']');

        // Allocate the first segment synchronously. All other segments will be allocated by archiver in background.
        if (allFiles.length == 0) {
            File first = new File(walWorkDir, FileDescriptor.fileName(0));

            createFile(first);
        }
        else
            checkFiles(0, false, null);
    }

    /**
     * Clears the file with zeros.
     *
     * @param file File to format.
     */
    private void formatFile(File file) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Formatting file [exists=" + file.exists() + ", file=" + file.getAbsolutePath() + ']');

        try (FileIO fileIO = ioFactory.create(file, CREATE, READ, WRITE)) {
            int left = psCfg.getWalSegmentSize();

            if (mode == WALMode.DEFAULT) {
                while (left > 0) {
                    int toWrite = Math.min(FILL_BUF.length, left);

                    fileIO.write(FILL_BUF, 0, toWrite);

                    left -= toWrite;
                }

                fileIO.force();
            }
            else
                fileIO.clear();
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to format WAL segment file: " + file.getAbsolutePath(), e);
        }
    }

    /**
     * Creates a file atomically with temp file.
     *
     * @param file File to create.
     * @throws IgniteCheckedException If failed.
     */
    private void createFile(File file) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Creating new file [exists=" + file.exists() + ", file=" + file.getAbsolutePath() + ']');

        File tmp = new File(file.getParent(), file.getName() + ".tmp");

        formatFile(tmp);

        try {
            Files.move(tmp.toPath(), file.toPath());
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to move temp file to a regular WAL segment file: " +
                file.getAbsolutePath(), e);
        }

        if (log.isDebugEnabled())
            log.debug("Created WAL segment [file=" + file.getAbsolutePath() + ", size=" + file.length() + ']');
    }

    /**
     * Retrieves next available file to write WAL data, waiting
     * if necessary for a segment to become available.
     *
     * @param curIdx Current absolute WAL segment index.
     * @return File ready for use as new WAL segment.
     * @throws IgniteCheckedException If failed.
     */
    private File pollNextFile(long curIdx) throws IgniteCheckedException {
        // Signal to archiver that we are done with the segment and it can be archived.
        long absNextIdx = archiver.nextAbsoluteSegmentIndex(curIdx);

        long segmentIdx = absNextIdx % psCfg.getWalSegments();

        return new File(walWorkDir, FileDescriptor.fileName(segmentIdx));
    }

    /**
     * @param cctx Shared context.
     * @param ver Serializer version.
     * @return Entry serializer.
     */
    public static RecordSerializer forVersion(GridCacheSharedContext cctx, int ver) throws IgniteCheckedException {
        return forVersion(cctx, ver, false);
    }

    /**
     * @param ver Serializer version.
     * @return Entry serializer.
     */
    static RecordSerializer forVersion(GridCacheSharedContext cctx, int ver, boolean writePointer) throws IgniteCheckedException {
        if (ver <= 0)
            throw new IgniteCheckedException("Failed to create a serializer (corrupted WAL file).");

        switch (ver) {
            case 1:
                return new RecordV1Serializer(new RecordDataV1Serializer(cctx), writePointer);

            case 2:
                RecordDataV2Serializer dataV2Serializer = new RecordDataV2Serializer(new RecordDataV1Serializer(cctx));

                return new RecordV2Serializer(dataV2Serializer, writePointer);

            default:
                throw new IgniteCheckedException("Failed to create a serializer with the given version " +
                    "(forward compatibility is not supported): " + ver);
        }
    }

    /**
     * @return Sorted WAL files descriptors.
     */
    public static FileDescriptor[] scan(File[] allFiles) {
        if (allFiles == null)
            return EMPTY_DESCRIPTORS;

        FileDescriptor[] descs = new FileDescriptor[allFiles.length];

        for (int i = 0; i < allFiles.length; i++) {
            File f = allFiles[i];

            descs[i] = new FileDescriptor(f);
        }

        Arrays.sort(descs);

        return descs;
    }

    /**
     * @throws StorageException If environment is no longer valid and we missed a WAL write.
     */
    private void checkEnvironment() throws StorageException {
        if (envFailed != null)
            throw new StorageException("Failed to flush WAL buffer (environment was invalidated by a " +
                    "previous error)", envFailed);
    }

    /**
     * File archiver operates on absolute segment indexes. For any given absolute segment index N we can calculate
     * the work WAL segment: S(N) = N % psCfg.walSegments.
     * When a work segment is finished, it is given to the archiver. If the absolute index of last archived segment
     * is denoted by A and the absolute index of next segment we want to write is denoted by W, then we can allow
     * write to S(W) if W - A <= walSegments. <br>
     *
     * Monitor of current object is used for notify on:
     * <ul>
     * <li>exception occurred ({@link FileArchiver#cleanException}!=null)</li>
     * <li>stopping thread ({@link FileArchiver#stopped}==true)</li>
     * <li>current file index changed ({@link FileArchiver#curAbsWalIdx})</li>
     * <li>last archived file index was changed ({@link FileArchiver#lastAbsArchivedIdx})</li>
     * <li>some WAL index was removed from {@link FileArchiver#locked} map</li>
     * </ul>
     */
    private class FileArchiver extends Thread {
        /** Exception which occurred during initial creation of files or during archiving WAL segment */
        private IgniteCheckedException cleanException;

        /**
         * Absolute current segment index WAL Manger writes to. Guarded by <code>this</code>.
         * Incremented during rollover. Also may be directly set if WAL is resuming logging after start.
         */
        private long curAbsWalIdx = -1;

        /** Last archived file index (absolute, 0-based). Guarded by <code>this</code>. */
        private long lastAbsArchivedIdx = -1;

        /** current thread stopping advice */
        private volatile boolean stopped;

        /** */
        private NavigableMap<Long, Integer> reserved = new TreeMap<>();

        /**
         * Maps absolute segment index to locks counter. Lock on segment protects from archiving segment and may
         * come from {@link RecordsIterator} during WAL replay. Map itself is guarded by <code>this</code>.
         */
        private Map<Long, Integer> locked = new HashMap<>();

        /**
         *
         */
        private FileArchiver(long lastAbsArchivedIdx) {
            super("wal-file-archiver%" + cctx.igniteInstanceName());

            this.lastAbsArchivedIdx = lastAbsArchivedIdx;
        }

        /**
         * @return Last archived segment absolute index.
         */
        private synchronized long lastArchivedAbsoluteIndex() {
            return lastAbsArchivedIdx;
        }

        /**
         * @throws IgniteInterruptedCheckedException If failed to wait for thread shutdown.
         */
        private void shutdown() throws IgniteInterruptedCheckedException {
            synchronized (this) {
                stopped = true;

                notifyAll();
            }

            U.join(this);
        }

        /**
         * @param curAbsWalIdx Current absolute WAL segment index.
         */
        private void currentWalIndex(long curAbsWalIdx) {
            synchronized (this) {
                this.curAbsWalIdx = curAbsWalIdx;

                notifyAll();
            }
        }

        /**
         * @param absIdx Index for reservation.
         */
        private synchronized void reserve(long absIdx) {
            Integer cur = reserved.get(absIdx);

            if (cur == null)
                reserved.put(absIdx, 1);
            else
                reserved.put(absIdx, cur + 1);
        }

        /**
         * Check if WAL segment locked or reserved
         *
         * @param absIdx Index for check reservation.
         * @return {@code True} if index is reserved.
         */
        private synchronized boolean reserved(long absIdx) {
            return locked.containsKey(absIdx) || reserved.floorKey(absIdx) != null;
        }

        /**
         * @param absIdx Reserved index.
         */
        private synchronized void release(long absIdx) {
            Integer cur = reserved.get(absIdx);

            assert cur != null && cur >= 1 : cur;

            if (cur == 1)
                reserved.remove(absIdx);
            else
                reserved.put(absIdx, cur - 1);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                allocateRemainingFiles();
            }
            catch (IgniteCheckedException e) {
                synchronized (this) {
                    // Stop the thread and report to starter.
                    cleanException = e;

                    notifyAll();

                    return;
                }
            }

            try {
                synchronized (this) {
                    while (curAbsWalIdx == -1 && !stopped)
                        wait();

                    if (curAbsWalIdx != 0 && lastAbsArchivedIdx == -1)
                        lastAbsArchivedIdx = curAbsWalIdx - 1;
                }

                while (!Thread.currentThread().isInterrupted() && !stopped) {
                    long toArchive;

                    synchronized (this) {
                        assert lastAbsArchivedIdx <= curAbsWalIdx : "lastArchived=" + lastAbsArchivedIdx +
                            ", current=" + curAbsWalIdx;

                        while (lastAbsArchivedIdx >= curAbsWalIdx - 1 && !stopped)
                            wait();

                        toArchive = lastAbsArchivedIdx + 1;
                    }

                    if (stopped)
                        break;

                    try {
                        final SegmentArchiveResult res = archiveSegment(toArchive);

                        synchronized (this) {
                            while (locked.containsKey(toArchive) && !stopped)
                                wait();

                            // Firstly, format working file
                            if (!stopped)
                                formatFile(res.getOrigWorkFile());

                            // Then increase counter to allow rollover on clean working file
                            lastAbsArchivedIdx = toArchive;

                            notifyAll();
                        }
                        if (evt.isRecordable(EventType.EVT_WAL_SEGMENT_ARCHIVED))
                            evt.record(new WalSegmentArchivedEvent(cctx.discovery().localNode(),
                                res.getAbsIdx(), res.getDstArchiveFile()));
                    }
                    catch (IgniteCheckedException e) {
                        synchronized (this) {
                            cleanException = e;

                            notifyAll();
                        }
                    }
                }
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Gets the absolute index of the next WAL segment available to write.
         * Blocks till there are available file to write
         *
         * @param curIdx Current absolute index that we want to increment.
         * @return Next index (curWalSegmIdx+1) when it is ready to be written.
         * @throws IgniteCheckedException If failed (if interrupted or if exception occurred in the archiver thread).
         */
        private long nextAbsoluteSegmentIndex(long curIdx) throws IgniteCheckedException {
            try {
                synchronized (this) {
                    if (cleanException != null)
                        throw cleanException;

                    assert curIdx == curAbsWalIdx;

                    curAbsWalIdx++;

                    // Notify archiver thread.
                    notifyAll();

                    while (curAbsWalIdx - lastAbsArchivedIdx > psCfg.getWalSegments() && cleanException == null)
                        wait();

                    return curAbsWalIdx;
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedCheckedException(e);
            }
        }

        /**
         * @param absIdx Segment absolute index.
         * @return {@code True} if can read, {@code false} if work segment
         */
        @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
        private boolean checkCanReadArchiveOrReserveWorkSegment(long absIdx) {
            synchronized (this) {
                if (lastAbsArchivedIdx >= absIdx)
                    return true;

                Integer cur = locked.get(absIdx);

                cur = cur == null ? 1 : cur + 1;

                locked.put(absIdx, cur);

                if (log.isDebugEnabled())
                    log.debug("Reserved work segment [absIdx=" + absIdx + ", pins=" + cur + ']');

                return false;
            }
        }

        /**
         * @param absIdx Segment absolute index.
         */
        @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
        private void releaseWorkSegment(long absIdx) {
            synchronized (this) {
                Integer cur = locked.get(absIdx);

                assert cur != null && cur > 0;

                if (cur == 1) {
                    locked.remove(absIdx);

                    if (log.isDebugEnabled())
                        log.debug("Fully released work segment (ready to archive) [absIdx=" + absIdx + ']');
                }
                else {
                    locked.put(absIdx, cur - 1);

                    if (log.isDebugEnabled())
                        log.debug("Partially released work segment [absIdx=" + absIdx + ", pins=" + (cur - 1) + ']');
                }

                notifyAll();
            }
        }

        /**
         * Moves WAL segment from work folder to archive folder.
         * Temp file is used to do movement
         *
         * @param absIdx Absolute index to archive.
         */
        private SegmentArchiveResult archiveSegment(long absIdx) throws IgniteCheckedException {
            long segIdx = absIdx % psCfg.getWalSegments();

            File origFile = new File(walWorkDir, FileDescriptor.fileName(segIdx));

            String name = FileDescriptor.fileName(absIdx);

            File dstTmpFile = new File(walArchiveDir, name + ".tmp");

            File dstFile = new File(walArchiveDir, name);

            if (log.isDebugEnabled())
                log.debug("Starting to copy WAL segment [absIdx=" + absIdx + ", segIdx=" + segIdx +
                    ", origFile=" + origFile.getAbsolutePath() + ", dstFile=" + dstFile.getAbsolutePath() + ']');

            try {
                Files.deleteIfExists(dstTmpFile.toPath());

                Files.copy(origFile.toPath(), dstTmpFile.toPath());

                Files.move(dstTmpFile.toPath(), dstFile.toPath());

                if (mode == WALMode.DEFAULT) {
                    try (FileIO f0 = ioFactory.create(dstFile, CREATE, READ, WRITE)) {
                        f0.force();
                    }
                }
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to archive WAL segment [" +
                    "srcFile=" + origFile.getAbsolutePath() +
                    ", dstFile=" + dstTmpFile.getAbsolutePath() + ']', e);
            }

            if (log.isDebugEnabled())
                log.debug("Copied file [src=" + origFile.getAbsolutePath() +
                    ", dst=" + dstFile.getAbsolutePath() + ']');

            return new SegmentArchiveResult(absIdx, origFile, dstFile);
        }

        /**
         *
         */
        private boolean checkStop() {
            return stopped;
        }

        /**
         * Background creation of all segments except first. First segment was created in main thread by
         * {@link FileWriteAheadLogManager#checkOrPrepareFiles()}
         */
        private void allocateRemainingFiles() throws IgniteCheckedException {
            checkFiles(1, true, new IgnitePredicate<Integer>() {
                @Override public boolean apply(Integer integer) {
                    return !checkStop();
                }
            });
        }
    }

    /**
     * Validate files depending on {@link PersistentStoreConfiguration#getWalSegments()}  and create if need.
     * Check end when exit condition return false or all files are passed.
     *
     * @param startWith Start with.
     * @param create Flag create file.
     * @param p Predicate Exit condition.
     * @throws IgniteCheckedException if validation or create file fail.
     */
    private void checkFiles(int startWith, boolean create, IgnitePredicate<Integer> p) throws IgniteCheckedException {
        for (int i = startWith; i < psCfg.getWalSegments() && (p == null || (p != null && p.apply(i))); i++) {
            File checkFile = new File(walWorkDir, FileDescriptor.fileName(i));

            if (checkFile.exists()) {
                if (checkFile.isDirectory())
                    throw new IgniteCheckedException("Failed to initialize WAL log segment (a directory with " +
                        "the same name already exists): " + checkFile.getAbsolutePath());
                else if (checkFile.length() != psCfg.getWalSegmentSize() && mode == WALMode.DEFAULT)
                    throw new IgniteCheckedException("Failed to initialize WAL log segment " +
                        "(WAL segment size change is not supported):" + checkFile.getAbsolutePath());
            }
            else if (create)
                createFile(checkFile);
        }
    }

    /**
     * Reads record serializer version from provided {@code io}.
     * NOTE: Method mutates position of {@code io}.
     *
     * @param io I/O interface for file.
     * @return Serializer version stored in the file.
     * @throws IgniteCheckedException If failed to read serializer version.
     */
    public static int readSerializerVersion(FileIO io)
            throws IgniteCheckedException, IOException {
        try (ByteBufferExpander buf = new ByteBufferExpander(RecordV1Serializer.HEADER_RECORD_SIZE, ByteOrder.nativeOrder())) {
            FileInput in = new FileInput(io, buf);

            in.ensure(RecordV1Serializer.HEADER_RECORD_SIZE);

            int recordType = in.readUnsignedByte();

            if (recordType == WALRecord.RecordType.STOP_ITERATION_RECORD_TYPE)
                throw new SegmentEofException("Reached logical end of the segment", null);

            WALRecord.RecordType type = WALRecord.RecordType.fromOrdinal(recordType - 1);

            if (type != WALRecord.RecordType.HEADER_RECORD)
                throw new IOException("Can't read serializer version", null);

            // Read file pointer.
            FileWALPointer ptr = RecordV1Serializer.readPosition(in);

            assert ptr.fileOffset() == 0 : "Header record should be placed at the beginning of file " + ptr;

            long headerMagicNumber = in.readLong();

            if (headerMagicNumber != HeaderRecord.MAGIC)
                throw new IOException("Magic is corrupted [exp=" + U.hexLong(HeaderRecord.MAGIC) +
                        ", actual=" + U.hexLong(headerMagicNumber) + ']');

            // Read serializer version.
            int version = in.readInt();

            // Read and skip CRC.
            in.readInt();

            return version;
        }
    }

    /**
     * Writes record serializer version to provided {@code io}.
     * NOTE: Method mutates position of {@code io}.
     *
     * @param io I/O interface for file.
     * @param idx Segment index.
     * @param version Serializer version.
     * @return I/O position after write version.
     * @throws IOException If failed to write serializer version.
     */
    public static long writeSerializerVersion(FileIO io, long idx, int version) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(RecordV1Serializer.HEADER_RECORD_SIZE);
        buffer.order(ByteOrder.nativeOrder());

        // Write record type.
        buffer.put((byte) (WALRecord.RecordType.HEADER_RECORD.ordinal() + 1));

        // Write position.
        RecordV1Serializer.putPosition(buffer, new FileWALPointer(idx, 0, 0));

        // Place magic number.
        buffer.putLong(HeaderRecord.MAGIC);

        // Place serializer version.
        buffer.putInt(version);

        // Place CRC if needed.
        if (!RecordV1Serializer.SKIP_CRC) {
            int curPos = buffer.position();

            buffer.position(0);

            // This call will move buffer position to the end of the record again.
            int crcVal = PureJavaCrc32.calcCrc32(buffer, curPos);

            buffer.putInt(crcVal);
        }
        else
            buffer.putInt(0);

        // Write header record through io.
        buffer.position(0);

        do {
            io.write(buffer);
        }
        while (buffer.hasRemaining());

        // Flush
        io.force();

        return io.position();
    }

    /**
     * WAL file descriptor.
     */
    public static class FileDescriptor implements Comparable<FileDescriptor> {
        /** */
        protected final File file;

        /** Absolute WAL segment file index */
        protected final long idx;

        /**
         * Creates file descriptor. Index is restored from file name
         *
         * @param file WAL segment file.
         */
        public FileDescriptor(@NotNull File file) {
            this(file, null);
        }

        /**
         * @param file WAL segment file.
         * @param idx Absolute WAL segment file index. For null value index is restored from file name
         */
        public FileDescriptor(@NotNull File file, @Nullable Long idx) {
            this.file = file;

            String fileName = file.getName();

            assert fileName.endsWith(WAL_SEGMENT_FILE_EXT);

            int end = fileName.length() - WAL_SEGMENT_FILE_EXT.length();

            this.idx = idx == null ? Long.parseLong(fileName.substring(0, end)) : idx;
        }

        /**
         * @param segment Segment index.
         * @return Segment file name.
         */
        public static String fileName(long segment) {
            SB b = new SB();

            String segmentStr = Long.toString(segment);

            for (int i = segmentStr.length(); i < 16; i++)
                b.a('0');

            b.a(segmentStr).a(WAL_SEGMENT_FILE_EXT);

            return b.toString();
        }

        /**
         * @param segment Segment number as integer.
         * @return Segment number as aligned string.
         */
        private static String segmentNumber(long segment) {
            SB b = new SB();

            String segmentStr = Long.toString(segment);

            for (int i = segmentStr.length(); i < 16; i++)
                b.a('0');

            b.a(segmentStr);

            return b.toString();
        }

        /** {@inheritDoc} */
        @Override public int compareTo(FileDescriptor o) {
            return Long.compare(idx, o.idx);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof FileDescriptor))
                return false;

            FileDescriptor that = (FileDescriptor)o;

            return idx == that.idx;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int)(idx ^ (idx >>> 32));
        }

        /**
         * @return Absolute WAL segment file index
         */
        public long getIdx() {
            return idx;
        }

        /**
         * @return absolute pathname string of this file descriptor pathname.
         */
        public String getAbsolutePath() {
            return file.getAbsolutePath();
        }
    }

    /**
     *
     */
    private abstract static class FileHandle {
        /** I/O interface for read/write operations with file */
        protected FileIO fileIO;

        /** Absolute WAL segment file index (incremental counter) */
        protected final long idx;

        /** */
        protected String gridName;

        /**
         * @param fileIO I/O interface for read/write operations of FileHandle.
         * @param idx Absolute WAL segment file index (incremental counter).
         */
        private FileHandle(FileIO fileIO, long idx, String gridName) {
            this.fileIO = fileIO;
            this.idx = idx;
            this.gridName = gridName;
        }
    }

    /**
     *
     */
    public static class ReadFileHandle extends FileHandle {
        /** Entry serializer. */
        RecordSerializer ser;

        /** */
        FileInput in;

        /**
         * <code>true</code> if this file handle came from work directory.
         * <code>false</code> if this file handle came from archive directory.
         */
        private boolean workDir;

        /**
         * @param fileIO I/O interface for read/write operations of FileHandle.
         * @param idx Absolute WAL segment file index (incremental counter).
         * @param ser Entry serializer.
         * @param in File input.
         */
        ReadFileHandle(
                FileIO fileIO,
                long idx,
                String gridName,
                RecordSerializer ser,
                FileInput in
        ) {
            super(fileIO, idx, gridName);

            this.ser = ser;
            this.in = in;
        }

        /**
         * @throws IgniteCheckedException If failed to close the WAL segment file.
         */
        public void close() throws IgniteCheckedException {
            try {
                fileIO.close();
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
        }
    }

    /**
     * File handle for one log segment.
     */
    @SuppressWarnings("SignalWithoutCorrespondingAwait")
    private class FileWriteHandle extends FileHandle {
        /** */
        private final RecordSerializer serializer;

        /** See {@link FileWriteAheadLogManager#maxWalSegmentSize} */
        private final long maxSegmentSize;

        /**
         * Accumulated WAL records chain.
         * This reference points to latest WAL record.
         * When writing records chain is iterated from latest to oldest (see {@link WALRecord#previous()})
         * Records from chain are saved into buffer in reverse order
         */
        private final AtomicReference<WALRecord> head = new AtomicReference<>();

        /**
         * Position in current file after the end of last written record (incremented after file channel write
         * operation)
         */
        private volatile long written;

        /** */
        private volatile long lastFsyncPos;

        /** Stop guard to provide warranty that only one thread will be successful in calling {@link #close(boolean)}*/
        private final AtomicBoolean stop = new AtomicBoolean(false);

        /** */
        private final Lock lock = new ReentrantLock();

        /** Condition activated each time writeBuffer() completes. Used to wait previously flushed write to complete */
        private final Condition writeComplete = lock.newCondition();

        /** Condition for timed wait of several threads, see {@link PersistentStoreConfiguration#getWalFsyncDelayNanos()} */
        private final Condition fsync = lock.newCondition();

        /**
         * Next segment available condition.
         * Protection from "spurious wakeup" is provided by predicate {@link #fileIO}=<code>null</code>
         */
        private final Condition nextSegment = lock.newCondition();

        /**
         * @param fileIO I/O file interface to use
         * @param idx Absolute WAL segment file index for easy access.
         * @param pos Position.
         * @param maxSegmentSize Max segment size.
         * @param serializer Serializer.
         * @throws IOException If failed.
         */
        private FileWriteHandle(
            FileIO fileIO,
            long idx,
            String gridName,
            long pos,
            long maxSegmentSize,
            RecordSerializer serializer
        ) throws IOException {
            super(fileIO, idx, gridName);

            assert serializer != null;

            fileIO.position(pos);

            this.maxSegmentSize = maxSegmentSize;
            this.serializer = serializer;

            head.set(new FakeRecord(new FileWALPointer(idx, (int)pos, 0), false));
            written = pos;
            lastFsyncPos = pos;
        }

        /**
         * Write serializer version to current handle.
         * NOTE: Method mutates {@code fileIO} position, written and lastFsyncPos fields.
         *
         * @throws IgniteCheckedException If fail to write serializer version.
         */
        public void writeSerializerVersion() throws IgniteCheckedException {
            try {
                assert fileIO.position() == 0 : "Serializer version can be written only at the begin of file " + fileIO.position();

                long updatedPosition = FileWriteAheadLogManager.writeSerializerVersion(fileIO, idx, serializer.version());

                written = updatedPosition;
                lastFsyncPos = updatedPosition;
                head.set(new FakeRecord(new FileWALPointer(idx, (int)updatedPosition, 0), false));
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Unable to write serializer version for segment " + idx, e);
            }
        }

        /**
         * Checks if current head is a close fake record and returns {@code true} if so.
         *
         * @return {@code true} if current head is close record.
         */
        private boolean stopped() {
            return stopped(head.get());
        }

        /**
         * @param record Record to check.
         * @return {@code true} if the record is fake close record.
         */
        private boolean stopped(WALRecord record) {
            return record instanceof FakeRecord && ((FakeRecord)record).stop;
        }

        /**
         * @param rec Record to be added to record chain as new {@link #head}
         * @return Pointer or null if roll over to next segment is required or already started by other thread.
         * @throws StorageException If failed.
         * @throws IgniteCheckedException If failed.
         */
        @Nullable private WALPointer addRecord(WALRecord rec) throws StorageException, IgniteCheckedException {
            assert rec.size() > 0 || rec.getClass() == FakeRecord.class;

            boolean flushed = false;

            for (; ; ) {
                WALRecord h = head.get();

                long nextPos = nextPosition(h);

                if (nextPos + rec.size() >= maxSegmentSize || stopped(h)) {
                    // Can not write to this segment, need to switch to the next one.
                    return null;
                }

                int newChainSize = h.chainSize() + rec.size();

                if (newChainSize > tlbSize && !flushed) {
                    boolean res = h.previous() == null || flush(h, false);

                    if (rec.size() > tlbSize)
                        flushed = res;

                    continue;
                }

                rec.chainSize(newChainSize);
                rec.previous(h);

                FileWALPointer ptr = new FileWALPointer(
                    idx,
                    (int)nextPos,
                    rec.size(),
                    // We need to force checkpoint records into file in BACKGROUND WALMode.
                    mode == WALMode.BACKGROUND && rec instanceof CheckpointRecord);

                rec.position(ptr);

                if (head.compareAndSet(h, rec))
                    return ptr;
            }
        }

        /**
         * @param rec Record.
         * @return Position for the next record.
         */
        private long nextPosition(WALRecord rec) {
            return recordOffset(rec) + rec.size();
        }

        /**
         * Flush or wait for concurrent flush completion.
         *
         * @param ptr Pointer.
         * @throws IgniteCheckedException If failed.
         */
        private void flushOrWait(FileWALPointer ptr, boolean stop) throws IgniteCheckedException {
            long expWritten;

            if (ptr != null) {
                // If requested obsolete file index, it must be already flushed by close.
                if (ptr.index() != idx)
                    return;

                expWritten = ptr.fileOffset();
            }
            else // We read head position before the flush because otherwise we can get wrong position.
                expWritten = recordOffset(head.get());

            if (flush(ptr, stop))
                return;
            else if (stop) {
                FakeRecord fr = (FakeRecord)head.get();

                assert fr.stop : "Invalid fake record on top of the queue: " + fr;

                expWritten = recordOffset(fr);
            }

            // Spin-wait for a while before acquiring the lock.
            for (int i = 0; i < 64; i++) {
                if (written >= expWritten)
                    return;
            }

            // If we did not flush ourselves then await for concurrent flush to complete.
            lock.lock();

            try {
                while (written < expWritten && envFailed == null)
                    U.awaitQuiet(writeComplete);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param ptr Pointer.
         * @return {@code true} If the flush really happened.
         * @throws IgniteCheckedException If failed.
         * @throws StorageException If failed.
         */
        private boolean flush(FileWALPointer ptr, boolean stop) throws IgniteCheckedException, StorageException {
            if (ptr == null) { // Unconditional flush.
                for (; ; ) {
                    WALRecord expHead = head.get();

                    if (expHead.previous() == null) {
                        FakeRecord frHead = (FakeRecord)expHead;

                        if (frHead.stop == stop || frHead.stop ||
                            head.compareAndSet(expHead, new FakeRecord(frHead.position(), stop)))
                            return false;
                    }

                    if (flush(expHead, stop))
                        return true;
                }
            }

            assert ptr.index() == idx;

            for (; ; ) {
                WALRecord h = head.get();

                // If current chain begin position is greater than requested, then someone else flushed our changes.
                if (chainBeginPosition(h) > ptr.fileOffset())
                    return false;

                if (flush(h, stop))
                    return true; // We are lucky.
            }
        }

        /**
         * @param h Head of the chain.
         * @return Chain begin position.
         */
        private long chainBeginPosition(WALRecord h) {
            return recordOffset(h) + h.size() - h.chainSize();
        }

        /**
         * @param expHead Expected head of chain. If head was changed, flush is not performed in this thread
         * @throws IgniteCheckedException If failed.
         * @throws StorageException If failed.
         */
        private boolean flush(WALRecord expHead, boolean stop) throws StorageException, IgniteCheckedException {
            if (expHead.previous() == null) {
                FakeRecord frHead = (FakeRecord)expHead;

                if (!stop || frHead.stop) // Protects from CASing terminal FakeRecord(true) to FakeRecord(false)
                    return false;
            }

            // Fail-fast before CAS.
            checkEnvironment();

            if (!head.compareAndSet(expHead, new FakeRecord(new FileWALPointer(idx, (int)nextPosition(expHead), 0), stop)))
                return false;

            if (expHead.chainSize() == 0)
                return false;

            // At this point we grabbed the piece of WAL chain.
            // Any failure in this code must invalidate the environment.
            try {
                // We can safely allow other threads to start building next chains while we are doing flush here.
                ByteBuffer buf;

                boolean tmpBuf = false;

                if (expHead.chainSize() > tlbSize) {
                    buf = GridUnsafe.allocateBuffer(expHead.chainSize());

                    tmpBuf = true; // We need to manually release this temporary direct buffer.
                }
                else
                    buf = tlb.get();

                try {
                    long pos = fillBuffer(buf, expHead);

                    writeBuffer(pos, buf);
                }
                finally {
                    if (tmpBuf)
                        GridUnsafe.freeBuffer(buf);
                }

                return true;
            }
            catch (Throwable e) {
                invalidateEnvironment(e);

                // All workers waiting for a next segment must be woken up and stopped
                signalNextAvailable();

                throw e;
            }
        }

        /**
         * Serializes WAL records chain to provided byte buffer
         *
         * @param buf Buffer, will be filled with records chain from end to beginning
         * @param head Head of the chain to write to the buffer.
         * @return Position in file for this buffer.
         * @throws IgniteCheckedException If failed.
         */
        private long fillBuffer(ByteBuffer buf, WALRecord head) throws IgniteCheckedException {
            final int limit = head.chainSize();

            assert limit <= buf.capacity();

            buf.rewind();
            buf.limit(limit);

            do {
                buf.position(head.chainSize() - head.size());
                buf.limit(head.chainSize()); // Just to make sure that serializer works in bounds.

                try {
                    serializer.writeRecord(head, buf);
                }
                catch (RuntimeException e) {
                    throw new IllegalStateException("Failed to write record: " + head, e);
                }

                assert !buf.hasRemaining() : "Reported record size is greater than actual: " + head;

                head = head.previous();
            }
            while (head.previous() != null);

            assert head instanceof FakeRecord : head.getClass();

            buf.rewind();
            buf.limit(limit);

            return recordOffset(head);
        }

        /**
         * Non-blocking check if this pointer needs to be sync'ed.
         *
         * @param ptr WAL pointer to check.
         * @return {@code False} if this pointer has been already sync'ed.
         */
        private boolean needFsync(FileWALPointer ptr) {
            // If index has changed, it means that the log was rolled over and already sync'ed.
            // If requested position is smaller than last sync'ed, it also means all is good.
            // If position is equal, then our record is the last not synced.
            return idx == ptr.index() && lastFsyncPos <= ptr.fileOffset();
        }

        /**
         * @return Pointer to the end of the last written record (probably not fsync-ed).
         */
        private FileWALPointer position() {
            lock.lock();

            try {
                return new FileWALPointer(idx, (int)written, 0);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param ptr Pointer to sync.
         * @throws StorageException If failed.
         */
        private void fsync(FileWALPointer ptr, boolean stop) throws StorageException, IgniteCheckedException {
            lock.lock();

            try {
                if (ptr != null) {
                    if (!needFsync(ptr))
                        return;

                    if (fsyncDelay > 0 && !stopped()) {
                        // Delay fsync to collect as many updates as possible: trade latency for throughput.
                        U.await(fsync, fsyncDelay, TimeUnit.NANOSECONDS);

                        if (!needFsync(ptr))
                            return;
                    }
                }

                flushOrWait(ptr, stop);

                if (stopped())
                    return;

                if (lastFsyncPos != written) {
                    assert lastFsyncPos < written; // Fsync position must be behind.

                    boolean metricsEnabled = metrics.metricsEnabled();

                    long start = metricsEnabled ? System.nanoTime() : 0;

                    try {
                        fileIO.force();
                    }
                    catch (IOException e) {
                        throw new StorageException(e);
                    }

                    lastFsyncPos = written;

                    if (fsyncDelay > 0)
                        fsync.signalAll();

                    long end = metricsEnabled ? System.nanoTime() : 0;

                    if (metricsEnabled)
                        metrics.onFsync(end - start);
                }
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @return {@code true} If this thread actually closed the segment.
         * @throws IgniteCheckedException If failed.
         * @throws StorageException If failed.
         */
        private boolean close(boolean rollOver) throws IgniteCheckedException, StorageException {
            if (stop.compareAndSet(false, true)) {
                lock.lock();

                try {
                    flushOrWait(null, true);

                    assert stopped() : "Segment is not closed after close flush: " + head.get();

                    try {
                        int switchSegmentRecSize = RecordV1Serializer.REC_TYPE_SIZE + RecordV1Serializer.FILE_WAL_POINTER_SIZE + RecordV1Serializer.CRC_SIZE;

                    if (rollOver && written < (maxSegmentSize - switchSegmentRecSize)) {
                        RecordV1Serializer backwardSerializer =
                            new RecordV1Serializer(new RecordDataV1Serializer(cctx), true);

                        final ByteBuffer buf = ByteBuffer.allocate(switchSegmentRecSize);

                        SwitchSegmentRecord segmentRecord = new SwitchSegmentRecord();
                        segmentRecord.position( new FileWALPointer(idx, (int)written, -1));
                        backwardSerializer.writeRecord(segmentRecord,buf);

                            buf.rewind();

                            int rem = buf.remaining();

                            while (rem > 0) {
                                int written0 = fileIO.write(buf, written);

                                written += written0;

                                rem -= written0;
                            }
                        }

                        // Do the final fsync.
                        if (mode == WALMode.DEFAULT) {
                            fileIO.force();

                            lastFsyncPos = written;
                        }

                        fileIO.close();
                    }
                    catch (IOException e) {
                        throw new IgniteCheckedException(e);
                    }

                    if (log.isDebugEnabled())
                        log.debug("Closed WAL write handle [idx=" + idx + "]");

                    return true;
                }
                finally {
                    lock.unlock();
                }
            }
            else
                return false;
        }

        /**
         * Signals next segment available to wake up other worker threads waiting for WAL to write
         */
        private void signalNextAvailable() {
            lock.lock();

            try {
                WALRecord rec = head.get();

                if (envFailed == null) {
                    assert rec instanceof FakeRecord : "Expected head FakeRecord, actual head "
                    + (rec != null ? rec.getClass().getSimpleName() : "null");

                    assert written == lastFsyncPos || mode != WALMode.DEFAULT :
                    "fsync [written=" + written + ", lastFsync=" + lastFsyncPos + ']';
                }

                fileIO = null;

                nextSegment.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void awaitNext() throws IgniteCheckedException {
            lock.lock();

            try {
                while (fileIO != null)
                    U.awaitQuiet(nextSegment);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param pos Position in file to start write from. May be checked against actual position to wait previous
         * writes to complete
         * @param buf Buffer to write to file
         * @throws StorageException If failed.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("TooBroadScope")
        private void writeBuffer(long pos, ByteBuffer buf) throws StorageException, IgniteCheckedException {
            boolean interrupted = false;

            lock.lock();

            try {
                assert fileIO != null : "Writing to a closed segment.";

                checkEnvironment();

                long lastLogged = U.currentTimeMillis();

                long logBackoff = 2_000;

                // If we were too fast, need to wait previous writes to complete.
                while (written != pos) {
                    assert written < pos : "written = " + written + ", pos = " + pos; // No one can write further than we are now.

                    // Permutation occurred between blocks write operations.
                    // Order of acquiring lock is not the same as order of write.
                    long now = U.currentTimeMillis();

                    if (now - lastLogged >= logBackoff) {
                        if (logBackoff < 60 * 60_000)
                            logBackoff *= 2;

                        U.warn(log, "Still waiting for a concurrent write to complete [written=" + written +
                            ", pos=" + pos + ", lastFsyncPos=" + lastFsyncPos + ", stop=" + stop.get() +
                            ", actualPos=" + safePosition() + ']');

                        lastLogged = now;
                    }

                    try {
                        writeComplete.await(2, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException ignore) {
                        interrupted = true;
                    }

                    checkEnvironment();
                }

                // Do the write.
                int size = buf.remaining();

                assert size > 0 : size;

                try {
                    assert written == fileIO.position();

                    do {
                        fileIO.write(buf);
                    }
                    while (buf.hasRemaining());

                    written += size;

                    metrics.onWalBytesWritten(size);

                    assert written == fileIO.position();
                }
                catch (IOException e) {
                    invalidateEnvironmentLocked(e);

                    throw new StorageException(e);
                }
            }
            finally {
                writeComplete.signalAll();

                lock.unlock();

                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        }

        /**
         * @param e Exception to set as a cause for all further operations.
         */
        private void invalidateEnvironment(Throwable e) {
            lock.lock();

            try {
                invalidateEnvironmentLocked(e);
            }
            finally {
                writeComplete.signalAll();

                lock.unlock();
            }
        }

        /**
         * @param e Exception to set as a cause for all further operations.
         */
        private void invalidateEnvironmentLocked(Throwable e) {
            if (envFailed == null) {
                envFailed = e;

                U.error(log, "IO error encountered while running WAL flush. All further operations will be failed and " +
                    "local node will be stopped.", e);

                new Thread() {
                    @Override public void run() {
                        G.stop(gridName, true);
                    }
                }.start();
            }
        }

        /**
         * @return Safely reads current position of the file channel as String. Will return "null" if channel is null.
         */
        private String safePosition() {
            FileIO io = this.fileIO;

            if (io == null)
                return "null";

            try {
                return String.valueOf(io.position());
            }
            catch (IOException e) {
                return "{Failed to read channel position: " + e.getMessage() + "}";
            }
        }
    }

    /**
     * Gets WAL record offset relative to the WAL segment file beginning.
     *
     * @param rec WAL record.
     * @return File offset.
     */
    private static int recordOffset(WALRecord rec) {
        FileWALPointer ptr = (FileWALPointer)rec.position();

        assert ptr != null;

        return ptr.fileOffset();
    }

    /**
     * Fake record is zero-sized record, which is not stored into file.
     * Fake record is used for storing position in file {@link WALRecord#position()}.
     * Fake record is allowed to have no previous record.
     */
    private static final class FakeRecord extends WALRecord {
        /** */
        private final boolean stop;

        /**
         * @param pos Position.
         */
        FakeRecord(FileWALPointer pos, boolean stop) {
            position(pos);

            this.stop = stop;
        }

        /** {@inheritDoc} */
        @Override public RecordType type() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public FileWALPointer position() {
            return (FileWALPointer) super.position();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(FakeRecord.class, this, "super", super.toString());
        }
    }

    /**
     * Iterator over WAL-log.
     */
    private static class RecordsIterator extends AbstractWalRecordsIterator {
        /** */
        private static final long serialVersionUID = 0L;
        /** */
        private final File walWorkDir;

        /** */
        private final File walArchiveDir;

        /** */
        private final FileArchiver archiver;

        /** */
        private final PersistentStoreConfiguration psCfg;

        /** Optional start pointer. */
        @Nullable
        private FileWALPointer start;

        /** Optional end pointer. */
        @Nullable
        private FileWALPointer end;

        /**
         * @param cctx Shared context.
         * @param walWorkDir WAL work dir.
         * @param walArchiveDir WAL archive dir.
         * @param start Optional start pointer.
         * @param end Optional end pointer.
         * @param psCfg Database configuration.
         * @param serializer Serializer of current version to read headers.
         * @param archiver Archiver.
         * @param log Logger
         * @throws IgniteCheckedException If failed to initialize WAL segment.
         */
        private RecordsIterator(
            GridCacheSharedContext cctx,
            File walWorkDir,
            File walArchiveDir,
            @Nullable FileWALPointer start,
            @Nullable FileWALPointer end,
            PersistentStoreConfiguration psCfg,
            @NotNull RecordSerializer serializer,
            FileIOFactory ioFactory,
            FileArchiver archiver,
            IgniteLogger log,
            int tlbSize
        ) throws IgniteCheckedException {
            super(log,
                cctx,
                serializer,
                ioFactory,
                psCfg.getWalRecordIteratorBufferSize());
            this.walWorkDir = walWorkDir;
            this.walArchiveDir = walArchiveDir;
            this.psCfg = psCfg;
            this.archiver = archiver;
            this.start = start;
            this.end = end;

            init();

            advance();
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            super.onClose();

            curRec = null;

            final ReadFileHandle handle = closeCurrentWalSegment();

            if (handle != null && handle.workDir)
                releaseWorkSegment(curWalSegmIdx);

            curWalSegmIdx = Integer.MAX_VALUE;
        }

        /**
         * @throws IgniteCheckedException If failed to initialize first file handle.
         */
        private void init() throws IgniteCheckedException {
            FileDescriptor[] descs = loadFileDescriptors(walArchiveDir);

            if (start != null) {
                if (!F.isEmpty(descs)) {
                    if (descs[0].idx > start.index())
                        throw new IgniteCheckedException("WAL history is too short " +
                            "[descs=" + Arrays.asList(descs) + ", start=" + start + ']');

                    for (FileDescriptor desc : descs) {
                        if (desc.idx == start.index()) {
                            curWalSegmIdx = start.index();

                            break;
                        }
                    }

                    if (curWalSegmIdx == -1) {
                        long lastArchived = descs[descs.length - 1].idx;

                        if (lastArchived > start.index())
                            throw new IgniteCheckedException("WAL history is corrupted (segment is missing): " + start);

                        // This pointer may be in work files because archiver did not
                        // copy the file yet, check that it is not too far forward.
                        curWalSegmIdx = start.index();
                    }
                }
                else {
                    // This means that whole checkpoint history fits in one segment in WAL work directory.
                    // Will start from this index right away.
                    curWalSegmIdx = start.index();
                }
            }
            else
                curWalSegmIdx = !F.isEmpty(descs) ? descs[0].idx : 0;

            curWalSegmIdx--;

            if (log.isDebugEnabled())
                log.debug("Initialized WAL cursor [start=" + start + ", end=" + end + ", curWalSegmIdx=" + curWalSegmIdx + ']');
        }

        /** {@inheritDoc} */
        @Override protected ReadFileHandle advanceSegment(
            @Nullable final ReadFileHandle curWalSegment) throws IgniteCheckedException {
            if (curWalSegment != null) {
                curWalSegment.close();

                if (curWalSegment.workDir)
                    releaseWorkSegment(curWalSegment.idx);

            }

            // We are past the end marker.
            if (end != null && curWalSegmIdx + 1 > end.index())
                return null; //stop iteration

            curWalSegmIdx++;

            FileDescriptor fd;

            boolean readArchive = canReadArchiveOrReserveWork(curWalSegmIdx);

            if (readArchive) {
                fd = new FileDescriptor(new File(walArchiveDir,
                    FileDescriptor.fileName(curWalSegmIdx)));
            }
            else {
                long workIdx = curWalSegmIdx % psCfg.getWalSegments();

                fd = new FileDescriptor(
                    new File(walWorkDir, FileDescriptor.fileName(workIdx)),
                    curWalSegmIdx);
            }

            if (log.isDebugEnabled())
                log.debug("Reading next file [absIdx=" + curWalSegmIdx + ", file=" + fd.file.getAbsolutePath() + ']');

            assert fd != null;

            ReadFileHandle nextHandle;
            try {
                nextHandle = initReadHandle(fd, start != null && curWalSegmIdx == start.index() ? start : null);
            }
            catch (FileNotFoundException e) {
                if (readArchive)
                    throw new IgniteCheckedException("Missing WAL segment in the archive", e);
                else
                    nextHandle = null;
            }

            if (nextHandle != null)
                nextHandle.workDir = !readArchive;
            else
                releaseWorkSegment(curWalSegmIdx);

            curRec = null;
            return nextHandle;
        }

        /**
         * @param absIdx Absolute index to check.
         * @return {@code True} if we can safely read the archive, {@code false} if the segment has not been archived
         * yet. In this case the corresponding work segment is reserved (will not be deleted until release).
         */
        private boolean canReadArchiveOrReserveWork(long absIdx) {
            return archiver != null && archiver.checkCanReadArchiveOrReserveWorkSegment(absIdx);
        }

        /**
         * @param absIdx Absolute index to release.
         */
        private void releaseWorkSegment(long absIdx) {
            if (archiver != null)
                archiver.releaseWorkSegment(absIdx);
        }
    }

    /**
     * Flushes current file handle for {@link WALMode#BACKGROUND} WALMode.
     * Called periodically from scheduler.
     */
    private void doFlush() {
        final FileWriteHandle hnd = currentHandle();
        try {
            hnd.flush(hnd.head.get(), false);
        }
        catch (Exception e) {
            U.warn(log, "Failed to flush WAL record queue", e);
        }
    }
}
