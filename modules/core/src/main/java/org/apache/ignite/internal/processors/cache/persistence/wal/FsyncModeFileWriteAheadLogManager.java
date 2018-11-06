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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.SwitchSegmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.WalStateManager.WALDisableContext;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SERIALIZER_VERSION;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readSegmentHeader;

/**
 * File WAL manager.
 */
public class FsyncModeFileWriteAheadLogManager extends GridCacheSharedManagerAdapter implements IgniteWriteAheadLogManager {
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

    /** */
    private static final Pattern WAL_SEGMENT_FILE_COMPACTED_PATTERN = Pattern.compile("\\d{16}\\.wal\\.zip");

    /** WAL segment file filter, see {@link #WAL_NAME_PATTERN} */
    public static final FileFilter WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File file) {
            return !file.isDirectory() && (WAL_NAME_PATTERN.matcher(file.getName()).matches() ||
                WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches());
        }
    };

    /** */
    private static final Pattern WAL_SEGMENT_TEMP_FILE_COMPACTED_PATTERN = Pattern.compile("\\d{16}\\.wal\\.zip\\.tmp");

    /** */
    private static final FileFilter WAL_SEGMENT_FILE_COMPACTED_FILTER = new FileFilter() {
        @Override public boolean accept(File file) {
            return !file.isDirectory() && WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches();
        }
    };

    /** */
    private static final FileFilter WAL_SEGMENT_TEMP_FILE_COMPACTED_FILTER = new FileFilter() {
        @Override public boolean accept(File file) {
            return !file.isDirectory() && WAL_SEGMENT_TEMP_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches();
        }
    };

    /** Latest serializer version to use. */
    private static final int LATEST_SERIALIZER_VERSION = 2;

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
    private final DataStorageConfiguration dsCfg;

    /** Events service */
    private final GridEventStorageManager evt;

    /** */
    private IgniteConfiguration igCfg;

    /** Persistence metrics tracker. */
    private DataStorageMetricsImpl metrics;

    /** */
    private File walWorkDir;

    /** WAL archive directory (including consistent ID as subfolder) */
    private File walArchiveDir;

    /** Serializer of latest version, used to read header record and for write records */
    private RecordSerializer serializer;

    /** Serializer latest version to use. */
    private final int serializerVersion =
        IgniteSystemProperties.getInteger(IGNITE_WAL_SERIALIZER_VERSION, LATEST_SERIALIZER_VERSION);

    /** Latest segment cleared by {@link #truncate(WALPointer, WALPointer)}. */
    private volatile long lastTruncatedArchiveIdx = -1L;

    /** Factory to provide I/O interfaces for read/write operations with files */
    private volatile FileIOFactory ioFactory;

    /** Updater for {@link #currentHnd}, used for verify there are no concurrent update for current log segment handle */
    private static final AtomicReferenceFieldUpdater<FsyncModeFileWriteAheadLogManager, FileWriteHandle> currentHndUpd =
        AtomicReferenceFieldUpdater.newUpdater(FsyncModeFileWriteAheadLogManager.class, FileWriteHandle.class, "currentHnd");

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

    /** Compressor. */
    private volatile FileCompressor compressor;

    /** Decompressor. */
    private volatile FileDecompressor decompressor;

    /** */
    private final ThreadLocal<WALPointer> lastWALPtr = new ThreadLocal<>();

    /** Current log segment handle */
    private volatile FileWriteHandle currentHnd;

    /** */
    private volatile WALDisableContext walDisableContext;

    /**
     * Positive (non-0) value indicates WAL can be archived even if not complete<br>
     * See {@link DataStorageConfiguration#setWalAutoArchiveAfterInactivity(long)}<br>
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
    public FsyncModeFileWriteAheadLogManager(@NotNull final GridKernalContext ctx) {
        igCfg = ctx.config();

        DataStorageConfiguration dsCfg = igCfg.getDataStorageConfiguration();

        assert dsCfg != null;

        this.dsCfg = dsCfg;

        maxWalSegmentSize = dsCfg.getWalSegmentSize();
        mode = dsCfg.getWalMode();
        tlbSize = dsCfg.getWalThreadLocalBufferSize();
        flushFreq = dsCfg.getWalFlushFrequency();
        fsyncDelay = dsCfg.getWalFsyncDelayNanos();
        alwaysWriteFullPages = dsCfg.isAlwaysWriteFullPages();
        ioFactory = dsCfg.getFileIOFactory();
        walAutoArchiveAfterInactivity = dsCfg.getWalAutoArchiveAfterInactivity();
        evt = ctx.event();

        assert mode == WALMode.FSYNC : dsCfg;
    }

    /**
     * For test purposes only.
     *
     * @param ioFactory IO factory.
     */
    public void setFileIOFactory(FileIOFactory ioFactory) {
        this.ioFactory = ioFactory;
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        if (!cctx.kernalContext().clientNode()) {
            final PdsFolderSettings resolveFolders = cctx.kernalContext().pdsFolderResolver().resolveFolders();

            checkWalConfiguration();

            final File walWorkDir0 = walWorkDir = initDirectory(
                dsCfg.getWalPath(),
                DataStorageConfiguration.DFLT_WAL_PATH,
                resolveFolders.folderName(),
                "write ahead log work directory"
            );

            final File walArchiveDir0 = walArchiveDir = initDirectory(
                dsCfg.getWalArchivePath(),
                DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH,
                resolveFolders.folderName(),
                "write ahead log archive directory"
            );

            serializer = new RecordSerializerFactoryImpl(cctx).createSerializer(serializerVersion);

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

            metrics = dbMgr.persistentStoreMetricsImpl();

            checkOrPrepareFiles();

            metrics.setWalSizeProvider(new CO<Long>() {
                @Override public Long apply() {
                    long size = 0;

                    for (File f : walWorkDir0.listFiles())
                        size += f.length();

                    for (File f : walArchiveDir0.listFiles())
                        size += f.length();

                    return size;
                }
            });

            IgniteBiTuple<Long, Long> tup = scanMinMaxArchiveIndices();

            lastTruncatedArchiveIdx = tup == null ? -1 : tup.get1() - 1;

            archiver = new FileArchiver(tup == null ? -1 : tup.get2(), log);

            if (dsCfg.isWalCompactionEnabled()) {
                compressor = new FileCompressor();

                if (decompressor == null) {  // Preventing of two file-decompressor thread instantiations.
                    decompressor = new FileDecompressor(log);

                    new IgniteThread(decompressor).start();
                }
            }

            walDisableContext = cctx.walState().walDisableContext();

            if (mode != WALMode.NONE) {
                if (log.isInfoEnabled())
                    log.info("Started write-ahead log manager [mode=" + mode + ']');
            }
            else
                U.quietAndWarn(log, "Started write-ahead log manager in NONE mode, persisted data may be lost in " +
                    "a case of unexpected node failure. Make sure to deactivate the cluster before shutdown.");
        }
    }

    /**
     * @throws IgniteCheckedException if WAL store path is configured and archive path isn't (or vice versa)
     */
    private void checkWalConfiguration() throws IgniteCheckedException {
        if (dsCfg.getWalPath() == null ^ dsCfg.getWalArchivePath() == null) {
            throw new IgniteCheckedException(
                "Properties should be either both specified or both null " +
                    "[walStorePath = " + dsCfg.getWalPath() +
                    ", walArchivePath = " + dsCfg.getWalArchivePath() + "]"
            );
        }
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

            if (compressor != null)
                compressor.shutdown();

            if (decompressor != null)
                decompressor.shutdown();
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
            if (isArchiverEnabled()) {
                assert archiver != null;

                new IgniteThread(archiver).start();
            }

            if (compressor != null)
                compressor.start();
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
        return mode == WALMode.FSYNC;
    }

    /** {@inheritDoc} */
    @Override public void resumeLogging(WALPointer lastPtr) throws IgniteCheckedException {
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
     * Archiver can be not created, all files will be written to WAL folder, using absolute segment index.
     *
     * @return flag indicating if archiver is disabled.
     */
    private boolean isArchiverEnabled() {
        if (walArchiveDir != null && walWorkDir != null)
            return !walArchiveDir.equals(walWorkDir);

        return !new File(dsCfg.getWalArchivePath()).equals(new File(dsCfg.getWalPath()));
    }

    /**
     *  Collect wal segment files from low pointer (include) to high pointer (not include) and reserve low pointer.
     *
     * @param low Low bound.
     * @param high High bound.
     */
    public Collection<File> getAndReserveWalFiles(FileWALPointer low, FileWALPointer high) throws IgniteCheckedException {
        final long awaitIdx = high.index() - 1;

        while (archiver.lastArchivedAbsoluteIndex() < awaitIdx)
            LockSupport.parkNanos(Thread.currentThread(), 1_000_000);

        if (!reserve(low))
            throw new IgniteCheckedException("WAL archive segment has been deleted [idx=" + low.index() + "]");

        List<File> res = new ArrayList<>();

        for (long i = low.index(); i < high.index(); i++) {
            String segmentName = FileWriteAheadLogManager.FileDescriptor.fileName(i);

            File file = new File(walArchiveDir, segmentName);
            File fileZip = new File(walArchiveDir, segmentName + ".zip");

            if (file.exists())
                res.add(file);
            else if (fileZip.exists())
                res.add(fileZip);
            else {
                if (log.isInfoEnabled()) {
                    log.info("Segment not found: " + file.getName() + "/" + fileZip.getName());

                    log.info("Stopped iteration on idx: " + i);
                }

                break;
            }
        }

        return res;
    }

    /** {@inheritDoc}*/
    @Override public int serializerVersion() {
        return serializerVersion;
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

            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, e));
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("TooBroadScope")
    @Override public WALPointer log(WALRecord record) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == WALMode.NONE)
            return null;

        FileWriteHandle currWrHandle = currentHandle();

        WALDisableContext isDisable = walDisableContext;

        // Logging was not resumed yet.
        if (currWrHandle == null || (isDisable != null && isDisable.check()))
            return null;

        // Need to calculate record size first.
        record.size(serializer.size(record));

        while (true) {
            if (record.rollOver()){
                assert cctx.database().checkpointLockIsHeldByThread();

                currWrHandle = rollOver(currWrHandle);
            }

            WALPointer ptr = currWrHandle.addRecord(record);

            if (ptr != null) {
                metrics.onWalRecordLogged();

                lastWALPtr.set(ptr);

                if (walAutoArchiveAfterInactivity > 0)
                    lastRecordLoggedMs.set(U.currentTimeMillis());

                return ptr;
            }
            else
                currWrHandle = rollOver(currWrHandle);

            checkNode();

            if (isStopping())
                throw new IgniteCheckedException("Stopping.");
        }
    }

    /** {@inheritDoc} */
    @Override public void flush(WALPointer ptr, boolean explicitFsync) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == WALMode.NONE)
            return;

        FileWriteHandle cur = currentHandle();

        // WAL manager was not started (client node).
        if (cur == null)
            return;

        FileWALPointer filePtr = (FileWALPointer)(ptr == null ? lastWALPtr.get() : ptr);

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
            dsCfg,
            new RecordSerializerFactoryImpl(cctx),
            ioFactory,
            archiver,
            decompressor,
            log
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
        String segmentName = FileDescriptor.fileName(absIdx);

        String zipSegmentName = FileDescriptor.fileName(absIdx) + ".zip";

        boolean inArchive = new File(walArchiveDir, segmentName).exists() ||
            new File(walArchiveDir, zipSegmentName).exists();

        if (inArchive)
            return true;

        if (absIdx <= lastArchivedIndex())
            return false;

        FileWriteHandle cur = currentHnd;

        return cur != null && cur.idx >= absIdx;
    }

    /** {@inheritDoc} */
    @Override public int truncate(WALPointer low, WALPointer high) {
        if (high == null)
            return 0;

        assert high instanceof FileWALPointer : high;

        // File pointer bound: older entries will be deleted from archive
        FileWALPointer lowPtr = (FileWALPointer)low;
        FileWALPointer highPtr = (FileWALPointer)high;

        FileDescriptor[] descs = scan(walArchiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER));

        int deleted = 0;

        FileArchiver archiver0 = archiver;

        for (FileDescriptor desc : descs) {
            if (lowPtr != null && desc.idx < lowPtr.index())
                continue;

            // Do not delete reserved or locked segment and any segment after it.
            if (archiver0 != null && archiver0.reserved(desc.idx))
                return deleted;

            long lastArchived = archiver0 != null ? archiver0.lastArchivedAbsoluteIndex() : lastArchivedIndex();

            // We need to leave at least one archived segment to correctly determine the archive index.
            if (desc.idx < highPtr.index() && desc.idx < lastArchived) {
                if (!desc.file.delete())
                    U.warn(log, "Failed to remove obsolete WAL segment (make sure the process has enough rights): " +
                        desc.file.getAbsolutePath());
                else
                    deleted++;

                // Bump up the oldest archive segment index.
                if (lastTruncatedArchiveIdx < desc.idx)
                    lastTruncatedArchiveIdx = desc.idx;
            }
        }

        return deleted;
    }

    /** {@inheritDoc} */
    @Override public void allowCompressionUntil(WALPointer ptr) {
        if (compressor != null)
            compressor.allowCompressionUntil(((FileWALPointer)ptr).index());
    }

    /** {@inheritDoc} */
    @Override public int walArchiveSegments() {
        long lastTruncated = lastTruncatedArchiveIdx;

        long lastArchived = archiver.lastArchivedAbsoluteIndex();

        if (lastArchived == -1)
            return 0;

        int res = (int)(lastArchived - lastTruncated);

        return res >= 0 ? res : 0;
    }

    /** {@inheritDoc} */
    @Override public long lastArchivedSegment() {
        return archiver.lastArchivedAbsoluteIndex();
    }

    /** {@inheritDoc} */
    @Override public boolean reserved(WALPointer ptr) {
        FileWALPointer fPtr = (FileWALPointer)ptr;

        FileArchiver archiver0 = archiver;

        return archiver0 != null && archiver0.reserved(fPtr.index());
    }

    /** {@inheritDoc} */
    @Override public int reserved(WALPointer low, WALPointer high) {
        // It is not clear now how to get the highest WAL pointer. So when high is null method returns 0.
        if (high == null)
            return 0;

        assert high instanceof FileWALPointer : high;

        assert low == null || low instanceof FileWALPointer : low;

        FileWALPointer lowPtr = (FileWALPointer)low;

        FileWALPointer highPtr = (FileWALPointer)high;

        FileArchiver archiver0 = archiver;

        long lowIdx = lowPtr != null ? lowPtr.index() : 0;

        long highIdx = highPtr.index();

        while (lowIdx < highIdx) {
            if(archiver0 != null && archiver0.reserved(lowIdx))
                break;

            lowIdx++;
        }

        return (int)(highIdx - lowIdx + 1);
    }

    /** {@inheritDoc} */
    @Override public boolean disabled(int grpId) {
        return cctx.walState().isDisabled(grpId);
    }

    /** {@inheritDoc} */
    @Override public void cleanupWalDirectories() throws IgniteCheckedException {
        try {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(walWorkDir.toPath())) {
                for (Path path : files)
                    Files.delete(path);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to cleanup wal work directory: " + walWorkDir, e);
        }

        try {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(walArchiveDir.toPath())) {
                for (Path path : files)
                    Files.delete(path);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to cleanup wal archive directory: " + walArchiveDir, e);
        }
    }

    /**
     * Lists files in archive directory and returns the index of last archived file.
     *
     * @return The absolute index of last archived file.
     */
    private long lastArchivedIndex() {
        long lastIdx = -1;

        for (File file : walArchiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER)) {
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
     * Lists files in archive directory and returns the indices of least and last archived files.
     * In case of holes, first segment after last "hole" is considered as minimum.
     * Example: minimum(0, 1, 10, 11, 20, 21, 22) should be 20
     *
     * @return The absolute indices of min and max archived files.
     */
    private IgniteBiTuple<Long, Long> scanMinMaxArchiveIndices() {
        TreeSet<Long> archiveIndices = new TreeSet<>();

        for (File file : walArchiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER)) {
            try {
                long idx = Long.parseLong(file.getName().substring(0, 16));

                archiveIndices.add(idx);
            }
            catch (NumberFormatException | IndexOutOfBoundsException ignore) {
                // No-op.
            }
        }

        if (archiveIndices.isEmpty())
            return null;
        else {
            Long min = archiveIndices.first();
            Long max = archiveIndices.last();

            if (max - min == archiveIndices.size() - 1)
                return F.t(min, max); // Short path.

            for (Long idx : archiveIndices.descendingSet()) {
                if (!archiveIndices.contains(idx - 1))
                    return F.t(idx, max);
            }

            throw new IllegalStateException("Should never happen if TreeSet is valid.");
        }
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
    private FileWriteHandle rollOver(FileWriteHandle cur) throws IgniteCheckedException {
        FileWriteHandle hnd = currentHandle();

        if (hnd != cur)
            return hnd;

        if (hnd.close(true)) {
            if (metrics.metricsEnabled())
                metrics.onWallRollOver();

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
     * @throws StorageException If failed to initialize WAL write handle.
     */
    private FileWriteHandle restoreWriteHandle(FileWALPointer lastReadPtr) throws StorageException {
        long absIdx = lastReadPtr == null ? 0 : lastReadPtr.index();

        long segNo = absIdx % dsCfg.getWalSegments();

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
                        serVer = readSegmentHeader(fileIO, absIdx).getSerializerVersion();
                    }
                    catch (SegmentEofException | EOFException ignore) {
                        serVer = serializerVersion;
                    }
                }

                RecordSerializer ser = new RecordSerializerFactoryImpl(cctx).createSerializer(serVer);

                if (log.isInfoEnabled())
                    log.info("Resuming logging to WAL segment [file=" + curFile.getAbsolutePath() +
                        ", offset=" + offset + ", ver=" + serVer + ']');

                FileWriteHandle hnd = new FileWriteHandle(
                    fileIO,
                    absIdx,
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
                try {
                    fileIO.close();
                }
                catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }

                if (e instanceof StorageException)
                    throw (StorageException) e;

                throw e instanceof IOException ? (IOException) e : new IOException(e);
            }
        }
        catch (IOException e) {
            throw new StorageException("Failed to restore WAL write handle: " + curFile.getAbsolutePath(), e);
        }
    }

    /**
     * Fills the file header for a new segment.
     * Calling this method signals we are done with the segment and it can be archived.
     * If we don't have prepared file yet and achiever is busy this method blocks
     *
     * @param curIdx current absolute segment released by WAL writer
     * @return Initialized file handle.
     * @throws IgniteCheckedException If exception occurred.
     */
    private FileWriteHandle initNextWriteHandle(long curIdx) throws IgniteCheckedException {
        IgniteCheckedException error = null;

        try {
            File nextFile = pollNextFile(curIdx);

            if (log.isDebugEnabled())
                log.debug("Switching to a new WAL segment: " + nextFile.getAbsolutePath());

            FileIO fileIO = ioFactory.create(nextFile);

            FileWriteHandle hnd = new FileWriteHandle(
                fileIO,
                curIdx + 1,
                0,
                maxWalSegmentSize,
                serializer);

            hnd.writeSerializerVersion();

            return hnd;
        }
        catch (IgniteCheckedException e) {
            throw error = e;
        }
        catch (IOException e) {
            throw error = new StorageException("Unable to initialize WAL segment", e);
        }
        finally {
            if (error != null)
                cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, error));
        }
    }

    /**
     * Deletes temp files, creates and prepares new; Creates first segment if necessary.
     *
     * @throws StorageException If failed.
     */
    private void checkOrPrepareFiles() throws StorageException {
        // Clean temp files.
        {
            File[] tmpFiles = walWorkDir.listFiles(WAL_SEGMENT_TEMP_FILE_FILTER);

            if (!F.isEmpty(tmpFiles)) {
                for (File tmp : tmpFiles) {
                    boolean deleted = tmp.delete();

                    if (!deleted)
                        throw new StorageException("Failed to delete previously created temp file " +
                            "(make sure Ignite process has enough rights): " + tmp.getAbsolutePath());
                }
            }
        }

        File[] allFiles = walWorkDir.listFiles(WAL_SEGMENT_FILE_FILTER);

        if (allFiles.length != 0 && allFiles.length > dsCfg.getWalSegments())
            throw new StorageException("Failed to initialize wal (work directory contains " +
                "incorrect number of segments) [cur=" + allFiles.length + ", expected=" + dsCfg.getWalSegments() + ']');

        // Allocate the first segment synchronously. All other segments will be allocated by archiver in background.
        if (allFiles.length == 0) {
            File first = new File(walWorkDir, FileDescriptor.fileName(0));

            createFile(first);
        }
        else
            checkFiles(0, false, null, null);
    }

    /**
     * Clears whole the file, fills with zeros for Default mode.
     *
     * @param file File to format.
     * @throws StorageException if formatting failed.
     */
    private void formatFile(File file) throws StorageException {
        formatFile(file, dsCfg.getWalSegmentSize());
    }

    /**
     * Clears the file, fills with zeros for Default mode.
     *
     * @param file File to format.
     * @param bytesCntToFormat Count of first bytes to format.
     * @throws StorageException If formatting failed.
     */
    private void formatFile(File file, int bytesCntToFormat) throws StorageException {
        if (log.isDebugEnabled())
            log.debug("Formatting file [exists=" + file.exists() + ", file=" + file.getAbsolutePath() + ']');

        try (FileIO fileIO = ioFactory.create(file, CREATE, READ, WRITE)) {
            int left = bytesCntToFormat;

            if (mode == WALMode.FSYNC) {
                while ((left -= fileIO.writeFully(FILL_BUF, 0, Math.min(FILL_BUF.length, left))) > 0)
                    ;

                fileIO.force();
            }
            else
                fileIO.clear();
        }
        catch (IOException e) {
            throw new StorageException("Failed to format WAL segment file: " + file.getAbsolutePath(), e);
        }
    }

    /**
     * Creates a file atomically with temp file.
     *
     * @param file File to create.
     * @throws StorageException If failed.
     */
    private void createFile(File file) throws StorageException {
        if (log.isDebugEnabled())
            log.debug("Creating new file [exists=" + file.exists() + ", file=" + file.getAbsolutePath() + ']');

        File tmp = new File(file.getParent(), file.getName() + ".tmp");

        formatFile(tmp);

        try {
            Files.move(tmp.toPath(), file.toPath());
        }
        catch (IOException e) {
            throw new StorageException("Failed to move temp file to a regular WAL segment file: " +
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
     * @throws StorageException If exception occurred in the archiver thread.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private File pollNextFile(long curIdx) throws StorageException, IgniteInterruptedCheckedException {
        // Signal to archiver that we are done with the segment and it can be archived.
        long absNextIdx = archiver.nextAbsoluteSegmentIndex(curIdx);

        long segmentIdx = absNextIdx % dsCfg.getWalSegments();

        return new File(walWorkDir, FileDescriptor.fileName(segmentIdx));
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
     * @throws StorageException If node is no longer valid and we missed a WAL operation.
     */
    private void checkNode() throws StorageException {
        if (cctx.kernalContext().invalid())
            throw new StorageException("Failed to perform WAL operation (environment was invalidated by a " +
                    "previous error)");
    }

    /**
     * File archiver operates on absolute segment indexes. For any given absolute segment index N we can calculate
     * the work WAL segment: S(N) = N % dsCfg.walSegments.
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
    private class FileArchiver extends GridWorker {
        /** Exception which occurred during initial creation of files or during archiving WAL segment */
        private StorageException cleanException;

        /**
         * Absolute current segment index WAL Manager writes to. Guarded by <code>this</code>.
         * Incremented during rollover. Also may be directly set if WAL is resuming logging after start.
         */
        private long curAbsWalIdx = -1;

        /** Last archived file index (absolute, 0-based). Guarded by <code>this</code>. */
        private volatile long lastAbsArchivedIdx = -1;

        /** current thread stopping advice */
        private volatile boolean stopped;

        /** */
        private NavigableMap<Long, Integer> reserved = new TreeMap<>();

        /**
         * Maps absolute segment index to locks counter. Lock on segment protects from archiving segment and may
         * come from {@link RecordsIterator} during WAL replay. Map itself is guarded by <code>this</code>.
         */
        private Map<Long, Integer> locked = new HashMap<>();

        /** Formatted index. */
        private int formatted;

        /**
         *
         */
        private FileArchiver(long lastAbsArchivedIdx, IgniteLogger log) {
            super(cctx.igniteInstanceName(), "wal-file-archiver%" + cctx.igniteInstanceName(), log,
                cctx.kernalContext().workersRegistry());

            this.lastAbsArchivedIdx = lastAbsArchivedIdx;
        }

        /**
         * @return Last archived segment absolute index.
         */
        private long lastArchivedAbsoluteIndex() {
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

            U.join(runner());
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
        @Override protected void body() {
            try {
                allocateRemainingFiles();
            }
            catch (StorageException e) {
                synchronized (this) {
                    // Stop the thread and report to starter.
                    cleanException = e;

                    notifyAll();
                }

                cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, e));

                return;
            }

            Throwable err = null;

            try {
                synchronized (this) {
                    while (curAbsWalIdx == -1 && !stopped)
                        wait();

                    // If the archive directory is empty, we can be sure that there were no WAL segments archived.
                    // This is ensured by the check in truncate() which will leave at least one file there
                    // once it was archived.
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

                    final SegmentArchiveResult res = archiveSegment(toArchive);

                    synchronized (this) {
                        while (locked.containsKey(toArchive) && !stopped)
                            wait();

                        changeLastArchivedIndexAndWakeupCompressor(toArchive);

                        notifyAll();
                    }

                    if (evt.isRecordable(EventType.EVT_WAL_SEGMENT_ARCHIVED)) {
                        evt.record(new WalSegmentArchivedEvent(cctx.discovery().localNode(),
                            res.getAbsIdx(), res.getDstArchiveFile()));
                    }
                }
            }
            catch (InterruptedException t) {
                Thread.currentThread().interrupt();

                if (!stopped)
                    err = t;
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                if (err == null && !stopped)
                    err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * @param idx Index.
         */
        private void changeLastArchivedIndexAndWakeupCompressor(long idx) {
            lastAbsArchivedIdx = idx;

            if (compressor != null)
                compressor.onNextSegmentArchived();
        }

        /**
         * Gets the absolute index of the next WAL segment available to write.
         * Blocks till there are available file to write
         *
         * @param curIdx Current absolute index that we want to increment.
         * @return Next index (curWalSegmIdx+1) when it is ready to be written.
         * @throws StorageException If exception occurred in the archiver thread.
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        private long nextAbsoluteSegmentIndex(long curIdx) throws StorageException, IgniteInterruptedCheckedException {
            try {
                synchronized (this) {
                    if (cleanException != null)
                        throw cleanException;

                    assert curIdx == curAbsWalIdx;

                    curAbsWalIdx++;

                    // Notify archiver thread.
                    notifyAll();

                    int segments = dsCfg.getWalSegments();

                    while ((curAbsWalIdx - lastAbsArchivedIdx > segments && cleanException == null))
                        wait();

                    if (cleanException != null)
                        throw cleanException;

                    // Wait for formatter so that we do not open an empty file in DEFAULT mode.
                    while (curAbsWalIdx % dsCfg.getWalSegments() > formatted && cleanException == null)
                        wait();

                    if (cleanException != null)
                        throw cleanException;

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
         * @return <ul><li>{@code True} if can read, no lock is held, </li><li>{@code false} if work segment, need
         * release segment later, use {@link #releaseWorkSegment} for unlock</li> </ul>
         */
        @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
        private boolean checkCanReadArchiveOrReserveWorkSegment(long absIdx) {
            synchronized (this) {
                if (lastAbsArchivedIdx >= absIdx) {
                    if (log.isDebugEnabled())
                        log.debug("Not needed to reserve WAL segment: absIdx=" + absIdx + ";" +
                            " lastAbsArchivedIdx=" + lastAbsArchivedIdx);

                    return true;
                }

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

                assert cur != null && cur > 0 : "WAL Segment with Index " + absIdx + " is not locked;" +
                    " lastAbsArchivedIdx = " + lastAbsArchivedIdx;

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
            long segIdx = absIdx % dsCfg.getWalSegments();

            File origFile = new File(walWorkDir, FileDescriptor.fileName(segIdx));

            String name = FileDescriptor.fileName(absIdx);

            File dstTmpFile = new File(walArchiveDir, name + ".tmp");

            File dstFile = new File(walArchiveDir, name);

            if (log.isInfoEnabled())
                log.info("Starting to copy WAL segment [absIdx=" + absIdx + ", segIdx=" + segIdx +
                    ", origFile=" + origFile.getAbsolutePath() + ", dstFile=" + dstFile.getAbsolutePath() + ']');

            try {
                Files.deleteIfExists(dstTmpFile.toPath());

                Files.copy(origFile.toPath(), dstTmpFile.toPath());

                Files.move(dstTmpFile.toPath(), dstFile.toPath());

                if (mode == WALMode.FSYNC) {
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

            if (log.isInfoEnabled())
                log.info("Copied file [src=" + origFile.getAbsolutePath() +
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
         * {@link FsyncModeFileWriteAheadLogManager#checkOrPrepareFiles()}
         */
        private void allocateRemainingFiles() throws StorageException {
            final FileArchiver archiver = this;

            checkFiles(1,
                true,
                new IgnitePredicate<Integer>() {
                    @Override public boolean apply(Integer integer) {
                        return !checkStop();
                    }
                }, new CI1<Integer>() {
                    @Override public void apply(Integer idx) {
                        synchronized (archiver) {
                            formatted = idx;

                            archiver.notifyAll();
                        }
                    }
                });
        }
    }

    /**
     * Responsible for compressing WAL archive segments.
     * Also responsible for deleting raw copies of already compressed WAL archive segments if they are not reserved.
     */
    private class FileCompressor extends Thread {
        /** Current thread stopping advice. */
        private volatile boolean stopped;

        /** Last successfully compressed segment. */
        private volatile long lastCompressedIdx = -1L;

        /** All segments prior to this (inclusive) can be compressed. */
        private volatile long lastAllowedToCompressIdx = -1L;

        /**
         *
         */
        FileCompressor() {
            super("wal-file-compressor%" + cctx.igniteInstanceName());
        }

        /**
         *
         */
        private void init() {
            File[] toDel = walArchiveDir.listFiles(WAL_SEGMENT_TEMP_FILE_COMPACTED_FILTER);

            for (File f : toDel) {
                if (stopped)
                    return;

                f.delete();
            }

            FileDescriptor[] alreadyCompressed = scan(walArchiveDir.listFiles(WAL_SEGMENT_FILE_COMPACTED_FILTER));

            if (alreadyCompressed.length > 0)
                lastCompressedIdx = alreadyCompressed[alreadyCompressed.length - 1].getIdx();
        }

        /**
         * @param lastCpStartIdx Segment index to allow compression until (exclusively).
         */
        synchronized void allowCompressionUntil(long lastCpStartIdx) {
            lastAllowedToCompressIdx = lastCpStartIdx - 1;

            notify();
        }

        /**
         * Callback for waking up compressor when new segment is archived.
         */
        synchronized void onNextSegmentArchived() {
            notify();
        }

        /**
         * Pessimistically tries to reserve segment for compression in order to avoid concurrent truncation.
         * Waits if there's no segment to archive right now.
         */
        private long tryReserveNextSegmentOrWait() throws InterruptedException, IgniteCheckedException {
            long segmentToCompress = lastCompressedIdx + 1;

            synchronized (this) {
                if (stopped)
                    return -1;

                while (segmentToCompress > Math.min(lastAllowedToCompressIdx, archiver.lastArchivedAbsoluteIndex())) {
                    wait();

                    if (stopped)
                        return -1;
                }
            }

            segmentToCompress = Math.max(segmentToCompress, lastTruncatedArchiveIdx + 1);

            boolean reserved = reserve(new FileWALPointer(segmentToCompress, 0, 0));

            return reserved ? segmentToCompress : -1;
        }

        /**
         * Deletes raw WAL segments if they aren't locked and already have compressed copies of themselves.
         */
        private void deleteObsoleteRawSegments() {
            FsyncModeFileWriteAheadLogManager.FileDescriptor[] descs = scan(walArchiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER));

            Set<Long> indices = new HashSet<>();
            Set<Long> duplicateIndices = new HashSet<>();

            for (FsyncModeFileWriteAheadLogManager.FileDescriptor desc : descs) {
                if (!indices.add(desc.idx))
                    duplicateIndices.add(desc.idx);
            }

            FileArchiver archiver0 = archiver;

            for (FsyncModeFileWriteAheadLogManager.FileDescriptor desc : descs) {
                if (desc.isCompressed())
                    continue;

                // Do not delete reserved or locked segment and any segment after it.
                if (archiver0 != null && archiver0.reserved(desc.idx))
                    return;

                if (desc.idx < lastCompressedIdx && duplicateIndices.contains(desc.idx)) {
                    if (!desc.file.delete())
                        U.warn(log, "Failed to remove obsolete WAL segment (make sure the process has enough rights): " +
                            desc.file.getAbsolutePath() + ", exists: " + desc.file.exists());
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void run() {
            init();

            while (!Thread.currentThread().isInterrupted() && !stopped) {
                long currReservedSegment = -1;

                try {
                    deleteObsoleteRawSegments();

                    currReservedSegment = tryReserveNextSegmentOrWait();
                    if (currReservedSegment == -1)
                        continue;

                    File tmpZip = new File(walArchiveDir, FileWriteAheadLogManager.FileDescriptor.fileName(currReservedSegment) + ".zip" + ".tmp");

                    File zip = new File(walArchiveDir, FileWriteAheadLogManager.FileDescriptor.fileName(currReservedSegment) + ".zip");

                    File raw = new File(walArchiveDir, FileWriteAheadLogManager.FileDescriptor.fileName(currReservedSegment));
                    if (!Files.exists(raw.toPath()))
                        throw new IgniteCheckedException("WAL archive segment is missing: " + raw);

                    compressSegmentToFile(currReservedSegment, raw, tmpZip);

                    Files.move(tmpZip.toPath(), zip.toPath());

                    if (mode != WALMode.NONE) {
                        try (FileIO f0 = ioFactory.create(zip, CREATE, READ, WRITE)) {
                            f0.force();
                        }
                    }

                    lastCompressedIdx = currReservedSegment;
                }
                catch (IgniteCheckedException | IOException e) {
                    U.error(log, "Compression of WAL segment [idx=" + currReservedSegment +
                        "] was skipped due to unexpected error", e);

                    lastCompressedIdx++;
                }
                catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    try {
                        if (currReservedSegment != -1)
                            release(new FileWALPointer(currReservedSegment, 0, 0));
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Can't release raw WAL segment [idx=" + currReservedSegment +
                            "] after compression", e);
                    }
                }
            }
        }

        /**
         * @param nextSegment Next segment absolute idx.
         * @param raw Raw file.
         * @param zip Zip file.
         */
        private void compressSegmentToFile(long nextSegment, File raw, File zip)
            throws IOException, IgniteCheckedException {
            int segmentSerializerVer;

            try (FileIO fileIO = ioFactory.create(raw)) {
                segmentSerializerVer = readSegmentHeader(fileIO, nextSegment).getSerializerVersion();
            }

            try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zip)))) {
                zos.putNextEntry(new ZipEntry(""));

                zos.write(prepareSerializerVersionBuffer(nextSegment, segmentSerializerVer, true).array());

                final CIX1<WALRecord> appendToZipC = new CIX1<WALRecord>() {
                    @Override public void applyx(WALRecord record) throws IgniteCheckedException {
                        final MarshalledRecord marshRec = (MarshalledRecord)record;

                        try {
                            zos.write(marshRec.buffer().array(), 0, marshRec.buffer().remaining());
                        }
                        catch (IOException e) {
                            throw new IgniteCheckedException(e);
                        }
                    }
                };

                try (SingleSegmentLogicalRecordsIterator iter = new SingleSegmentLogicalRecordsIterator(
                    log, cctx, ioFactory, tlbSize, nextSegment, walArchiveDir, appendToZipC)) {

                    while (iter.hasNextX())
                        iter.nextX();
                }
            }
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
    }

    /**
     * Responsible for decompressing previously compressed segments of WAL archive if they are needed for replay.
     */
    private class FileDecompressor extends GridWorker {
        /** Decompression futures. */
        private Map<Long, GridFutureAdapter<Void>> decompressionFutures = new HashMap<>();

        /** Segments queue. */
        private PriorityBlockingQueue<Long> segmentsQueue = new PriorityBlockingQueue<>();

        /** Byte array for draining data. */
        private byte[] arr = new byte[tlbSize];

        /**
         * @param log Logger.
         */
        FileDecompressor(IgniteLogger log) {
            super(cctx.igniteInstanceName(), "wal-file-decompressor%" + cctx.igniteInstanceName(), log,
                cctx.kernalContext().workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() {
            Throwable err = null;

            try {
                while (!isCancelled()) {
                    long segmentToDecompress = -1L;

                    try {
                        segmentToDecompress = segmentsQueue.take();

                        if (isCancelled())
                            break;

                        File zip = new File(walArchiveDir, FileDescriptor.fileName(segmentToDecompress) + ".zip");
                        File unzipTmp = new File(walArchiveDir, FileDescriptor.fileName(segmentToDecompress) + ".tmp");
                        File unzip = new File(walArchiveDir, FileDescriptor.fileName(segmentToDecompress));

                        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(zip)));
                             FileIO io = ioFactory.create(unzipTmp)) {
                            zis.getNextEntry();

                            while (io.writeFully(arr, 0, zis.read(arr)) > 0)
                                ;
                        }

                        try {
                            Files.move(unzipTmp.toPath(), unzip.toPath());
                        }
                        catch (FileAlreadyExistsException e) {
                            U.error(log, "Can't rename temporary unzipped segment: raw segment is already present " +
                                "[tmp=" + unzipTmp + ", raw=" + unzip + ']', e);

                            if (!unzipTmp.delete())
                                U.error(log, "Can't delete temporary unzipped segment [tmp=" + unzipTmp + ']');
                        }

                        synchronized (this) {
                            decompressionFutures.remove(segmentToDecompress).onDone();
                        }
                    }
                    catch (IOException ex) {
                        if (!isCancelled && segmentToDecompress != -1L) {
                            IgniteCheckedException e = new IgniteCheckedException("Error during WAL segment " +
                                "decompression [segmentIdx=" + segmentToDecompress + ']', ex);

                            synchronized (this) {
                                decompressionFutures.remove(segmentToDecompress).onDone(e);
                            }
                        }
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (!isCancelled)
                    err = e;
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                if (err == null && !isCancelled)
                    err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * Asynchronously decompresses WAL segment which is present only in .zip file.
         *
         * @return Future which is completed once file is decompressed.
         */
        synchronized IgniteInternalFuture<Void> decompressFile(long idx) {
            if (decompressionFutures.containsKey(idx))
                return decompressionFutures.get(idx);

            File f = new File(walArchiveDir, FileDescriptor.fileName(idx));

            if (f.exists())
                return new GridFinishedFuture<>();

            segmentsQueue.put(idx);

            GridFutureAdapter<Void> res = new GridFutureAdapter<>();

            decompressionFutures.put(idx, res);

            return res;
        }

        /** */
        private void shutdown() {
            synchronized (this) {
                U.cancel(this);

                // Put fake -1 to wake thread from queue.take()
                segmentsQueue.put(-1L);
            }

            U.join(this, log);
        }
    }

    /**
     * Validate files depending on {@link DataStorageConfiguration#getWalSegments()}  and create if need.
     * Check end when exit condition return false or all files are passed.
     *
     * @param startWith Start with.
     * @param create Flag create file.
     * @param p Predicate Exit condition.
     * @throws StorageException if validation or create file fail.
     */
    private void checkFiles(
        int startWith,
        boolean create,
        @Nullable IgnitePredicate<Integer> p,
        @Nullable IgniteInClosure<Integer> completionCallback
    ) throws StorageException {
        for (int i = startWith; i < dsCfg.getWalSegments() && (p == null || (p != null && p.apply(i))); i++) {
            File checkFile = new File(walWorkDir, FileDescriptor.fileName(i));

            if (checkFile.exists()) {
                if (checkFile.isDirectory())
                    throw new StorageException("Failed to initialize WAL log segment (a directory with " +
                        "the same name already exists): " + checkFile.getAbsolutePath());
                else if (checkFile.length() != dsCfg.getWalSegmentSize() && mode == WALMode.FSYNC)
                    throw new StorageException("Failed to initialize WAL log segment " +
                        "(WAL segment size change is not supported):" + checkFile.getAbsolutePath());
            }
            else if (create)
                createFile(checkFile);

            if (completionCallback != null)
                completionCallback.apply(i);
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
    public static long writeSerializerVersion(FileIO io, long idx, int version, WALMode mode) throws IOException {
        ByteBuffer buffer = prepareSerializerVersionBuffer(idx, version, false);

        io.writeFully(buffer);

        // Flush
        if (mode == WALMode.FSYNC)
            io.force();

        return io.position();
    }

    /**
     * @param idx Index.
     * @param ver Version.
     * @param compacted Compacted flag.
     */
    @NotNull private static ByteBuffer prepareSerializerVersionBuffer(long idx, int ver, boolean compacted) {
        ByteBuffer buf = ByteBuffer.allocate(RecordV1Serializer.HEADER_RECORD_SIZE);
        buf.order(ByteOrder.nativeOrder());

        // Write record type.
        buf.put((byte) (WALRecord.RecordType.HEADER_RECORD.ordinal() + 1));

        // Write position.
        RecordV1Serializer.putPosition(buf, new FileWALPointer(idx, 0, 0));

        // Place magic number.
        buf.putLong(compacted ? HeaderRecord.COMPACTED_MAGIC : HeaderRecord.REGULAR_MAGIC);

        // Place serializer version.
        buf.putInt(ver);

        // Place CRC if needed.
        if (!RecordV1Serializer.skipCrc) {
            int curPos = buf.position();

            buf.position(0);

            // This call will move buffer position to the end of the record again.
            int crcVal = PureJavaCrc32.calcCrc32(buf, curPos);

            buf.putInt(crcVal);
        }
        else
            buf.putInt(0);

        // Write header record through io.
        buf.position(0);

        return buf;
    }

    /**
     * WAL file descriptor.
     */
    public static class FileDescriptor implements Comparable<FileDescriptor>, AbstractWalRecordsIterator.AbstractFileDescriptor {
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

            assert fileName.contains(WAL_SEGMENT_FILE_EXT);

            this.idx = idx == null ? Long.parseLong(fileName.substring(0, 16)) : idx;
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

        /** {@inheritDoc} */
        @Override public boolean isCompressed() {
            return file.getName().endsWith(".zip");
        }

        /** {@inheritDoc} */
        @Override public File file() {
            return file;
        }

        /** {@inheritDoc} */
        @Override public long idx() {
            return idx;
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

        /**
         * @param fileIO I/O interface for read/write operations of FileHandle.
         * @param idx Absolute WAL segment file index (incremental counter).
         */
        private FileHandle(FileIO fileIO, long idx) {
            this.fileIO = fileIO;
            this.idx = idx;
        }
    }

    /**
     *
     */
    public static class ReadFileHandle extends FileHandle implements AbstractWalRecordsIterator.AbstractReadFileHandle {
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
                RecordSerializer ser,
                FileInput in
        ) {
            super(fileIO, idx);

            this.ser = ser;
            this.in = in;
        }

        /**
         * @throws IgniteCheckedException If failed to close the WAL segment file.
         */
        @Override public void close() throws IgniteCheckedException {
            try {
                fileIO.close();
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long idx() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override public FileInput in() {
            return in;
        }

        /** {@inheritDoc} */
        @Override public RecordSerializer ser() {
            return ser;
        }

        /** {@inheritDoc} */
        @Override public boolean workDir() {
            return workDir;
        }
    }

    /**
     * File handle for one log segment.
     */
    @SuppressWarnings("SignalWithoutCorrespondingAwait")
    private class FileWriteHandle extends FileHandle {
        /** */
        private final RecordSerializer serializer;

        /** See {@link FsyncModeFileWriteAheadLogManager#maxWalSegmentSize} */
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

        /** Condition for timed wait of several threads, see {@link DataStorageConfiguration#getWalFsyncDelayNanos()} */
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
            long pos,
            long maxSegmentSize,
            RecordSerializer serializer
        ) throws IOException {
            super(fileIO, idx);

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
         * @throws IOException If fail to write serializer version.
         */
        private void writeSerializerVersion() throws IOException {
            try {
                assert fileIO.position() == 0 : "Serializer version can be written only at the begin of file " +
                    fileIO.position();

                long updatedPosition = FsyncModeFileWriteAheadLogManager.writeSerializerVersion(fileIO, idx,
                    serializer.version(), mode);

                written = updatedPosition;
                lastFsyncPos = updatedPosition;
                head.set(new FakeRecord(new FileWALPointer(idx, (int)updatedPosition, 0), false));
            }
            catch (IOException e) {
                throw new IOException("Unable to write serializer version for segment " + idx, e);
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
         */
        @Nullable private WALPointer addRecord(WALRecord rec) throws StorageException {
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
                    rec.size());

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
         * @throws StorageException If failed.
         */
        private void flushOrWait(FileWALPointer ptr, boolean stop) throws StorageException {
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
                while (written < expWritten && !cctx.kernalContext().invalid())
                    U.awaitQuiet(writeComplete);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param ptr Pointer.
         * @return {@code true} If the flush really happened.
         * @throws StorageException If failed.
         */
        private boolean flush(FileWALPointer ptr, boolean stop) throws StorageException {
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
         * @throws StorageException If failed.
         */
        private boolean flush(WALRecord expHead, boolean stop) throws StorageException {
            if (expHead.previous() == null) {
                FakeRecord frHead = (FakeRecord)expHead;

                if (!stop || frHead.stop) // Protects from CASing terminal FakeRecord(true) to FakeRecord(false)
                    return false;
            }

            // Fail-fast before CAS.
            checkNode();

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
                StorageException se = e instanceof StorageException ? (StorageException) e :
                    new StorageException("Unable to write", new IOException(e));

                cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, se));

                // All workers waiting for a next segment must be woken up and stopped
                signalNextAvailable();

                throw se;
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
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        private void fsync(FileWALPointer ptr, boolean stop) throws StorageException, IgniteInterruptedCheckedException {
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
         * @throws StorageException If failed.
         */
        private boolean close(boolean rollOver) throws StorageException {
            if (stop.compareAndSet(false, true)) {
                lock.lock();

                try {
                    flushOrWait(null, true);

                    assert stopped() : "Segment is not closed after close flush: " + head.get();

                    try {
                        try {
                            RecordSerializer backwardSerializer = new RecordSerializerFactoryImpl(cctx)
                                .createSerializer(serializerVersion);

                            SwitchSegmentRecord segmentRecord = new SwitchSegmentRecord();

                            int switchSegmentRecSize = backwardSerializer.size(segmentRecord);

                            if (rollOver && written < (maxSegmentSize - switchSegmentRecSize)) {
                                final ByteBuffer buf = ByteBuffer.allocate(switchSegmentRecSize);

                                segmentRecord.position(new FileWALPointer(idx, (int)written, switchSegmentRecSize));
                                backwardSerializer.writeRecord(segmentRecord, buf);

                                buf.rewind();

                                written += fileIO.writeFully(buf, written);
                            }
                        }
                        catch (IgniteCheckedException e) {
                            throw new IOException(e);
                        }
                        finally {
                            assert mode == WALMode.FSYNC;

                            // Do the final fsync.
                            fileIO.force();

                            lastFsyncPos = written;

                            fileIO.close();
                        }
                    }
                    catch (IOException e) {
                        throw new StorageException("Failed to close WAL write handle [idx=" + idx + "]", e);
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

                if (!cctx.kernalContext().invalid()) {
                    assert rec instanceof FakeRecord : "Expected head FakeRecord, actual head "
                    + (rec != null ? rec.getClass().getSimpleName() : "null");

                    assert written == lastFsyncPos || mode != WALMode.FSYNC :
                    "fsync [written=" + written + ", lastFsync=" + lastFsyncPos + ']';

                    fileIO = null;
                }
                else {
                    try {
                        fileIO.close();
                    }
                    catch (IOException e) {
                        U.error(log, "Failed to close WAL file [idx=" + idx + ", fileIO=" + fileIO + "]", e);
                    }
                }

                nextSegment.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         *
         */
        private void awaitNext() {
            lock.lock();

            try {
                while (fileIO != null && !cctx.kernalContext().invalid())
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
        private void writeBuffer(long pos, ByteBuffer buf) throws StorageException {
            boolean interrupted = false;

            lock.lock();

            try {
                assert fileIO != null : "Writing to a closed segment.";

                checkNode();

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

                    checkNode();
                }

                // Do the write.
                int size = buf.remaining();

                assert size > 0 : size;

                try {
                    assert written == fileIO.position();

                    fileIO.writeFully(buf);

                    written += size;

                    metrics.onWalBytesWritten(size);

                    assert written == fileIO.position();
                }
                catch (IOException e) {
                    StorageException se = new StorageException("Unable to write", e);

                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, se));

                    throw se;
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
    private class RecordsIterator extends AbstractWalRecordsIterator {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final File walWorkDir;

        /** */
        private final File walArchiveDir;

        /** */
        private final FileArchiver archiver;

        /** */
        private final FileDecompressor decompressor;

        /** */
        private final DataStorageConfiguration psCfg;

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
         * @param serializerFactory Serializer factory.
         * @param archiver Archiver.
         * @param decompressor Decompressor.
         *@param log Logger  @throws IgniteCheckedException If failed to initialize WAL segment.
         */
        private RecordsIterator(
            GridCacheSharedContext cctx,
            File walWorkDir,
            File walArchiveDir,
            @Nullable FileWALPointer start,
            @Nullable FileWALPointer end,
            DataStorageConfiguration psCfg,
            @NotNull RecordSerializerFactory serializerFactory,
            FileIOFactory ioFactory,
            FileArchiver archiver,
            FileDecompressor decompressor,
            IgniteLogger log
        ) throws IgniteCheckedException {
            super(log,
                cctx,
                serializerFactory,
                ioFactory,
                psCfg.getWalRecordIteratorBufferSize());
            this.walWorkDir = walWorkDir;
            this.walArchiveDir = walArchiveDir;
            this.psCfg = psCfg;
            this.archiver = archiver;
            this.start = start;
            this.end = end;
            this.decompressor = decompressor;

            init();

            advance();
        }

        /** {@inheritDoc} */
        @Override protected ReadFileHandle initReadHandle(
            @NotNull AbstractFileDescriptor desc,
            @Nullable FileWALPointer start
        ) throws IgniteCheckedException, FileNotFoundException {
            AbstractFileDescriptor currDesc = desc;

            if (!desc.file().exists()) {
                FileDescriptor zipFile = new FileDescriptor(
                        new File(walArchiveDir, FileDescriptor.fileName(desc.idx()) + ".zip"));

                if (!zipFile.file.exists()) {
                    throw new FileNotFoundException("Both compressed and raw segment files are missing in archive " +
                            "[segmentIdx=" + desc.idx() + "]");
                }

                if (decompressor != null)
                    decompressor.decompressFile(desc.idx()).get();
                else
                    currDesc = zipFile;
            }

            return (ReadFileHandle) super.initReadHandle(currDesc, start);
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            super.onClose();

            curRec = null;

            final AbstractReadFileHandle handle = closeCurrentWalSegment();

            if (handle != null && handle.workDir())
                releaseWorkSegment(curWalSegmIdx);

            curWalSegmIdx = Integer.MAX_VALUE;
        }

        /**
         * @throws IgniteCheckedException If failed to initialize first file handle.
         */
        private void init() throws IgniteCheckedException {
            AbstractFileDescriptor[] descs = loadFileDescriptors(walArchiveDir);

            if (start != null) {
                if (!F.isEmpty(descs)) {
                    if (descs[0].idx() > start.index())
                        throw new IgniteCheckedException("WAL history is too short " +
                            "[descs=" + Arrays.asList(descs) + ", start=" + start + ']');

                    for (AbstractFileDescriptor desc : descs) {
                        if (desc.idx() == start.index()) {
                            curWalSegmIdx = start.index();

                            break;
                        }
                    }

                    if (curWalSegmIdx == -1) {
                        long lastArchived = descs[descs.length - 1].idx();

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
                curWalSegmIdx = !F.isEmpty(descs) ? descs[0].idx() : 0;

            curWalSegmIdx--;

            if (log.isDebugEnabled())
                log.debug("Initialized WAL cursor [start=" + start + ", end=" + end + ", curWalSegmIdx=" + curWalSegmIdx + ']');
        }

        /** {@inheritDoc} */
        @Override protected AbstractReadFileHandle advanceSegment(
            @Nullable final AbstractReadFileHandle curWalSegment
        ) throws IgniteCheckedException {
            if (curWalSegment != null) {
                curWalSegment.close();

                if (curWalSegment.workDir())
                    releaseWorkSegment(curWalSegment.idx());

            }

            // We are past the end marker.
            if (end != null && curWalSegmIdx + 1 > end.index())
                return null; //stop iteration

            curWalSegmIdx++;

            FileDescriptor fd;

            boolean readArchive = canReadArchiveOrReserveWork(curWalSegmIdx);

            if (readArchive)
                fd = new FileDescriptor(new File(walArchiveDir, FileDescriptor.fileName(curWalSegmIdx)));
            else {
                long workIdx = curWalSegmIdx % psCfg.getWalSegments();

                fd = new FileDescriptor(
                    new File(walWorkDir, FileDescriptor.fileName(workIdx)),
                    curWalSegmIdx);
            }

            if (log.isDebugEnabled())
                log.debug("Reading next file [absIdx=" + curWalSegmIdx + ", file=" + fd.file().getAbsolutePath() + ']');

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

            if (nextHandle == null) {
                if (!readArchive)
                    releaseWorkSegment(curWalSegmIdx);
            }
            else
                nextHandle.workDir = !readArchive;

            curRec = null;

            return nextHandle;
        }

        /**
         * @param absIdx Absolute index to check.
         * @return <ul><li> {@code True} if we can safely read the archive,  </li> <li>{@code false} if the segment has
         * not been archived yet. In this case the corresponding work segment is reserved (will not be deleted until
         * release). Use {@link #releaseWorkSegment} for unlock </li></ul>
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

        /** {@inheritDoc} */
        @Override protected AbstractReadFileHandle createReadFileHandle(FileIO fileIO, long idx,
            RecordSerializer ser, FileInput in) {
            return new ReadFileHandle(fileIO, idx, ser, in);
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

    /**
     * Scans provided folder for a WAL segment files
     * @param walFilesDir directory to scan
     * @return found WAL file descriptors
     */
    private static FileDescriptor[] loadFileDescriptors(@NotNull final File walFilesDir) throws IgniteCheckedException {
        final File[] files = walFilesDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

        if (files == null) {
            throw new IgniteCheckedException("WAL files directory does not not denote a " +
                "directory, or if an I/O error occurs: [" + walFilesDir.getAbsolutePath() + "]");
        }
        return scan(files);
    }
}
