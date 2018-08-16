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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
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
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
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
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.wal.AbstractWalRecordsIterator.AbstractFileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.F;
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
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_MMAP;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SEGMENT_SYNC_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SERIALIZER_VERSION;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer.BufferMode.DIRECT;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readSegmentHeader;
import static org.apache.ignite.internal.util.IgniteUtils.findField;
import static org.apache.ignite.internal.util.IgniteUtils.findNonPublicMethod;
import static org.apache.ignite.internal.util.IgniteUtils.sleep;

/**
 * File WAL manager.
 */
@SuppressWarnings("IfMayBeConditional")
public class FileWriteAheadLogManager extends GridCacheSharedManagerAdapter implements IgniteWriteAheadLogManager {
    /** Dfault wal segment sync timeout. */
    public static final long DFLT_WAL_SEGMENT_SYNC_TIMEOUT = 500L;
    /** {@link MappedByteBuffer#force0(java.io.FileDescriptor, long, long)}. */
    private static final Method force0 = findNonPublicMethod(
        MappedByteBuffer.class, "force0",
        java.io.FileDescriptor.class, long.class, long.class
    );

    /** {@link MappedByteBuffer#mappingOffset()}. */
    private static final Method mappingOffset = findNonPublicMethod(MappedByteBuffer.class, "mappingOffset");

    /** {@link MappedByteBuffer#mappingAddress(long)}. */
    private static final Method mappingAddress = findNonPublicMethod(
        MappedByteBuffer.class, "mappingAddress", long.class
    );

    /** {@link MappedByteBuffer#fd} */
    private static final Field fd = findField(MappedByteBuffer.class, "fd");

    /** Page size. */
    private static final int PAGE_SIZE = GridUnsafe.pageSize();

    /** */
    private static final FileDescriptor[] EMPTY_DESCRIPTORS = new FileDescriptor[0];

    /** WAL segment file extension. */
    private static final String WAL_SEGMENT_FILE_EXT = ".wal";

    /** */
    private static final byte[] FILL_BUF = new byte[1024 * 1024];

    /** Pattern for segment file names */
    public static final Pattern WAL_NAME_PATTERN = Pattern.compile("\\d{16}\\.wal");

    /** */
    public static final Pattern WAL_TEMP_NAME_PATTERN = Pattern.compile("\\d{16}\\.wal\\.tmp");

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
    public static final Pattern WAL_SEGMENT_FILE_COMPACTED_PATTERN = Pattern.compile("\\d{16}\\.wal\\.zip");

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

    /** Buffer size. */
    private static final int BUF_SIZE = 1024 * 1024;

    /** Use mapped byte buffer. */
    private final boolean mmap = IgniteSystemProperties.getBoolean(IGNITE_WAL_MMAP, true);

    /** {@link FileWriteHandle#written} atomic field updater. */
    private static final AtomicLongFieldUpdater<FileWriteHandle> WRITTEN_UPD =
        AtomicLongFieldUpdater.newUpdater(FileWriteHandle.class, "written");

    /** Interrupted flag. */
    private final ThreadLocal<Boolean> interrupted = new ThreadLocal<Boolean>() {
        @Override protected Boolean initialValue() {
            return false;
        }
    };

    /** */
    private final boolean alwaysWriteFullPages;

    /** WAL segment size in bytes. . This is maximum value, actual segments may be shorter. */
    private final long maxWalSegmentSize;

    /** */
    private final WALMode mode;

    /** WAL flush frequency. Makes sense only for {@link WALMode#BACKGROUND} log WALMode. */
    private final long flushFreq;

    /** Fsync delay. */
    private final long fsyncDelay;

    /** */
    private final DataStorageConfiguration dsCfg;

    /** Events service */
    private final GridEventStorageManager evt;

    /** Failure processor */
    private final FailureProcessor failureProcessor;

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
    private final int serializerVer =
        IgniteSystemProperties.getInteger(IGNITE_WAL_SERIALIZER_VERSION, LATEST_SERIALIZER_VERSION);

    /** Latest segment cleared by {@link #truncate(WALPointer, WALPointer)}. */
    private volatile long lastTruncatedArchiveIdx = -1L;

    /** Factory to provide I/O interfaces for read/write operations with files */
    private volatile FileIOFactory ioFactory;

    /** Next WAL segment archived monitor. Manages last archived index, emulates archivation in no-archiver mode. */
    private final SegmentArchivedMonitor archivedMonitor = new SegmentArchivedMonitor();

    /** Segment reservations storage: Protects WAL segments from deletion during WAL log cleanup. */
    private final SegmentReservationStorage reservationStorage = new SegmentReservationStorage();

    /** Updater for {@link #currHnd}, used for verify there are no concurrent update for current log segment handle */
    private static final AtomicReferenceFieldUpdater<FileWriteAheadLogManager, FileWriteHandle> CURR_HND_UPD =
        AtomicReferenceFieldUpdater.newUpdater(FileWriteAheadLogManager.class, FileWriteHandle.class, "currHnd");

    /**
     * File archiver moves segments from work directory to archive. Locked segments may be kept not moved until
     * release. For mode archive and work folders set to equal value, archiver is not created.
     */
    @Nullable private volatile FileArchiver archiver;

    /** Compressor. */
    private volatile FileCompressor compressor;

    /** Decompressor. */
    private volatile FileDecompressor decompressor;

    /** */
    private final ThreadLocal<WALPointer> lastWALPtr = new ThreadLocal<>();

    /** Current log segment handle */
    private volatile FileWriteHandle currHnd;

    /** */
    private volatile WALDisableContext walDisableContext;

    /**
     * Positive (non-0) value indicates WAL can be archived even if not complete<br>
     * See {@link DataStorageConfiguration#setWalAutoArchiveAfterInactivity(long)}<br>
     */
    private final long walAutoArchiveAfterInactivity;

    /**
     * Container with last WAL record logged timestamp.<br> Zero value means there was no records logged to current
     * segment, skip possible archiving for this case<br> Value is filled only for case {@link
     * #walAutoArchiveAfterInactivity} > 0<br>
     */
    private AtomicLong lastRecordLoggedMs = new AtomicLong();

    /**
     * Cancellable task for {@link WALMode#BACKGROUND}, should be cancelled at shutdown.
     * Null for non background modes.
     */
    @Nullable private volatile GridTimeoutProcessor.CancelableTask backgroundFlushSchedule;

    /**
     * Reference to the last added next archive timeout check object. Null if mode is not enabled. Should be cancelled
     * at shutdown
     */
    @Nullable private volatile GridTimeoutObject nextAutoArchiveTimeoutObj;

    /** WAL writer worker. */
    private WALWriter walWriter;

    /**
     * Listener invoked for each segment file IO initializer.
     */
    @Nullable private volatile IgniteInClosure<FileIO> createWalFileListener;

    /** Wal segment sync worker. */
    private WalSegmentSyncer walSegmentSyncWorker;

    /**
     * @param ctx Kernal context.
     */
    public FileWriteAheadLogManager(@NotNull final GridKernalContext ctx) {
        igCfg = ctx.config();

        DataStorageConfiguration dsCfg = igCfg.getDataStorageConfiguration();

        assert dsCfg != null;

        this.dsCfg = dsCfg;

        maxWalSegmentSize = dsCfg.getWalSegmentSize();
        mode = dsCfg.getWalMode();
        flushFreq = dsCfg.getWalFlushFrequency();
        fsyncDelay = dsCfg.getWalFsyncDelayNanos();
        alwaysWriteFullPages = dsCfg.isAlwaysWriteFullPages();
        ioFactory = new RandomAccessFileIOFactory();
        walAutoArchiveAfterInactivity = dsCfg.getWalAutoArchiveAfterInactivity();
        evt = ctx.event();
        failureProcessor = ctx.failure();
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

            serializer = new RecordSerializerFactoryImpl(cctx).createSerializer(serializerVer);

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

            metrics = dbMgr.persistentStoreMetricsImpl();

            checkOrPrepareFiles();

            if (metrics != null)
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

            long lastAbsArchivedIdx = tup == null ? -1 : tup.get2();

            if (isArchiverEnabled())
                archiver = new FileArchiver(lastAbsArchivedIdx, log);
            else
                archiver = null;

            if (lastAbsArchivedIdx > 0)
                archivedMonitor.setLastArchivedAbsoluteIndex(lastAbsArchivedIdx);

            if (dsCfg.isWalCompactionEnabled()) {
                compressor = new FileCompressor();

                if (decompressor == null) {  // Preventing of two file-decompressor thread instantiations.
                    decompressor = new FileDecompressor(log);

                    new IgniteThread(decompressor).start();
                }
            }

            walDisableContext = cctx.walState().walDisableContext();

            if (mode != WALMode.NONE && mode != WALMode.FSYNC) {
                walSegmentSyncWorker = new WalSegmentSyncer(igCfg.getIgniteInstanceName(),
                    cctx.kernalContext().log(WalSegmentSyncer.class));

                if (log.isInfoEnabled())
                    log.info("Started write-ahead log manager [mode=" + mode + ']');
            }
            else
                U.quietAndWarn(log, "Started write-ahead log manager in NONE mode, persisted data may be lost in " +
                    "a case of unexpected node failure. Make sure to deactivate the cluster before shutdown.");
        }
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

        archivedMonitor.awaitSegmentArchived(awaitIdx);

        if (!reserve(low))
            throw new IgniteCheckedException("WAL archive segment has been deleted [idx=" + low.index() + "]");

        List<File> res = new ArrayList<>();

        for (long i = low.index(); i < high.index(); i++) {
            String segmentName = FileDescriptor.fileName(i);

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
                    currHnd.flush(null);
            }

            if (currHnd != null)
                currHnd.close(false);

            if (walWriter != null)
                walWriter.shutdown();

            if (archiver != null)
                archiver.shutdown();

            if (compressor != null)
                compressor.shutdown();

            if (decompressor != null)
                decompressor.shutdown();

            if (walSegmentSyncWorker != null)
                walSegmentSyncWorker.shutdown();
        }
        catch (Exception e) {
            U.error(log, "Failed to gracefully close WAL segment: " + this.currHnd.fileIO, e);
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

            if (walSegmentSyncWorker != null)
                new IgniteThread(walSegmentSyncWorker).start();

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

        currHnd = null;
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
        assert currHnd == null;
        assert lastPtr == null || lastPtr instanceof FileWALPointer;

        FileWALPointer filePtr = (FileWALPointer)lastPtr;

        walWriter = new WALWriter(log);

        if (!mmap)
            new IgniteThread(walWriter).start();

        currHnd = restoreWriteHandle(filePtr);

        // For new handle write serializer version to it.
        if (filePtr == null)
            currHnd.writeHeader();

        if (currHnd.serializer.version() != serializer.version()) {
            if (log.isInfoEnabled())
                log.info("Record serializer version change detected, will start logging with a new WAL record " +
                    "serializer to a new WAL segment [curFile=" + currHnd + ", newVer=" + serializer.version() +
                    ", oldVer=" + currHnd.serializer.version() + ']');

            rollOver(currHnd);
        }

        currHnd.resume = false;

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
     * Schedules next check of inactivity period expired. Based on current record update timestamp. At timeout method
     * does check of inactivity period and schedules new launch.
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

    /** {@inheritDoc} */
    @Override public int serializerVersion() {
        return serializerVer;
    }

    /**
     * Checks if there was elapsed significant period of inactivity. If WAL auto-archive is enabled using
     * {@link #walAutoArchiveAfterInactivity} > 0 this method will activate roll over by timeout.<br>
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
            handle.buf.close();

            rollOver(handle);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Unable to perform segment rollover: " + e.getMessage(), e);

            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, e));
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("TooBroadScope")
    @Override public WALPointer log(WALRecord rec) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == WALMode.NONE)
            return null;

        FileWriteHandle currWrHandle = currentHandle();

        WALDisableContext isDisable = walDisableContext;

        // Logging was not resumed yet.
        if (currWrHandle == null || (isDisable != null && isDisable.check()))
            return null;

        // Need to calculate record size first.
        rec.size(serializer.size(rec));

        while (true) {
            if (rec.rollOver()){
                assert cctx.database().checkpointLockIsHeldByThread();

                long idx = currWrHandle.idx;

                currWrHandle.buf.close();

                currWrHandle = rollOver(currWrHandle);

                if (log != null && log.isInfoEnabled())
                    log.info("Rollover segment [" + idx + " to " + currWrHandle.idx + "], recordType=" + rec.type());
            }

            WALPointer ptr = currWrHandle.addRecord(rec);

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

        if (mode == LOG_ONLY)
            cur.flushOrWait(filePtr);

        if (!explicitFsync && mode != WALMode.FSYNC)
            return; // No need to sync in LOG_ONLY or BACKGROUND unless explicit fsync is required.

        // No need to sync if was rolled over.
        if (filePtr != null && !cur.needFsync(filePtr))
            return;

        cur.fsync(filePtr);
    }

    /** {@inheritDoc} */
    @Override public WALIterator replay(WALPointer start) throws IgniteCheckedException, StorageException {
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

        reservationStorage.reserve(((FileWALPointer)start).index());

        if (!hasIndex(((FileWALPointer)start).index())) {
            reservationStorage.release(((FileWALPointer)start).index());

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void release(WALPointer start) {
        assert start != null && start instanceof FileWALPointer : "Invalid start pointer: " + start;

        if (mode == WALMode.NONE)
            return;

        reservationStorage.release(((FileWALPointer)start).index());
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

        FileWriteHandle cur = currHnd;

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

        for (FileDescriptor desc : descs) {
            if (lowPtr != null && desc.idx < lowPtr.index())
                continue;

            // Do not delete reserved or locked segment and any segment after it.
            if (segmentReservedOrLocked(desc.idx))
                return deleted;

            long archivedAbsIdx = archivedMonitor.lastArchivedAbsoluteIndex();

            long lastArchived = archivedAbsIdx >= 0 ? archivedAbsIdx : lastArchivedIndex();

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

    /**
     * Check if WAL segment locked (protected from move to archive) or reserved (protected from deletion from WAL
     * cleanup).
     *
     * @param absIdx Absolute WAL segment index for check reservation.
     * @return {@code True} if index is locked.
     */
    private boolean segmentReservedOrLocked(long absIdx) {
        FileArchiver archiver0 = archiver;

        return ((archiver0 != null) && archiver0.locked(absIdx))
            || (reservationStorage.reserved(absIdx));

    }

    /** {@inheritDoc} */
    @Override public void allowCompressionUntil(WALPointer ptr) {
        if (compressor != null)
            compressor.allowCompressionUntil(((FileWALPointer)ptr).index());
    }

    /** {@inheritDoc} */
    @Override public int walArchiveSegments() {
        long lastTruncated = lastTruncatedArchiveIdx;

        long lastArchived = archivedMonitor.lastArchivedAbsoluteIndex();

        if (lastArchived == -1)
            return 0;

        int res = (int)(lastArchived - lastTruncated);

        return res >= 0 ? res : 0;
    }

    /** {@inheritDoc} */
    @Override public long lastArchivedSegment() {
        return archivedMonitor.lastArchivedAbsoluteIndex();
    }

    /** {@inheritDoc} */
    @Override public boolean reserved(WALPointer ptr) {
        FileWALPointer fPtr = (FileWALPointer)ptr;

        return segmentReservedOrLocked(fPtr.index());
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

        long lowIdx = lowPtr != null ? lowPtr.index() : 0;

        long highIdx = highPtr.index();

        while (lowIdx < highIdx) {
            if (segmentReservedOrLocked(lowIdx))
                break;

            lowIdx++;
        }

        return (int)(highIdx - lowIdx + 1);
    }

    /** {@inheritDoc} */
    @Override public boolean disabled(int grpId) {
        return cctx.walState().isDisabled(grpId);
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
        return currHnd;
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
            if (metrics.metricsEnabled())
                metrics.onWallRollOver();

            FileWriteHandle next = initNextWriteHandle(cur);

            next.writeHeader();

            boolean swapped = CURR_HND_UPD.compareAndSet(this, hnd, next);

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

        @Nullable FileArchiver archiver0 = archiver;

        long segNo = archiver0 == null ? absIdx : absIdx % dsCfg.getWalSegments();

        File curFile = new File(walWorkDir, FileDescriptor.fileName(segNo));

        int off = lastReadPtr == null ? 0 : lastReadPtr.fileOffset();
        int len = lastReadPtr == null ? 0 : lastReadPtr.length();

        try {
            FileIO fileIO = ioFactory.create(curFile);

            IgniteInClosure<FileIO> lsnr = createWalFileListener;

            if (lsnr != null)
                lsnr.apply(fileIO);

            try {
                int serVer = serializerVer;

                // If we have existing segment, try to read version from it.
                if (lastReadPtr != null) {
                    try {
                        serVer = readSegmentHeader(fileIO, absIdx).getSerializerVersion();
                    }
                    catch (SegmentEofException | EOFException ignore) {
                        serVer = serializerVer;
                    }
                }

                RecordSerializer ser = new RecordSerializerFactoryImpl(cctx).createSerializer(serVer);

                if (log.isInfoEnabled())
                    log.info("Resuming logging to WAL segment [file=" + curFile.getAbsolutePath() +
                        ", offset=" + off + ", ver=" + serVer + ']');

                SegmentedRingByteBuffer rbuf;

                if (mmap) {
                    MappedByteBuffer buf = fileIO.map((int)maxWalSegmentSize);

                    rbuf = new SegmentedRingByteBuffer(buf, metrics);
                }
                else
                    rbuf = new SegmentedRingByteBuffer(dsCfg.getWalBufferSize(), maxWalSegmentSize, DIRECT, metrics);

                if (lastReadPtr != null)
                    rbuf.init(lastReadPtr.fileOffset() + lastReadPtr.length());

                FileWriteHandle hnd = new FileWriteHandle(
                    fileIO,
                    absIdx,
                    off + len,
                    true,
                    ser,
                    rbuf);

                if (archiver0 != null)
                    archiver0.currentWalIndex(absIdx);
                else
                    archivedMonitor.setLastArchivedAbsoluteIndex(absIdx - 1);

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
     * Fills the file header for a new segment. Calling this method signals we are done with the segment and it can be
     * archived. If we don't have prepared file yet and achiever is busy this method blocks
     *
     * @param cur Current file write handle released by WAL writer
     * @return Initialized file handle.
     * @throws IgniteCheckedException If exception occurred.
     */
    private FileWriteHandle initNextWriteHandle(FileWriteHandle cur) throws IgniteCheckedException {
        IgniteCheckedException error = null;

        try {
            File nextFile = pollNextFile(cur.idx);

            if (log.isDebugEnabled())
                log.debug("Switching to a new WAL segment: " + nextFile.getAbsolutePath());

            SegmentedRingByteBuffer rbuf = null;

            FileIO fileIO = null;

            FileWriteHandle hnd;

            boolean interrupted = this.interrupted.get();

            while (true) {
                try {
                    fileIO = ioFactory.create(nextFile);

                    IgniteInClosure<FileIO> lsnr = createWalFileListener;
                    if (lsnr != null)
                        lsnr.apply(fileIO);

                    if (mmap) {
                        MappedByteBuffer buf = fileIO.map((int)maxWalSegmentSize);

                        rbuf = new SegmentedRingByteBuffer(buf, metrics);
                    }
                    else
                        rbuf = cur.buf.reset();

                    hnd = new FileWriteHandle(
                        fileIO,
                        cur.idx + 1,
                        0,
                        false,
                        serializer,
                        rbuf);


                    if (interrupted)
                        Thread.currentThread().interrupt();

                    break;
                }
                catch (ClosedByInterruptException ignore) {
                    interrupted = true;

                    Thread.interrupted();

                    if (fileIO != null) {
                        try {
                            fileIO.close();
                        }
                        catch (IOException ignored) {
                            // No-op.
                        }

                        fileIO = null;
                    }

                    if (rbuf != null) {
                        rbuf.free();

                        rbuf = null;
                    }
                }
                finally {
                    this.interrupted.set(false);
                }
            }

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
     * Deletes temp files, creates and prepares new; Creates first segment if necessary
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

        if(isArchiverEnabled())
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
     * Clears whole the file, fills with zeros for Default mode.
     *
     * @param file File to format.
     * @throws StorageException if formatting failed
     */
    private void formatFile(File file) throws StorageException {
        formatFile(file, dsCfg.getWalSegmentSize());
    }

    /**
     * Clears the file, fills with zeros for Default mode.
     *
     * @param file File to format.
     * @param bytesCntToFormat Count of first bytes to format.
     * @throws StorageException if formatting failed
     */
    private void formatFile(File file, int bytesCntToFormat) throws StorageException {
        if (log.isDebugEnabled())
            log.debug("Formatting file [exists=" + file.exists() + ", file=" + file.getAbsolutePath() + ']');

        try (FileIO fileIO = ioFactory.create(file, CREATE, READ, WRITE)) {
            int left = bytesCntToFormat;

            if (mode == WALMode.FSYNC || mmap) {
                while ((left -= fileIO.writeFully(FILL_BUF, 0, Math.min(FILL_BUF.length, left))) > 0)
                    ;

                fileIO.force();
            }
            else
                fileIO.clear();
        }
        catch (IOException e) {
            StorageException ex = new StorageException("Failed to format WAL segment file: " + file.getAbsolutePath(), e);

            if (failureProcessor != null)
                failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, ex));
            
            throw ex;
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
     * Retrieves next available file to write WAL data, waiting if necessary for a segment to become available.
     *
     * @param curIdx Current absolute WAL segment index.
     * @return File ready for use as new WAL segment.
     * @throws StorageException If exception occurred in the archiver thread.
     */
    private File pollNextFile(long curIdx) throws StorageException {
        FileArchiver archiver0 = archiver;

        if (archiver0 == null) {
            archivedMonitor.setLastArchivedAbsoluteIndex(curIdx);

            return new File(walWorkDir, FileDescriptor.fileName(curIdx + 1));
        }

        // Signal to archiver that we are done with the segment and it can be archived.
        long absNextIdx = archiver0.nextAbsoluteSegmentIndex(curIdx);

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
     * Setup listener for WAL segment write File IO creation.
     * @param createWalFileListener Listener to be invoked for new segment file IO creation.
     */
    public void setCreateWalFileListener(@Nullable IgniteInClosure<FileIO> createWalFileListener) {
        this.createWalFileListener = createWalFileListener;
    }

    /**
     * @return {@link #maxWalSegmentSize}.
     */
    public long maxWalSegmentSize() {
        return maxWalSegmentSize;
    }

    /**
     * File archiver operates on absolute segment indexes. For any given absolute segment index N we can calculate the
     * work WAL segment: S(N) = N % dsCfg.walSegments. When a work segment is finished, it is given to the archiver. If
     * the absolute index of last archived segment is denoted by A and the absolute index of next segment we want to
     * write is denoted by W, then we can allow write to S(W) if W - A <= walSegments. <br>
     *
     * Monitor of current object is used for notify on: <ul> <li>exception occurred ({@link
     * FileArchiver#cleanErr}!=null)</li> <li>stopping thread ({@link FileArchiver#stopped}==true)</li> <li>current file
     * index changed ({@link FileArchiver#curAbsWalIdx})</li> <li>last archived file index was changed ({@link
     * FileArchiver#lastAbsArchivedIdx})</li> <li>some WAL index was removed from {@link FileArchiver#locked} map</li>
     * </ul>
     */
    private class FileArchiver extends GridWorker {
        /** Exception which occurred during initial creation of files or during archiving WAL segment */
        private StorageException cleanErr;

        /**
         * Absolute current segment index WAL Manager writes to. Guarded by <code>this</code>. Incremented during
         * rollover. Also may be directly set if WAL is resuming logging after start.
         */
        private long curAbsWalIdx = -1;

        /** Last archived file index (absolute, 0-based). Guarded by <code>this</code>. */
        private volatile long lastAbsArchivedIdx = -1;

        /** current thread stopping advice */
        private volatile boolean stopped;

        /** Formatted index. */
        private int formatted;

        /**
         * Maps absolute segment index to locks counter. Lock on segment protects from archiving segment and may come
         * from {@link RecordsIterator} during WAL replay. Map itself is guarded by <code>this</code>.
         */
        private Map<Long, Integer> locked = new HashMap<>();

        /**
         *
         */
        private FileArchiver(long lastAbsArchivedIdx, IgniteLogger log) {
            super(cctx.igniteInstanceName(), "wal-file-archiver%" + cctx.igniteInstanceName(), log,
                cctx.kernalContext().workersRegistry());

            this.lastAbsArchivedIdx = lastAbsArchivedIdx;
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
         * Check if WAL segment locked (protected from move to archive)
         *
         * @param absIdx Index for check reservation.
         * @return {@code True} if index is locked.
         */
        private synchronized boolean locked(long absIdx) {
            return locked.containsKey(absIdx);
        }

        /** {@inheritDoc} */
        @Override protected void body() {
            try {
                allocateRemainingFiles();
            }
            catch (StorageException e) {
                synchronized (this) {
                    // Stop the thread and report to starter.
                    cleanErr = e;

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

                        // Then increase counter to allow rollover on clean working file
                        changeLastArchivedIndexAndNotifyWaiters(toArchive);

                        notifyAll();
                    }

                    if (evt.isRecordable(EVT_WAL_SEGMENT_ARCHIVED)) {
                        evt.record(new WalSegmentArchivedEvent(
                                cctx.discovery().localNode(),
                                res.getAbsIdx(),
                                res.getDstArchiveFile())
                        );
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
                    failureProcessor.process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    failureProcessor.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * @param idx Index.
         */
        private void changeLastArchivedIndexAndNotifyWaiters(long idx) {
            lastAbsArchivedIdx = idx;

            if (compressor != null)
                compressor.onNextSegmentArchived();

            archivedMonitor.setLastArchivedAbsoluteIndex(idx);
        }

        /**
         * Gets the absolute index of the next WAL segment available to write. Blocks till there are available file to
         * write
         *
         * @param curIdx Current absolute index that we want to increment.
         * @return Next index (curWalSegmIdx+1) when it is ready to be written.
         * @throws StorageException If exception occurred in the archiver thread.
         */
        private long nextAbsoluteSegmentIndex(long curIdx) throws StorageException {
            synchronized (this) {
                if (cleanErr != null)
                    throw cleanErr;

                assert curIdx == curAbsWalIdx;

                curAbsWalIdx++;

                // Notify archiver thread.
                notifyAll();

                while (curAbsWalIdx - lastAbsArchivedIdx > dsCfg.getWalSegments() && cleanErr == null) {
                    try {
                        wait();

                        if (cleanErr != null)
                            throw cleanErr;
                    }
                    catch (InterruptedException ignore) {
                        interrupted.set(true);
                    }
                }

                // Wait for formatter so that we do not open an empty file in DEFAULT mode.
                while (curAbsWalIdx % dsCfg.getWalSegments() > formatted && cleanErr == null)
                    try {
                        wait();

                        if (cleanErr != null)
                            throw cleanErr;
                    }
                    catch (InterruptedException ignore) {
                        interrupted.set(true);
                    }

                return curAbsWalIdx;
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
         * Moves WAL segment from work folder to archive folder. Temp file is used to do movement
         *
         * @param absIdx Absolute index to archive.
         */
        private SegmentArchiveResult archiveSegment(long absIdx) throws StorageException {
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

                if (mode != WALMode.NONE) {
                    try (FileIO f0 = ioFactory.create(dstFile, CREATE, READ, WRITE)) {
                        f0.force();
                    }
                }
            }
            catch (IOException e) {
                throw new StorageException("Failed to archive WAL segment [" +
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
         * Background creation of all segments except first. First segment was created in main thread by {@link
         * FileWriteAheadLogManager#checkOrPrepareFiles()}
         */
        private void allocateRemainingFiles() throws StorageException {
            checkFiles(
                1,
                true,
                new IgnitePredicate<Integer>() {
                    @Override public boolean apply(Integer integer) {
                        return !checkStop();
                    }
                },
                new CI1<Integer>() {
                    @Override public void apply(Integer idx) {
                        synchronized (FileArchiver.this) {
                            formatted = idx;

                            FileArchiver.this.notifyAll();
                        }
                    }
                }
            );
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
                lastCompressedIdx = alreadyCompressed[alreadyCompressed.length - 1].idx();
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

                while (segmentToCompress > Math.min(lastAllowedToCompressIdx, archivedMonitor.lastArchivedAbsoluteIndex())) {
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
            FileDescriptor[] descs = scan(walArchiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER));

            Set<Long> indices = new HashSet<>();
            Set<Long> duplicateIndices = new HashSet<>();

            for (FileDescriptor desc : descs) {
                if (!indices.add(desc.idx))
                    duplicateIndices.add(desc.idx);
            }

            for (FileDescriptor desc : descs) {
                if (desc.isCompressed())
                    continue;

                // Do not delete reserved or locked segment and any segment after it.
                if (segmentReservedOrLocked(desc.idx))
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

                    File tmpZip = new File(walArchiveDir, FileDescriptor.fileName(currReservedSegment) + ".zip" + ".tmp");

                    File zip = new File(walArchiveDir, FileDescriptor.fileName(currReservedSegment) + ".zip");

                    File raw = new File(walArchiveDir, FileDescriptor.fileName(currReservedSegment));
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
                    if (currReservedSegment != -1)
                        release(new FileWALPointer(currReservedSegment, 0, 0));
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

                ByteBuffer buf = ByteBuffer.allocate(HEADER_RECORD_SIZE);
                buf.order(ByteOrder.nativeOrder());

                zos.write(prepareSerializerVersionBuffer(nextSegment, segmentSerializerVer, true, buf).array());

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
                    log, cctx, ioFactory, BUF_SIZE, nextSegment, walArchiveDir, appendToZipC)) {

                    while (iter.hasNextX())
                        iter.nextX();
                }

                RecordSerializer ser = new RecordSerializerFactoryImpl(cctx).createSerializer(segmentSerializerVer);

                ByteBuffer heapBuf = prepareSwitchSegmentRecordBuffer(nextSegment, ser);

                zos.write(heapBuf.array());
            }
        }

        /**
         * @param nextSegment Segment index.
         * @param ser Record Serializer.
         */
        @NotNull private ByteBuffer prepareSwitchSegmentRecordBuffer(long nextSegment, RecordSerializer ser)
            throws IgniteCheckedException {
            SwitchSegmentRecord switchRecord = new SwitchSegmentRecord();

            int switchRecordSize = ser.size(switchRecord);
            switchRecord.size(switchRecordSize);

            switchRecord.position(new FileWALPointer(nextSegment, 0, switchRecordSize));

            ByteBuffer heapBuf = ByteBuffer.allocate(switchRecordSize);

            ser.writeRecord(switchRecord, heapBuf);
            return heapBuf;
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
        private final PriorityBlockingQueue<Long> segmentsQueue = new PriorityBlockingQueue<>();

        /** Byte array for draining data. */
        private byte[] arr = new byte[BUF_SIZE];

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
                                "[tmp=" + unzipTmp + ", raw=" + unzip + "]", e);

                            if (!unzipTmp.delete())
                                U.error(log, "Can't delete temporary unzipped segment [tmp=" + unzipTmp + "]");
                        }

                        synchronized (this) {
                            decompressionFutures.remove(segmentToDecompress).onDone();
                        }
                    }
                    catch (IOException ex) {
                        if (!isCancelled && segmentToDecompress != -1L) {
                            IgniteCheckedException e = new IgniteCheckedException("Error during WAL segment " +
                                "decompression [segmentIdx=" + segmentToDecompress + "]", ex);

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
                    failureProcessor.process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    failureProcessor.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
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
     * Validate files depending on {@link DataStorageConfiguration#getWalSegments()}  and create if need. Check end
     * when exit condition return false or all files are passed.
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
        for (int i = startWith; i < dsCfg.getWalSegments() && (p == null || p.apply(i)); i++) {
            File checkFile = new File(walWorkDir, FileDescriptor.fileName(i));

            if (checkFile.exists()) {
                if (checkFile.isDirectory())
                    throw new StorageException("Failed to initialize WAL log segment (a directory with " +
                        "the same name already exists): " + checkFile.getAbsolutePath());
                else if (checkFile.length() != dsCfg.getWalSegmentSize() && mode == WALMode.FSYNC)
                    throw new StorageException("Failed to initialize WAL log segment " +
                        "(WAL segment size change is not supported in 'DEFAULT' WAL mode) " +
                        "[filePath=" + checkFile.getAbsolutePath() +
                        ", fileSize=" + checkFile.length() +
                        ", configSize=" + dsCfg.getWalSegments() + ']');
            }
            else if (create)
                createFile(checkFile);

            if (completionCallback != null)
                completionCallback.apply(i);
        }
    }

    /**
     * Needs only for WAL compaction.
     *
     * @param idx Index.
     * @param ver Version.
     * @param compacted Compacted flag.
     */
    @NotNull private static ByteBuffer prepareSerializerVersionBuffer(long idx, int ver, boolean compacted, ByteBuffer buf) {
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
    public static class FileDescriptor implements
        Comparable<FileDescriptor>, AbstractFileDescriptor {
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

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull FileDescriptor o) {
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
         * @return True if segment is ZIP compressed.
         */
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
        FileIO fileIO;

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
         * <code>true</code> if this file handle came from work directory. <code>false</code> if this file handle came
         * from archive directory.
         */
        private boolean workDir;

        /**
         * @param fileIO I/O interface for read/write operations of FileHandle.
         * @param idx Absolute WAL segment file index (incremental counter).
         * @param ser Entry serializer.
         * @param in File input.
         */
        public ReadFileHandle(
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

        /** Created on resume logging. */
        private volatile boolean resume;

        /**
         * Position in current file after the end of last written record (incremented after file channel write
         * operation)
         */
        volatile long written;

        /** */
        private volatile long lastFsyncPos;

        /** Stop guard to provide warranty that only one thread will be successful in calling {@link #close(boolean)} */
        private final AtomicBoolean stop = new AtomicBoolean(false);

        /** */
        private final Lock lock = new ReentrantLock();

        /** Condition for timed wait of several threads, see {@link DataStorageConfiguration#getWalFsyncDelayNanos()} */
        private final Condition fsync = lock.newCondition();

        /**
         * Next segment available condition. Protection from "spurious wakeup" is provided by predicate {@link
         * #fileIO}=<code>null</code>
         */
        private final Condition nextSegment = lock.newCondition();

        /** Buffer. */
        private final SegmentedRingByteBuffer buf;

        /**
         * @param fileIO I/O file interface to use
         * @param idx Absolute WAL segment file index for easy access.
         * @param pos Position.
         * @param resume Created on resume logging flag.
         * @param serializer Serializer.
         * @param buf Buffer.
         * @throws IOException If failed.
         */
        private FileWriteHandle(
            FileIO fileIO,
            long idx,
            long pos,
            boolean resume,
            RecordSerializer serializer,
            SegmentedRingByteBuffer buf
        ) throws IOException {
            super(fileIO, idx);

            assert serializer != null;

            if (!mmap)
                fileIO.position(pos);

            this.serializer = serializer;

            written = pos;
            lastFsyncPos = pos;
            this.resume = resume;
            this.buf = buf;
        }


        /**
         * Write serializer version to current handle.
         */
        public void writeHeader() {
            SegmentedRingByteBuffer.WriteSegment seg = buf.offer(HEADER_RECORD_SIZE);

            assert seg != null && seg.position() > 0;

            prepareSerializerVersionBuffer(idx, serializerVersion(), false, seg.buffer());

            seg.release();
        }

        /**
         * @param rec Record to be added to write queue.
         * @return Pointer or null if roll over to next segment is required or already started by other thread.
         * @throws StorageException If failed.
         * @throws IgniteCheckedException If failed.
         */
        @Nullable private WALPointer addRecord(WALRecord rec) throws StorageException, IgniteCheckedException {
            assert rec.size() > 0 : rec;

            for (;;) {
                checkNode();

                SegmentedRingByteBuffer.WriteSegment seg;

                // Buffer can be in open state in case of resuming with different serializer version.
                if (rec.type() == SWITCH_SEGMENT_RECORD && !currHnd.resume)
                    seg = buf.offerSafe(rec.size());
                else
                    seg = buf.offer(rec.size());

                FileWALPointer ptr = null;

                if (seg != null) {
                    try {
                        int pos = (int)(seg.position() - rec.size());

                        ByteBuffer buf = seg.buffer();

                        if (buf == null)
                            return null; // Can not write to this segment, need to switch to the next one.

                        ptr = new FileWALPointer(idx, pos, rec.size());

                        rec.position(ptr);

                        fillBuffer(buf, rec);

                        if (mmap) {
                            // written field must grow only, but segment with greater position can be serialized
                            // earlier than segment with smaller position.
                            while (true) {
                                long written0 = written;

                                if (seg.position() > written0) {
                                    if (WRITTEN_UPD.compareAndSet(this, written0, seg.position()))
                                        break;
                                }
                                else
                                    break;
                            }
                        }

                        return ptr;
                    }
                    finally {
                        seg.release();

                        if (mode == WALMode.BACKGROUND && rec instanceof CheckpointRecord)
                            flushOrWait(ptr);
                    }
                }
                else
                    walWriter.flushAll();
            }
        }

        /**
         * Flush or wait for concurrent flush completion.
         *
         * @param ptr Pointer.
         */
        private void flushOrWait(FileWALPointer ptr) {
            if (ptr != null) {
                // If requested obsolete file index, it must be already flushed by close.
                if (ptr.index() != idx)
                    return;
            }

            flush(ptr);
        }

        /**
         * @param ptr Pointer.
         */
        private void flush(FileWALPointer ptr) {
            if (ptr == null) { // Unconditional flush.
                walWriter.flushAll();

                return;
            }

            assert ptr.index() == idx;

            walWriter.flushBuffer(ptr.fileOffset());
        }

        /**
         * @param buf Buffer.
         * @param rec WAL record.
         * @throws IgniteCheckedException If failed.
         */
        private void fillBuffer(ByteBuffer buf, WALRecord rec) throws IgniteCheckedException {
            try {
                serializer.writeRecord(rec, buf);
            }
            catch (RuntimeException e) {
                throw new IllegalStateException("Failed to write record: " + rec, e);
            }
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
        private void fsync(FileWALPointer ptr) throws StorageException, IgniteCheckedException {
            lock.lock();

            try {
                if (ptr != null) {
                    if (!needFsync(ptr))
                        return;

                    if (fsyncDelay > 0 && !stop.get()) {
                        // Delay fsync to collect as many updates as possible: trade latency for throughput.
                        U.await(fsync, fsyncDelay, TimeUnit.NANOSECONDS);

                        if (!needFsync(ptr))
                            return;
                    }
                }

                flushOrWait(ptr);

                if (stop.get())
                    return;

                long lastFsyncPos0 = lastFsyncPos;
                long written0 = written;

                if (lastFsyncPos0 != written0) {
                    // Fsync position must be behind.
                    assert lastFsyncPos0 < written0 : "lastFsyncPos=" + lastFsyncPos0 + ", written=" + written0;

                    boolean metricsEnabled = metrics.metricsEnabled();

                    long start = metricsEnabled ? System.nanoTime() : 0;

                    if (mmap) {
                        long pos = ptr == null ? -1 : ptr.fileOffset();

                        List<SegmentedRingByteBuffer.ReadSegment> segs = buf.poll(pos);

                        if (segs != null) {
                            assert segs.size() == 1;

                            SegmentedRingByteBuffer.ReadSegment seg = segs.get(0);

                            int off = seg.buffer().position();
                            int len = seg.buffer().limit() - off;

                            fsync((MappedByteBuffer)buf.buf, off, len);

                            seg.release();
                        }
                    }
                    else
                        walWriter.force();

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
         * @param buf Mapped byte buffer..
         * @param off Offset.
         * @param len Length.
         */
        private void fsync(MappedByteBuffer buf, int off, int len) throws IgniteCheckedException {
            try {
                long mappedOff = (Long)mappingOffset.invoke(buf);

                assert mappedOff == 0 : mappedOff;

                long addr = (Long)mappingAddress.invoke(buf, mappedOff);

                long delta = (addr + off) % PAGE_SIZE;

                long alignedAddr = (addr + off) - delta;

                force0.invoke(buf, fd.get(buf), alignedAddr, len + delta);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IgniteCheckedException(e);
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
                    flushOrWait(null);

                    try {
                        RecordSerializer backwardSerializer = new RecordSerializerFactoryImpl(cctx)
                            .createSerializer(serializerVer);

                        SwitchSegmentRecord segmentRecord = new SwitchSegmentRecord();

                        int switchSegmentRecSize = backwardSerializer.size(segmentRecord);

                        if (rollOver && written < (maxWalSegmentSize - switchSegmentRecSize)) {
                            segmentRecord.size(switchSegmentRecSize);

                            WALPointer segRecPtr = addRecord(segmentRecord);

                            if (segRecPtr != null)
                                fsync((FileWALPointer)segRecPtr);
                        }

                        if (mmap) {
                            List<SegmentedRingByteBuffer.ReadSegment> segs = buf.poll(maxWalSegmentSize);

                            if (segs != null) {
                                assert segs.size() == 1;

                                segs.get(0).release();
                            }
                        }

                        // Do the final fsync.
                        if (mode != WALMode.NONE) {
                            if (mmap)
                                ((MappedByteBuffer)buf.buf).force();
                            else
                                fileIO.force();

                            lastFsyncPos = written;
                        }

                        if (mmap) {
                            try {
                                fileIO.close();
                            }
                            catch (IOException ignore) {
                                // No-op.
                            }
                        }
                        else {
                            walWriter.close();

                            if (!rollOver)
                                buf.free();
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
                    if (mmap)
                        buf.free();

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
                assert cctx.kernalContext().invalid() ||
                    written == lastFsyncPos || mode != WALMode.FSYNC :
                    "fsync [written=" + written + ", lastFsync=" + lastFsyncPos + ", idx=" + idx + ']';

                fileIO = null;

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
                while (fileIO != null)
                    U.awaitQuiet(nextSegment);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @return Safely reads current position of the file channel as String. Will return "null" if channel is null.
         */
        private String safePosition() {
            FileIO io = fileIO;

            if (io == null)
                return "null";

            try {
                return String.valueOf(io.position());
            }
            catch (IOException e) {
                return "{Failed to read channel position: " + e.getMessage() + '}';
            }
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

        /** See {@link FileWriteAheadLogManager#archiver}. */
        @Nullable private final FileArchiver archiver;

        /** */
        private final FileDecompressor decompressor;

        /** */
        private final DataStorageConfiguration dsCfg;

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
         * @param dsCfg Database configuration.
         * @param serializerFactory Serializer factory.
         * @param archiver File Archiver.
         * @param decompressor Decompressor.
         *@param log Logger  @throws IgniteCheckedException If failed to initialize WAL segment.
         */
        private RecordsIterator(
            GridCacheSharedContext cctx,
            File walWorkDir,
            File walArchiveDir,
            @Nullable FileWALPointer start,
            @Nullable FileWALPointer end,
            DataStorageConfiguration dsCfg,
            @NotNull RecordSerializerFactory serializerFactory,
            FileIOFactory ioFactory,
            @Nullable FileArchiver archiver,
            FileDecompressor decompressor,
            IgniteLogger log
        ) throws IgniteCheckedException {
            super(log,
                cctx,
                serializerFactory,
                ioFactory,
                dsCfg.getWalRecordIteratorBufferSize());
            this.walWorkDir = walWorkDir;
            this.walArchiveDir = walArchiveDir;
            this.dsCfg = dsCfg;
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

            if (archiver == null || readArchive) {
                fd = new FileDescriptor(new File(walArchiveDir,
                    FileDescriptor.fileName(curWalSegmIdx)));
            }
            else {
                long workIdx = curWalSegmIdx % dsCfg.getWalSegments();

                fd = new FileDescriptor(
                    new File(walWorkDir, FileDescriptor.fileName(workIdx)),
                    curWalSegmIdx);
            }

            if (log.isDebugEnabled())
                log.debug("Reading next file [absIdx=" + curWalSegmIdx + ", file=" + fd.file.getAbsolutePath() + ']');

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
     * Flushes current file handle for {@link WALMode#BACKGROUND} WALMode. Called periodically from scheduler.
     */
    private void doFlush() {
        FileWriteHandle hnd = currentHandle();

        try {
            hnd.flush(null);
        }
        catch (Exception e) {
            U.warn(log, "Failed to flush WAL record queue", e);
        }
    }

    /**
     * WAL writer worker.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private class WALWriter extends GridWorker {
        /** Unconditional flush. */
        private static final long UNCONDITIONAL_FLUSH = -1L;

        /** File close. */
        private static final long FILE_CLOSE = -2L;

        /** File force. */
        private static final long FILE_FORCE = -3L;

        /** Err. */
        private volatile Throwable err;

        //TODO: replace with GC free data structure.
        /** Parked threads. */
        final Map<Thread, Long> waiters = new ConcurrentHashMap<>();

        /**
         * Default constructor.
         *
         * @param log Logger.
         */
        WALWriter(IgniteLogger log) {
            super(cctx.igniteInstanceName(), "wal-write-worker%" + cctx.igniteInstanceName(), log,
                cctx.kernalContext().workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() {
            Throwable err = null;

            try {
                while (!isCancelled()) {
                    while (waiters.isEmpty()) {
                        if (!isCancelled())
                            LockSupport.park();
                        else {
                            unparkWaiters(Long.MAX_VALUE);

                            return;
                        }
                    }

                    Long pos = null;

                    for (Long val : waiters.values()) {
                        if (val > Long.MIN_VALUE)
                            pos = val;
                    }

                    if (pos == null)
                        continue;
                    else if (pos < UNCONDITIONAL_FLUSH) {
                        try {
                            assert pos == FILE_CLOSE || pos == FILE_FORCE : pos;

                            if (pos == FILE_CLOSE)
                                currHnd.fileIO.close();
                            else if (pos == FILE_FORCE)
                                currHnd.fileIO.force();
                        }
                        catch (IOException e) {
                            log.error("Exception in WAL writer thread: ", e);

                            err = e;

                            unparkWaiters(Long.MAX_VALUE);

                            return;
                        }

                        unparkWaiters(pos);
                    }

                    List<SegmentedRingByteBuffer.ReadSegment> segs = currentHandle().buf.poll(pos);

                    if (segs == null) {
                        unparkWaiters(pos);

                        continue;
                    }

                    for (int i = 0; i < segs.size(); i++) {
                        SegmentedRingByteBuffer.ReadSegment seg = segs.get(i);

                        try {
                            writeBuffer(seg.position(), seg.buffer());
                        }
                        catch (Throwable e) {
                            log.error("Exception in WAL writer thread:", e);

                            err = e;
                        }
                        finally {
                            seg.release();

                            long p = pos <= UNCONDITIONAL_FLUSH || err != null ? Long.MAX_VALUE : currentHandle().written;

                            unparkWaiters(p);
                        }
                    }
                }
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                unparkWaiters(Long.MAX_VALUE);

                if (err == null && !isCancelled)
                    err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * Shutdowns thread.
         */
        public void shutdown() throws IgniteInterruptedCheckedException {
            U.cancel(this);

            LockSupport.unpark(runner());

            U.join(runner());
        }

        /**
         * Unparks waiting threads.
         *
         * @param pos Pos.
         */
        private void unparkWaiters(long pos) {
            assert pos > Long.MIN_VALUE : pos;

            for (Map.Entry<Thread, Long> e : waiters.entrySet()) {
                Long val = e.getValue();

                if (val <= pos) {
                    if (val != Long.MIN_VALUE)
                        waiters.put(e.getKey(), Long.MIN_VALUE);

                    LockSupport.unpark(e.getKey());
                }
            }
        }

        /**
         * Forces all made changes to the file.
         */
        void force() {
            flushBuffer(FILE_FORCE);
        }

        /**
         * Closes file.
         */
        void close() {
            flushBuffer(FILE_CLOSE);
        }

        /**
         * Flushes all data from the buffer.
         */
        void flushAll() {
            flushBuffer(UNCONDITIONAL_FLUSH);
        }

        /**
         * @param expPos Expected position.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        void flushBuffer(long expPos) {
            if (mmap)
                return;

            Throwable err = walWriter.err;

            if (err != null)
                cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));

            if (expPos == UNCONDITIONAL_FLUSH)
                expPos = (currentHandle().buf.tail());

            Thread t = Thread.currentThread();

            waiters.put(t, expPos);

            LockSupport.unpark(walWriter.runner());

            while (true) {
                Long val = waiters.get(t);

                assert val != null : "Only this thread can remove thread from waiters";

                if (val == Long.MIN_VALUE) {
                    waiters.remove(t);

                    return;
                }
                else
                    LockSupport.park();
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
            FileWriteHandle hdl = currentHandle();

            assert hdl.fileIO != null : "Writing to a closed segment.";

            checkNode();

            long lastLogged = U.currentTimeMillis();

            long logBackoff = 2_000;

            // If we were too fast, need to wait previous writes to complete.
            while (hdl.written != pos) {
                assert hdl.written < pos : "written = " + hdl.written + ", pos = " + pos; // No one can write further than we are now.

                // Permutation occurred between blocks write operations.
                // Order of acquiring lock is not the same as order of write.
                long now = U.currentTimeMillis();

                if (now - lastLogged >= logBackoff) {
                    if (logBackoff < 60 * 60_000)
                        logBackoff *= 2;

                    U.warn(log, "Still waiting for a concurrent write to complete [written=" + hdl.written +
                        ", pos=" + pos + ", lastFsyncPos=" + hdl.lastFsyncPos + ", stop=" + hdl.stop.get() +
                        ", actualPos=" + hdl.safePosition() + ']');

                    lastLogged = now;
                }

                checkNode();
            }

            // Do the write.
            int size = buf.remaining();

            assert size > 0 : size;

            try {
                assert hdl.written == hdl.fileIO.position();

                hdl.written += hdl.fileIO.writeFully(buf);

                metrics.onWalBytesWritten(size);

                assert hdl.written == hdl.fileIO.position();
            }
            catch (IOException e) {
                StorageException se = new StorageException("Failed to write buffer.", e);

                cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, se));

                throw se;
            }
        }
    }

    /**
     * Syncs WAL segment file.
     */
    private class WalSegmentSyncer extends GridWorker {
        /** Sync timeout. */
        long syncTimeout;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param log Logger.
         */
        public WalSegmentSyncer(String igniteInstanceName, IgniteLogger log) {
            super(igniteInstanceName, "wal-segment-syncer", log);

            syncTimeout = Math.max(IgniteSystemProperties.getLong(IGNITE_WAL_SEGMENT_SYNC_TIMEOUT,
                DFLT_WAL_SEGMENT_SYNC_TIMEOUT), 100L);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                sleep(syncTimeout);

                try {
                    flush(null, true);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Exception when flushing WAL.", e);
                }
            }
        }

        /** Shutted down the worker. */
        private void shutdown() {
            synchronized (this) {
                U.cancel(this);
            }

            U.join(this, log);
        }
    }

    /**
     * Scans provided folder for a WAL segment files
     * @param walFilesDir directory to scan
     * @return found WAL file descriptors
     */
    public static FileDescriptor[] loadFileDescriptors(@NotNull final File walFilesDir) throws IgniteCheckedException {
        final File[] files = walFilesDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

        if (files == null) {
            throw new IgniteCheckedException("WAL files directory does not not denote a " +
                "directory, or if an I/O error occurs: [" + walFilesDir.getAbsolutePath() + "]");
        }
        return scan(files);
    }
}
