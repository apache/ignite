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
import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.WalSegmentArchivedEvent;
import org.apache.ignite.events.WalSegmentCompactedEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.RolloverType;
import org.apache.ignite.internal.pagemem.wal.record.SwitchSegmentRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.WalStateManager.WALDisableContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.persistence.wal.filehandle.AbstractFileHandle;
import org.apache.ignite.internal.processors.cache.persistence.wal.filehandle.FileHandleManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.filehandle.FileHandleManagerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.filehandle.FileWriteHandle;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.LockedSegmentFileInputFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentFileInputFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SegmentIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.SimpleSegmentFileInputFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.io.GridFileUtils;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECKPOINT_TRIGGER_ARCHIVE_SIZE_PERCENTAGE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_THRESHOLD_WAIT_TIME_NEXT_WAL_SEGMENT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_COMPRESSOR_WORKER_THREAD_CNT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_MMAP;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SERIALIZER_VERSION;
import static org.apache.ignite.IgniteSystemProperties.getDouble;
import static org.apache.ignite.configuration.DataStorageConfiguration.HALF_MAX_WAL_ARCHIVE_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_ARCHIVED;
import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_COMPACTED;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.TMP_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.ZIP_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor.fileName;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory.LATEST_SERIALIZER_VERSION;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readPosition;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readSegmentHeader;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.checkCompressionLevelBounds;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.getDefaultCompressionLevel;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.util.io.GridFileUtils.ensureHardLinkAvailable;

/**
 * File WAL manager.
 */
@SuppressWarnings("IfMayBeConditional")
public class FileWriteAheadLogManager extends GridCacheSharedManagerAdapter implements IgniteWriteAheadLogManager {
    /** */
    private static final FileDescriptor[] EMPTY_DESCRIPTORS = new FileDescriptor[0];

    /** Zero-filled buffer for file formatting. */
    private static final byte[] FILL_BUF = new byte[1024 * 1024];

    /** Pattern for segment file names. */
    public static final Pattern WAL_NAME_PATTERN = Pattern.compile("\\d{16}\\.wal");

    /** Pattern for WAL temp files - these files will be cleared at startup. */
    public static final Pattern WAL_TEMP_NAME_PATTERN = Pattern.compile("\\d{16}\\.wal\\.tmp");

    /** WAL segment file filter, see {@link #WAL_NAME_PATTERN} */
    public static final FileFilter WAL_SEGMENT_FILE_FILTER = file -> !file.isDirectory() &&
        WAL_NAME_PATTERN.matcher(file.getName()).matches();

    /** WAL segment temporary file filter, see {@link #WAL_TEMP_NAME_PATTERN} */
    private static final FileFilter WAL_SEGMENT_TEMP_FILE_FILTER = file -> !file.isDirectory() &&
        WAL_TEMP_NAME_PATTERN.matcher(file.getName()).matches();

    /** */
    public static final Pattern WAL_SEGMENT_FILE_COMPACTED_PATTERN = Pattern.compile("\\d{16}\\.wal\\.zip");

    /** WAL segment file filter, see {@link #WAL_NAME_PATTERN} */
    public static final FileFilter WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER = file -> !file.isDirectory() &&
        (WAL_NAME_PATTERN.matcher(file.getName()).matches() ||
            WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches());

    /** */
    private static final Pattern WAL_SEGMENT_TEMP_FILE_COMPACTED_PATTERN = Pattern.compile("\\d{16}\\.wal\\.zip\\.tmp");

    /** */
    private static final FileFilter WAL_SEGMENT_FILE_COMPACTED_FILTER = file -> !file.isDirectory() &&
        WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches();

    /** */
    private static final FileFilter WAL_SEGMENT_TEMP_FILE_COMPACTED_FILTER = file -> !file.isDirectory() &&
        WAL_SEGMENT_TEMP_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches();

    /** Buffer size. */
    private static final int BUF_SIZE = 1024 * 1024;

    /** @see IgniteSystemProperties#IGNITE_WAL_MMAP */
    public static final boolean DFLT_WAL_MMAP = true;

    /** @see IgniteSystemProperties#IGNITE_WAL_COMPRESSOR_WORKER_THREAD_CNT */
    public static final int DFLT_WAL_COMPRESSOR_WORKER_THREAD_CNT = 4;

    /** @see IgniteSystemProperties#IGNITE_CHECKPOINT_TRIGGER_ARCHIVE_SIZE_PERCENTAGE */
    public static final double DFLT_CHECKPOINT_TRIGGER_ARCHIVE_SIZE_PERCENTAGE = 0.25;

    /** @see IgniteSystemProperties#IGNITE_THRESHOLD_WAIT_TIME_NEXT_WAL_SEGMENT */
    public static final long DFLT_THRESHOLD_WAIT_TIME_NEXT_WAL_SEGMENT = 1000L;

    /** CDC disabled distributed property name. */
    public static final String CDC_DISABLED = "cdc.disabled";

    /** Use mapped byte buffer. */
    private final boolean mmap = IgniteSystemProperties.getBoolean(IGNITE_WAL_MMAP, DFLT_WAL_MMAP);

    /**
     * Number of WAL compressor worker threads.
     */
    private final int WAL_COMPRESSOR_WORKER_THREAD_CNT =
            IgniteSystemProperties.getInteger(IGNITE_WAL_COMPRESSOR_WORKER_THREAD_CNT,
                DFLT_WAL_COMPRESSOR_WORKER_THREAD_CNT);

    /**
     * Threshold time to print warning to log if awaiting for next wal segment took too long (exceeded this threshold).
     */
    private static final long THRESHOLD_WAIT_TIME_NEXT_WAL_SEGMENT =
        IgniteSystemProperties.getLong(IGNITE_THRESHOLD_WAIT_TIME_NEXT_WAL_SEGMENT,
            DFLT_THRESHOLD_WAIT_TIME_NEXT_WAL_SEGMENT);

    /** */
    private final boolean alwaysWriteFullPages;

    /** WAL segment size in bytes. This is maximum value, actual segments may be shorter. */
    private final long maxWalSegmentSize;

    /**
     * Maximum number of allowed segments without checkpoint. If we have their more checkpoint should be triggered.
     * It is simple way to calculate WAL size without checkpoint instead fair WAL size calculating.
     */
    private final long maxSegCountWithoutCheckpoint;

    /** Maximum size of the WAL archive in bytes. */
    private final long maxWalArchiveSize;

    /** Minimum size of the WAL archive in bytes. */
    private final long minWalArchiveSize;

    /** */
    private final WALMode mode;

    /** WAL flush frequency. Makes sense only for {@link WALMode#BACKGROUND} log WALMode. */
    private final long flushFreq;

    /** */
    private final DataStorageConfiguration dsCfg;

    /** Events service */
    private final GridEventStorageManager evt;

    /** Failure processor */
    private final FailureProcessor failureProcessor;

    /** Ignite configuration. */
    private final IgniteConfiguration igCfg;

    /** Persistence metrics tracker. */
    private DataStorageMetricsImpl metrics;

    /** WAL work directory (including consistent ID as subfolder). */
    private File walWorkDir;

    /** WAL archive directory (including consistent ID as subfolder). */
    private File walArchiveDir;

    /** WAL cdc directory (including consistent ID as subfolder) */
    private File walCdcDir;

    /** Serializer of latest version, used to read header record and for write records */
    private RecordSerializer serializer;

    /** Serializer latest version to use. */
    private final int serializerVer =
        IgniteSystemProperties.getInteger(IGNITE_WAL_SERIALIZER_VERSION, LATEST_SERIALIZER_VERSION);

    /** Factory to provide I/O interfaces for read/write operations with files */
    private volatile FileIOFactory ioFactory;

    /** Factory to provide I/O interfaces for read primitives with files */
    private final SegmentFileInputFactory segmentFileInputFactory;

    /** Holder of actual information of latest manipulation on WAL segments. */
    private volatile SegmentAware segmentAware;

    /** Updater for {@link #currHnd}, used for verify there are no concurrent update for current log segment handle. */
    private static final AtomicReferenceFieldUpdater<FileWriteAheadLogManager, FileWriteHandle> CURR_HND_UPD =
        AtomicReferenceFieldUpdater.newUpdater(FileWriteAheadLogManager.class, FileWriteHandle.class, "currHnd");

    /**
     * File archiver moves segments from work directory to archive. Locked segments may be kept not moved until release.
     * For mode archive and work folders set to equal value, archiver is not created.
     */
    @Nullable private FileArchiver archiver;

    /** Compressor. */
    @Nullable private FileCompressor compressor;

    /** Decompressor. */
    @Nullable private FileDecompressor decompressor;

    /**
     * Cleaner of segments from WAL archive when the maximum size is reached.
     * Will not work if WAL archive size is {@link DataStorageConfiguration#UNLIMITED_WAL_ARCHIVE}.
     */
    @Nullable private FileCleaner cleaner;

    /** Current log segment handle. */
    private volatile FileWriteHandle currHnd;

    /** File handle manager. */
    private FileHandleManager fileHandleManager;

    /** */
    private WALDisableContext walDisableContext;

    /**
     * Positive (non-0) value indicates WAL can be archived even if not complete<br>
     * See {@link DataStorageConfiguration#setWalAutoArchiveAfterInactivity(long)}<br>
     */
    private final long walAutoArchiveAfterInactivity;

    /** Positive (non-0) value indicates WAL must be archived even if not complete. */
    private final long walForceArchiveTimeout;

    /**
     * {@code True} if WAL enabled only for CDC.
     * This mean {@link DataRegionConfiguration#isPersistenceEnabled()} is {@code false} for all {@link DataRegion},
     * and {@link DataRegionConfiguration#isCdcEnabled()} {@code true} for some of them.
     */
    private final boolean inMemoryCdc;

    /**
     * Container with last WAL record logged timestamp.<br> Zero value means there was no records logged to current
     * segment, skip possible archiving for this case<br> Value is filled only for case {@link
     * #walAutoArchiveAfterInactivity} > 0<br>
     */
    private final AtomicLong lastRecordLoggedMs = new AtomicLong();

    /**
     * Container with last {@link DataRecord} logged timestamp.<br> Zero value means there was no records logged to current
     * segment, skip possible archiving for this case<br> Value is filled only for case {@link
     * #walAutoArchiveAfterInactivity} > 0
     */
    private final AtomicLong lastDataRecordLoggedMs = new AtomicLong();

    /**
     * Cancellable task for {@link WALMode#BACKGROUND}, should be cancelled at shutdown.
     * Null for non background modes.
     */
    @Nullable private volatile GridTimeoutProcessor.CancelableTask backgroundFlushSchedule;

    /** Reference to the last added next timeout rollover object. */
    @Nullable private TimeoutRollover timeoutRollover;

    /** Timeout rollover mutex. */
    @Nullable private final Object timeoutRolloverMux;

    /**
     * Listener invoked for each segment file IO initializer.
     */
    @Nullable private volatile IgniteInClosure<FileIO> createWalFileListener;

    /**
     * Manage of segment location.
     */
    private SegmentRouter segmentRouter;

    /** Segment factory with ability locked segment during reading. */
    private SegmentFileInputFactory lockedSegmentFileInputFactory;

    /** FileHandleManagerFactory. */
    private final FileHandleManagerFactory fileHandleManagerFactory;

    /** Switch segment record offset. */
    @Nullable private final AtomicLongArray switchSegmentRecordOffset;

    /** Page snapshot records compression algorithm. */
    private DiskPageCompression pageCompression;

    /** Page snapshot records compression level. */
    private int pageCompressionLevel;

    /**
     * Local segment sizes: absolute segment index -> size in bytes.
     * For segments from {@link #walWorkDir} and {@link #walArchiveDir}.
     * If there is a raw and compressed segment, compressed size is getting.
     */
    private final Map<Long, Long> segmentSize = new ConcurrentHashMap<>();

    /** Pointer to the last successful checkpoint until which WAL segments can be safely deleted. */
    private volatile WALPointer lastCheckpointPtr = new WALPointer(0, 0, 0);

    /** CDC disabled flag. */
    private final DistributedBooleanProperty cdcDisabled = detachedBooleanProperty(CDC_DISABLED);

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public FileWriteAheadLogManager(final GridKernalContext ctx) {
        igCfg = ctx.config();

        DataStorageConfiguration dsCfg = igCfg.getDataStorageConfiguration();

        assert dsCfg != null;

        this.dsCfg = dsCfg;

        maxWalSegmentSize = dsCfg.getWalSegmentSize();
        mode = dsCfg.getWalMode();
        flushFreq = dsCfg.getWalFlushFrequency();
        alwaysWriteFullPages = dsCfg.isAlwaysWriteFullPages();
        ioFactory = mode == WALMode.FSYNC ? dsCfg.getFileIOFactory() : new RandomAccessFileIOFactory();
        segmentFileInputFactory = new SimpleSegmentFileInputFactory();
        walAutoArchiveAfterInactivity = dsCfg.getWalAutoArchiveAfterInactivity();
        walForceArchiveTimeout = dsCfg.getWalForceArchiveTimeout();
        inMemoryCdc = !CU.isPersistenceEnabled(dsCfg) && CU.isCdcEnabled(igCfg);

        timeoutRolloverMux = (walAutoArchiveAfterInactivity > 0 || walForceArchiveTimeout > 0) ? new Object() : null;

        maxWalArchiveSize = dsCfg.getMaxWalArchiveSize();

        minWalArchiveSize = minWalArchiveSize(dsCfg);

        evt = ctx.event();
        failureProcessor = ctx.failure();

        fileHandleManagerFactory = new FileHandleManagerFactory(dsCfg);

        double cpTriggerArchiveSizePercentage = getDouble(
            IGNITE_CHECKPOINT_TRIGGER_ARCHIVE_SIZE_PERCENTAGE, DFLT_CHECKPOINT_TRIGGER_ARCHIVE_SIZE_PERCENTAGE);

        maxSegCountWithoutCheckpoint = (long)((U.adjustedWalHistorySize(dsCfg, log) * cpTriggerArchiveSizePercentage)
            / dsCfg.getWalSegmentSize());

        switchSegmentRecordOffset = isArchiverEnabled() ? new AtomicLongArray(dsCfg.getWalSegments()) : null;
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
        if (cctx.kernalContext().clientNode())
            return;

        final PdsFolderSettings resolveFolders = cctx.kernalContext().pdsFolderResolver().resolveFolders();

        checkWalConfiguration();

        synchronized (this) {
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

            if (CU.isCdcEnabled(igCfg)) {
                walCdcDir = initDirectory(
                    dsCfg.getCdcWalPath(),
                    DataStorageConfiguration.DFLT_WAL_CDC_PATH,
                    resolveFolders.folderName(),
                    "change data capture directory"
                );

                ensureHardLinkAvailable(walArchiveDir.toPath(), walCdcDir.toPath());

                cctx.kernalContext().internalSubscriptionProcessor()
                    .registerDistributedConfigurationListener(dispatcher -> {
                        cdcDisabled.addListener((name, oldVal, newVal) -> {
                            if (log.isInfoEnabled()) {
                                log.info(format("Distributed property '%s' was changed from '%s' to '%s'.",
                                    name, oldVal, newVal));
                            }

                            if (newVal != null && newVal)
                                log.warning("CDC was disabled.");
                        });

                        dispatcher.registerProperty(cdcDisabled);
                    });
            }

            serializer = new RecordSerializerFactoryImpl(cctx).createSerializer(serializerVer);

            IgniteCacheDatabaseSharedManager dbMgr = cctx.database();

            metrics = dbMgr.dataStorageMetricsImpl();

            if (metrics != null) {
                metrics.setWalSizeProvider(new CO<Long>() {
                    /** {@inheritDoc} */
                    @Override public Long apply() {
                        long size = 0;

                        for (File f : walWorkDir0.listFiles())
                            size += f.length();

                        if (isArchiverEnabled()) {
                            for (File f : walArchiveDir0.listFiles())
                                size += f.length();
                        }

                        return size;
                    }
                });
            }

            segmentAware = new SegmentAware(
                log,
                dsCfg.getWalSegments(),
                dsCfg.isWalCompactionEnabled(),
                minWalArchiveSize,
                maxWalArchiveSize
            );

            // We have to initialize compressor before archiver in order to setup already compressed segments.
            // Otherwise, FileArchiver initialization will trigger redundant work for FileCompressor.
            if (dsCfg.isWalCompactionEnabled()) {
                compressor = new FileCompressor(log);

                decompressor = new FileDecompressor(log);
            }

            if (isArchiverEnabled())
                archiver = new FileArchiver(log);

            if (!walArchiveUnlimited())
                cleaner = new FileCleaner(log);

            prepareAndCheckWalFiles();

            if (compressor != null)
                compressor.initAlreadyCompressedSegments();

            if (archiver != null)
                archiver.init(segmentAware);

            segmentRouter = new SegmentRouter(walWorkDir, walArchiveDir, segmentAware, dsCfg);

            fileHandleManager = fileHandleManagerFactory.build(
                cctx, metrics, mmap, serializer, this::currentHandle
            );

            lockedSegmentFileInputFactory = new LockedSegmentFileInputFactory(
                segmentAware,
                segmentRouter,
                ioFactory
            );

            pageCompression = dsCfg.getWalPageCompression();

            if (pageCompression != DiskPageCompression.DISABLED) {
                if (serializerVer < 2) {
                    throw new IgniteCheckedException("WAL page snapshots compression not supported for serializerVer=" +
                        serializerVer);
                }

                cctx.kernalContext().compress().checkPageCompressionSupported();

                pageCompressionLevel = dsCfg.getWalPageCompressionLevel() != null ?
                    checkCompressionLevelBounds(dsCfg.getWalPageCompressionLevel(), pageCompression) :
                    getDefaultCompressionLevel(pageCompression);
            }
        }
    }

    /**
     * @return Info about of WAL paths.
     */
    public SegmentRouter getSegmentRouter() {
        return segmentRouter;
    }

    /**
     * Running workers of WAL archive.
     */
    private void startArchiveWorkers() {
        segmentAware.reset();

        segmentAware.resetWalArchiveSizes();

        for (FileDescriptor descriptor : walArchiveFiles())
            segmentAware.addSize(descriptor.idx, descriptor.file.length());

        if (isArchiverEnabled()) {
            assert archiver != null : "FileArchiver should be initialized.";

            archiver.restart();
        }

        if (dsCfg.isWalCompactionEnabled() && !cctx.kernalContext().recoveryMode()) {
            assert compressor != null : "Compressor should be initialized.";

            compressor.restart();

            assert decompressor != null : "Compressor should be initialized.";

            decompressor.restart();
        }

        if (!walArchiveUnlimited()) {
            assert cleaner != null : "FileCleaner should be initialized.";

            cleaner.restart();
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
     * Collects WAL segments from the archive only if they are all present.
     * Will wait for the last segment to be archived if it is not.
     * If there are missing segments an empty collection is returned.
     *
     * @param low Low bound (include).
     * @param high High bound (not include).
     * @return WAL segments from the archive, or an empty collection if at
     *      least a segment between {@code low} and {@code high} is missing.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<File> getWalFilesFromArchive(
        WALPointer low,
        WALPointer high
    ) throws IgniteCheckedException {
        segmentAware.awaitSegmentArchived(high.index() - 1);

        List<File> res = new ArrayList<>();

        for (long i = low.index(); i < high.index(); i++) {
            String segmentName = fileName(i);

            File file = new File(walArchiveDir, segmentName);
            File fileZip = new File(walArchiveDir, segmentName + ZIP_SUFFIX);

            if (file.exists())
                res.add(file);
            else if (fileZip.exists())
                res.add(fileZip);
            else {
                if (log.isInfoEnabled())
                    log.info("Segment not found: " + file.getName() + "/" + fileZip.getName());

                res.clear();

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

    /**
     * Method is called twice on deactivate and stop.
     * It shutdown workers but do not deallocate them to avoid duplication.
     */
    @Override protected void stop0(boolean cancel) {
        final GridTimeoutProcessor.CancelableTask schedule = backgroundFlushSchedule;

        if (schedule != null)
            schedule.close();

        stopAutoRollover();

        try {
            if (fileHandleManager != null)
                fileHandleManager.onDeactivate();
        }
        catch (Exception e) {
            U.error(log, "Failed to gracefully close WAL segment: " + currHnd, e);
        }

        if (segmentAware != null)
            segmentAware.interrupt();

        try {
            if (archiver != null)
                archiver.shutdown();

            if (compressor != null)
                compressor.shutdown();

            if (decompressor != null)
                decompressor.shutdown();

            if (cleaner != null)
                cleaner.shutdown();
        }
        catch (IgniteInterruptedCheckedException e) {
            U.error(log, "Failed to gracefully shutdown WAL components, thread was interrupted.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activated file write ahead log manager [nodeId=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");
        //NOOP implementation, we need to override it.
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
    @Override public void resumeLogging(WALPointer filePtr) throws IgniteCheckedException {
        if (log.isDebugEnabled()) {
            log.debug("File write ahead log manager resuming logging [nodeId=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");
        }

        // walDisableContext is started after FileWriteAheadLogManager, so we obtain actual walDisableContext ref here.
        synchronized (this) {
            walDisableContext = cctx.walState().walDisableContext();
        }

        assert currHnd == null;

        startArchiveWorkers();

        assert (isArchiverEnabled() && archiver != null) || (!isArchiverEnabled() && archiver == null) :
            "Trying to restore FileWriteHandle on deactivated write ahead log manager";

        fileHandleManager.resumeLogging();

        updateCurrentHandle(restoreWriteHandle(filePtr), null);

        // For new handle write serializer version to it.
        if (filePtr == null)
            currHnd.writeHeader();

        if (currHnd.serializerVersion() != serializer.version()) {
            if (log.isInfoEnabled()) {
                log.info("Record serializer version change detected, will start logging with a new WAL record " +
                    "serializer to a new WAL segment [curFile=" + currHnd + ", newVer=" + serializer.version() +
                    ", oldVer=" + currHnd.serializerVersion() + ']');
            }

            rollOver(currHnd, null);
        }

        currHnd.finishResumeLogging();

        if (mode == WALMode.BACKGROUND)
            backgroundFlushSchedule = cctx.time().schedule(this::doFlush, flushFreq, flushFreq);

        if (walAutoArchiveAfterInactivity > 0 || walForceArchiveTimeout > 0)
            scheduleNextRolloverCheck();
    }

    /**
     * Schedules next rollover check.
     * If {@link DataStorageConfiguration#getWalForceArchiveTimeout()} configured rollover happens forcefully.
     * Else check based on current record update timestamp and at timeout method does check of inactivity period and schedules new launch.
     */
    private void scheduleNextRolloverCheck() {
        assert walAutoArchiveAfterInactivity > 0 || walForceArchiveTimeout > 0;
        assert timeoutRolloverMux != null;

        synchronized (timeoutRolloverMux) {
            if (timeoutRollover != null)
                return;

            long nextEndTime = walForceArchiveTimeout > 0
                ? nextTimeout(lastDataRecordLoggedMs.get(), walForceArchiveTimeout)
                : nextTimeout(lastRecordLoggedMs.get(), walAutoArchiveAfterInactivity);

            cctx.time().addTimeoutObject(timeoutRollover = new TimeoutRollover(nextEndTime));
        }
    }

    /** */
    private long nextTimeout(long lastEvt, long timeout) {
        return (lastEvt <= 0 ? U.currentTimeMillis() : lastEvt) + timeout;
    }

    /** {@inheritDoc} */
    @Override public int serializerVersion() {
        return serializerVer;
    }

    /**
     * Checks if there was elapsed significant period of inactivity or force archive timeout.
     * If WAL auto-archive is enabled using {@link #walAutoArchiveAfterInactivity} > 0 or {@link #walForceArchiveTimeout}
     * this method will activate roll over by timeout.
     */
    private void checkWalRolloverRequired() {
        if (walAutoArchiveAfterInactivity <= 0 && walForceArchiveTimeout <= 0)
            return; // feature not configured, nothing to do.

        if (lastRecordLoggedMs.get() == 0)
            return; //no records were logged to current segment, does not consider inactivity.

        if (walForceArchiveTimeout > 0) {
            if (lastDataRecordLoggedMs.get() == 0)
                return; //no data records were logged to current segment, do not rollover.
        }
        else if (!checkTimeout(lastRecordLoggedMs, walAutoArchiveAfterInactivity))
            return;

        final FileWriteHandle handle = currentHandle();

        try {
            closeBufAndRollover(handle, null, RolloverType.NONE);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Unable to perform segment rollover: " + e.getMessage(), e);

            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, e));
        }
    }

    /** */
    private boolean checkTimeout(AtomicLong lastEvt, long timeout) {
        final long lastEvtMs = lastEvt.get();

        final long elapsedMs = U.currentTimeMillis() - lastEvtMs;

        if (elapsedMs <= timeout)
            return false; // not enough time elapsed since last write.

        // Will return false if record write occurred concurrently.
        return lastEvt.compareAndSet(lastEvtMs, 0);
    }

    /** {@inheritDoc} */
    @Override public WALPointer log(WALRecord rec) throws IgniteCheckedException {
        return log(rec, RolloverType.NONE);
    }

    /** {@inheritDoc} */
    @Override public WALPointer log(WALRecord rec, RolloverType rolloverType) throws IgniteCheckedException {
        if (serializer == null || mode == WALMode.NONE)
            return null;

        // Only delta-records, page snapshots and memory recovery are allowed to write in recovery mode.
        if (cctx.kernalContext().recoveryMode() &&
            !(rec instanceof PageDeltaRecord || rec instanceof PageSnapshot || rec instanceof MemoryRecoveryRecord))
            return null;

        FileWriteHandle currWrHandle = currentHandle();

        WALDisableContext isDisable = walDisableContext;

        // Logging was not resumed yet.
        if (currWrHandle == null || (isDisable != null && isDisable.check()))
            return null;

        // Do page snapshots compression if configured.
        if (pageCompression != DiskPageCompression.DISABLED && rec instanceof PageSnapshot) {
            PageSnapshot pageSnapshot = (PageSnapshot)rec;

            int pageSize = pageSnapshot.realPageSize();

            ByteBuffer pageData = pageSnapshot.pageDataBuffer();

            ByteBuffer compressedPage = cctx.kernalContext().compress().compressPage(pageData, pageSize, 1,
                pageCompression, pageCompressionLevel);

            if (compressedPage != pageData) {
                assert compressedPage.isDirect() : "Is direct buffer: " + compressedPage.isDirect();

                rec = new PageSnapshot(pageSnapshot.fullPageId(), GridUnsafe.bufferAddress(compressedPage),
                    compressedPage.limit(), pageSize);
            }
        }

        // Need to calculate record size first.
        rec.size(serializer.size(rec));

        while (true) {
            WALPointer ptr;

            if (rolloverType == RolloverType.NONE)
                ptr = currWrHandle.addRecord(rec);
            else {
                assert cctx.database().checkpointLockIsHeldByThread();

                if (rolloverType == RolloverType.NEXT_SEGMENT) {
                    WALPointer pos = rec.position();

                    do {
                        // This will change rec.position() unless concurrent rollover happened.
                        currWrHandle = closeBufAndRollover(currWrHandle, rec, rolloverType);
                    }
                    while (Objects.equals(pos, rec.position()));

                    ptr = rec.position();
                }
                else if (rolloverType == RolloverType.CURRENT_SEGMENT) {
                    if ((ptr = currWrHandle.addRecord(rec)) != null)
                        currWrHandle = closeBufAndRollover(currWrHandle, rec, rolloverType);
                }
                else
                    throw new IgniteCheckedException("Unknown rollover type: " + rolloverType);
            }

            if (ptr != null) {
                metrics.onWalRecordLogged(rec.size());

                if (walAutoArchiveAfterInactivity > 0 || walForceArchiveTimeout > 0) {
                    long millis = U.currentTimeMillis();

                    lastRecordLoggedMs.set(millis);

                    // Only data records handled by CDC.
                    // No need to forcefully rollover for other record types.
                    if (walForceArchiveTimeout > 0 && rec.type() == DATA_RECORD_V2)
                        lastDataRecordLoggedMs.set(millis);
                }

                return ptr;
            }
            else
                currWrHandle = rollOver(currWrHandle, null);

            checkNode();

            if (isStopping())
                throw new IgniteCheckedException("Stopping.");
        }
    }

    /** */
    private FileWriteHandle closeBufAndRollover(
        FileWriteHandle currWriteHandle,
        @Nullable WALRecord rec,
        RolloverType rolloverType
    ) throws IgniteCheckedException {
        long idx = currWriteHandle.getSegmentId();

        currWriteHandle.closeBuffer();

        FileWriteHandle res = rollOver(currWriteHandle, rolloverType == RolloverType.NEXT_SEGMENT ? rec : null);

        if (log != null && log.isInfoEnabled()) {
            log.info("Rollover segment [" + idx + " to " + res.getSegmentId() + "], recordType=" +
                (rec == null ? null : rec.type()));
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public WALPointer flush(WALPointer ptr, boolean explicitFsync) throws IgniteCheckedException, StorageException {
        return fileHandleManager.flush(ptr, explicitFsync);
    }

    /** {@inheritDoc} */
    @Override public WALRecord read(WALPointer ptr) throws IgniteCheckedException, StorageException {
        try (WALIterator it = replay(ptr)) {
            IgniteBiTuple<WALPointer, WALRecord> rec = it.next();

            if (rec != null && rec.get2().position().equals(ptr))
                return rec.get2();
            else
                throw new StorageException("Failed to read record by pointer [ptr=" + ptr + ", rec=" + rec + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public WALIterator replay(WALPointer start) throws IgniteCheckedException, StorageException {
        return replay(start, null);
    }

    /** {@inheritDoc} */
    @Override public WALIterator replay(
        WALPointer start,
        @Nullable IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordDeserializeFilter
    ) throws IgniteCheckedException, StorageException {
        FileWriteHandle hnd = currentHandle();

        WALPointer end = null;

        if (hnd != null)
            end = hnd.position();

        RecordsIterator iter = new RecordsIterator(
            cctx,
            walArchiveDir,
            walWorkDir,
            start,
            end,
            dsCfg,
            new RecordSerializerFactoryImpl(cctx).recordDeserializeFilter(recordDeserializeFilter),
            ioFactory,
            archiver,
            decompressor,
            log,
            segmentAware,
            segmentRouter,
            lockedSegmentFileInputFactory
        );

        try {
            iter.init(); // Make sure iterator is closed on any error.
        }
        catch (Throwable t) {
            iter.close();

            throw t;
        }

        return iter;
    }

    /** {@inheritDoc} */
    @Override public boolean reserve(WALPointer start) {
        assert start != null;

        if (mode == WALMode.NONE)
            return false;

        // Protection from deletion.
        boolean reserved = segmentAware.reserve(start.index());

        // Segment presence check.
        if (reserved && !hasIndex(start.index())) {
            segmentAware.release(start.index());

            reserved = false;
        }

        return reserved;
    }

    /** {@inheritDoc} */
    @Override public void release(WALPointer start) {
        assert start != null;

        if (mode == WALMode.NONE)
            return;

        segmentAware.release(start.index());
    }

    /**
     * Checking for the existence of an index.
     *
     * @param absIdx Segment index.
     * @return {@code True} exists.
     */
    private boolean hasIndex(long absIdx) {
        String segmentName = fileName(absIdx);

        boolean inArchive = new File(walArchiveDir, segmentName).exists() ||
            new File(walArchiveDir, segmentName + ZIP_SUFFIX).exists();

        if (inArchive)
            return true;

        if (absIdx <= lastArchivedIndex())
            return false;

        FileWriteHandle cur = currHnd;

        return cur != null && cur.getSegmentId() >= absIdx;
    }

    /** {@inheritDoc} */
    @Override public int truncate(@Nullable WALPointer high) {
        if (high == null)
            return 0;

        FileDescriptor[] descs = walArchiveFiles();

        int deleted = 0;

        for (FileDescriptor desc : descs) {
            long archivedAbsIdx = segmentAware.lastArchivedAbsoluteIndex();

            long lastArchived = archivedAbsIdx >= 0 ? archivedAbsIdx : lastArchivedIndex();

            if (desc.idx >= lastCheckpointPtr.index() // We cannot delete segments needed for binary recovery.
                || desc.idx >= lastArchived // We cannot delete last segment, it is needed at start of node and avoid gaps.
                || desc.idx >= high.index() // We cannot delete segments larger than the border.
                || !segmentAware.minReserveIndex(desc.idx)) // We cannot delete reserved segment.
                return deleted;

            long len = desc.file.length();

            if (!desc.file.delete()) {
                U.warn(log, "Failed to remove obsolete WAL segment (make sure the process has enough rights): " +
                    desc.file.getAbsolutePath());
            }
            else {
                deleted++;

                long idx = desc.idx();

                segmentSize.remove(idx);
                segmentAware.addSize(idx, -len);
            }

            // Bump up the oldest archive segment index.
            if (segmentAware.lastTruncatedArchiveIdx() < desc.idx)
                segmentAware.lastTruncatedArchiveIdx(desc.idx);

            cctx.kernalContext().encryption().onWalSegmentRemoved(desc.idx);
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

        return ((archiver0 != null) && segmentAware.locked(absIdx)) || (segmentAware.reserved(absIdx));
    }

    /** {@inheritDoc} */
    @Override public void notchLastCheckpointPtr(WALPointer ptr) {
        lastCheckpointPtr = ptr;

        segmentAware.lastCheckpointIdx(ptr.index());
    }

    /** {@inheritDoc} */
    @Override public long currentSegment() {
        return segmentAware.curAbsWalIdx();
    }

    /** {@inheritDoc} */
    @Override public int walArchiveSegments() {
        long lastTruncated = segmentAware.lastTruncatedArchiveIdx();

        long lastArchived = segmentAware.lastArchivedAbsoluteIndex();

        if (lastArchived == -1)
            return 0;

        return Math.max((int)(lastArchived - lastTruncated), 0);
    }

    /** {@inheritDoc} */
    @Override public long lastArchivedSegment() {
        return segmentAware.lastArchivedAbsoluteIndex();
    }

    /** {@inheritDoc} */
    @Override public long lastCompactedSegment() {
        return segmentAware.lastCompressedIdx();
    }

    /** {@inheritDoc} */
    @Override public boolean reserved(WALPointer ptr) {
        return segmentReservedOrLocked(ptr.index());
    }

    /** {@inheritDoc} */
    @Override public int reserved(WALPointer low, WALPointer high) {
        // It is not clear now how to get the highest WAL pointer. So when high is null method returns 0.
        if (high == null)
            return 0;

        long lowIdx = low != null ? low.index() : 0;

        long highIdx = high.index();

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
     * @param file File to read.
     * @param ioFactory IO factory.
     */
    @Nullable private FileDescriptor readFileDescriptor(File file, FileIOFactory ioFactory) {
        FileDescriptor ds = new FileDescriptor(file);

        try (SegmentIO fileIO = ds.toReadOnlyIO(ioFactory)) {
            // File may be empty when LOG_ONLY mode is enabled and mmap is disabled.
            if (fileIO.size() == 0)
                return null;

            try (ByteBufferExpander buf = new ByteBufferExpander(HEADER_RECORD_SIZE, ByteOrder.nativeOrder())) {
                final DataInput in = segmentFileInputFactory.createFileInput(fileIO, buf);

                // Header record must be agnostic to the serializer version.
                final int type = in.readUnsignedByte();

                if (type == WALRecord.RecordType.STOP_ITERATION_RECORD_TYPE) {
                    if (log.isInfoEnabled())
                        log.info("Reached logical end of the segment for file " + file);

                    return null;
                }

                WALPointer ptr = readPosition(in);

                return new FileDescriptor(file, ptr.index());
            }
        }
        catch (IOException e) {
            U.warn(log, "Failed to read file header [" + file + "]. Skipping this file", e);

            return null;
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
     * @param rec Optional record to be added right after header.
     * @return Handle that will fit the entry.
     */
    private FileWriteHandle rollOver(FileWriteHandle cur, @Nullable WALRecord rec) throws IgniteCheckedException {
        FileWriteHandle hnd = currentHandle();

        if (hnd != cur)
            return hnd;

        if (hnd.close(true)) {
            if (metrics.metricsEnabled())
                metrics.onWallRollOver();

            if (switchSegmentRecordOffset != null) {
                int idx = (int)(cur.getSegmentId() % dsCfg.getWalSegments());

                switchSegmentRecordOffset.set(idx, hnd.getSwitchSegmentRecordOffset());
            }

            long idx = cur.getSegmentId() + 1;
            long currSize = 0;
            long reservedSize = maxWalSegmentSize;

            if (archiver == null)
                segmentAware.addSize(idx, reservedSize);

            FileWriteHandle next;
            try {
                try {
                    next = initNextWriteHandle(cur);
                }
                catch (IgniteCheckedException e) {
                    //Allow to avoid forever waiting in other threads.
                    cur.signalNextAvailable();

                    throw e;
                }

                if (rec != null) {
                    WALPointer ptr = next.addRecord(rec);

                    assert ptr != null;
                }

                currSize = reservedSize;
                segmentSize.put(idx, currSize);
            }
            finally {
                // Move checkpoint pointer to the edge as node don't have actual checkpoints in `inMemoryCdc=true` mode.
                // This will allow cleaner to remove segments from archive.
                if (inMemoryCdc)
                    notchLastCheckpointPtr(hnd.position());

                if (archiver == null)
                    segmentAware.addSize(idx, currSize - reservedSize);
            }

            if (next.getSegmentId() - lastCheckpointPtr.index() >= maxSegCountWithoutCheckpoint)
                cctx.database().forceCheckpoint("too big size of WAL without checkpoint");

            boolean updated = updateCurrentHandle(next, hnd);

            assert updated : "Concurrent updates on rollover are not allowed";

            if (walAutoArchiveAfterInactivity > 0 || walForceArchiveTimeout > 0) {
                lastRecordLoggedMs.set(0);

                if (walForceArchiveTimeout > 0)
                    lastDataRecordLoggedMs.set(0);
            }

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
    private FileWriteHandle restoreWriteHandle(@Nullable WALPointer lastReadPtr) throws StorageException {
        long absIdx = lastReadPtr == null ? 0 : lastReadPtr.index();

        @Nullable FileArchiver archiver0 = archiver;

        long segNo = archiver0 == null ? absIdx : absIdx % dsCfg.getWalSegments();

        File curFile = new File(walWorkDir, fileName(segNo));

        int off = lastReadPtr == null ? 0 : lastReadPtr.fileOffset();
        int len = lastReadPtr == null ? 0 : lastReadPtr.length();

        try {
            SegmentIO fileIO = new SegmentIO(absIdx, ioFactory.create(curFile));

            IgniteInClosure<FileIO> lsnr = createWalFileListener;

            if (lsnr != null)
                lsnr.apply(fileIO);

            try {
                int serVer = serializerVer;

                // If we have existing segment, try to read version from it.
                if (lastReadPtr != null) {
                    try {
                        serVer = readSegmentHeader(fileIO, segmentFileInputFactory).getSerializerVersion();
                    }
                    catch (SegmentEofException | EOFException ignore) {
                        serVer = serializerVer;
                    }
                }

                RecordSerializer ser = new RecordSerializerFactoryImpl(cctx).createSerializer(serVer);

                if (log.isInfoEnabled()) {
                    log.info("Resuming logging to WAL segment [file=" + curFile.getAbsolutePath() +
                        ", offset=" + off + ", ver=" + serVer + ']');
                }

                FileWriteHandle hnd = fileHandleManager.initHandle(fileIO, off + len, ser);

                segmentAware.curAbsWalIdx(absIdx);

                FileDescriptor[] walArchiveFiles = walArchiveFiles();

                segmentAware.minReserveIndex(F.isEmpty(walArchiveFiles) ? -1 : walArchiveFiles[0].idx - 1);
                segmentAware.lastTruncatedArchiveIdx(F.isEmpty(walArchiveFiles) ? -1 : walArchiveFiles[0].idx - 1);

                if (archiver0 == null)
                    segmentAware.setLastArchivedAbsoluteIndex(absIdx - 1);

                // Getting segment sizes.
                F.asList(walArchiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER)).stream()
                    .map(FileDescriptor::new)
                    .forEach(fd -> {
                        if (fd.isCompressed())
                            segmentSize.put(fd.idx(), fd.file().length());
                        else
                            segmentSize.putIfAbsent(fd.idx(), fd.file().length());
                    });

                // If walArchiveDir != walWorkDir, then need to get size of all segments that were not in archive.
                // For example, absIdx == 8, and there are 0-4 segments in archive, then we need to get sizes of 5-7 segments.
                // Size of the 8th segment will be set in #resumeLogging.
                if (archiver0 != null) {
                    for (long i = absIdx - (absIdx % dsCfg.getWalSegments()); i < absIdx; i++)
                        segmentSize.putIfAbsent(i, maxWalSegmentSize);
                }

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
                    throw (StorageException)e;

                throw e instanceof IOException ? (IOException)e : new IOException(e);
            }
        }
        catch (IOException e) {
            throw new StorageException("Failed to restore WAL write handle: " + curFile.getAbsolutePath(), e);
        }
    }

    /**
     * Fills the file header for a new segment. Calling this method signals we are done with the segment and it can be
     * archived. If we don't have prepared file yet and achiever is busy this method blocks.
     *
     * @param cur Current file write handle released by WAL writer.
     * @return Initialized file handle.
     * @throws IgniteCheckedException If exception occurred.
     */
    private FileWriteHandle initNextWriteHandle(FileWriteHandle cur) throws IgniteCheckedException {
        IgniteCheckedException error = null;

        try {
            File nextFile = pollNextFile(cur.getSegmentId());

            if (log.isDebugEnabled())
                log.debug("Switching to a new WAL segment: " + nextFile.getAbsolutePath());

            SegmentIO fileIO = null;

            FileWriteHandle hnd;

            boolean interrupted = false;

            if (switchSegmentRecordOffset != null)
                switchSegmentRecordOffset.set((int)((cur.getSegmentId() + 1) % dsCfg.getWalSegments()), 0);

            while (true) {
                try {
                    fileIO = new SegmentIO(cur.getSegmentId() + 1, ioFactory.create(nextFile));

                    IgniteInClosure<FileIO> lsnr = createWalFileListener;
                    if (lsnr != null)
                        lsnr.apply(fileIO);

                    hnd = fileHandleManager.nextHandle(fileIO, serializer);

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
                }
            }

            hnd.writeHeader();

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
     * Prepare and check WAL files.
     *
     * @throws StorageException If failed.
     */
    private void prepareAndCheckWalFiles() throws StorageException {
        Collection<File> tmpFiles = new HashSet<>();

        for (File walDir : F.asList(walWorkDir, walArchiveDir)) {
            tmpFiles.addAll(F.asList(walDir.listFiles(WAL_SEGMENT_TEMP_FILE_FILTER)));
            tmpFiles.addAll(F.asList(walDir.listFiles(WAL_SEGMENT_TEMP_FILE_COMPACTED_FILTER)));
        }

        for (File tmpFile : tmpFiles) {
            if (tmpFile.exists() && !tmpFile.delete()) {
                throw new StorageException("Failed to delete previously created temp file " +
                    "(make sure Ignite process has enough rights): " + tmpFile.getAbsolutePath());
            }
        }

        if (F.isEmpty(walWorkDir.listFiles(WAL_SEGMENT_FILE_FILTER)))
            createFile(new File(walWorkDir, fileName(0)));

        if (isArchiverEnabled()) {
            moveSegmentsToArchive();

            renameLastSegment();

            formatWorkSegments();

            checkFiles(0, false, null, null);
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

        File tmp = new File(file.getParent(), file.getName() + TMP_SUFFIX);

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
    private File pollNextFile(long curIdx) throws StorageException, IgniteInterruptedCheckedException {
        FileArchiver archiver0 = archiver;

        if (archiver0 == null) {
            segmentAware.curAbsWalIdx(curIdx + 1);
            segmentAware.setLastArchivedAbsoluteIndex(curIdx);

            return new File(walWorkDir, fileName(curIdx + 1));
        }

        long absNextIdxStartTime = System.nanoTime();

        // Signal to archiver that we are done with the segment, and it can be archived.
        long absNextIdx = archiver0.nextAbsoluteSegmentIndex();

        assert absNextIdx == curIdx + 1 : "curIdx=" + curIdx + ", nextIdx=" + absNextIdx;

        long absNextIdxWaitTime = U.nanosToMillis(System.nanoTime() - absNextIdxStartTime);

        if (absNextIdxWaitTime > THRESHOLD_WAIT_TIME_NEXT_WAL_SEGMENT) {
            log.warning(
                String.format("Waiting for next wal segment was too long " +
                        "[waitingTime=%s, curIdx=%s, absNextIdx=%s, walSegments=%s]",
                    absNextIdxWaitTime,
                    curIdx,
                    absNextIdx,
                    dsCfg.getWalSegments())
            );
        }

        long segmentIdx = absNextIdx % dsCfg.getWalSegments();

        return new File(walWorkDir, fileName(segmentIdx));
    }

    /**
     * Files from {@link #walArchiveDir}.
     *
     * @return Raw or compressed WAL segments from archive.
     */
    public FileDescriptor[] walArchiveFiles() {
        return scan(walArchiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER));
    }

    /**
     * @return Sorted WAL files descriptors.
     */
    public static FileDescriptor[] scan(@Nullable File[] allFiles) {
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
     * Monitor of current object is used for notify on: <ul>
     *     <li>exception occurred ({@link FileArchiver#cleanErr}!=null)</li>
     *     <li>stopping thread ({@link FileArchiver#isCancelled==true})</li>
     *     <li>current file index changed </li>
     *     <li>last archived file index was changed</li>
     *     <li>some WAL index was removed from map</li>
     * </ul>
     */
    private class FileArchiver extends GridWorker {
        /** Exception which occurred during initial creation of files or during archiving WAL segment */
        private StorageException cleanErr;

        /** Formatted index. */
        private int formatted;

        /**
         * Constructor.
         *
         * @param log Logger.
         */
        private FileArchiver(IgniteLogger log) {
            super(cctx.igniteInstanceName(), "wal-file-archiver%" + cctx.igniteInstanceName(), log,
                cctx.kernalContext().workersRegistry());
        }

        /**
         * Initialization.
         *
         * @param segmentAware Segment aware.
         * @throws IgniteCheckedException If initialization failed.
         */
        private void init(SegmentAware segmentAware) throws IgniteCheckedException {
            IgniteBiTuple<Long, Long> tup = scanMinMaxArchiveIndices();

            segmentAware.lastTruncatedArchiveIdx(tup == null ? -1 : tup.get1() - 1);

            long lastAbsArchivedIdx = tup == null ? -1 : tup.get2();

            if (lastAbsArchivedIdx >= 0)
                segmentAware.setLastArchivedAbsoluteIndex(lastAbsArchivedIdx);
        }

        /**
         * Lists files in archive directory and returns the indices of least and last archived files.
         * In case of holes, first segment after last "hole" is considered as minimum.
         * Example: minimum(0, 1, 10, 11, 20, 21, 22) should be 20
         *
         * @return The absolute indices of min and max archived files.
         */
        private IgniteBiTuple<Long, Long> scanMinMaxArchiveIndices() throws IgniteCheckedException {
            TreeMap<Long, FileDescriptor> archiveIndices = new TreeMap<>();

            for (File file : walArchiveDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER)) {
                try {
                    long idx = new FileDescriptor(file).idx();

                    FileDescriptor desc = readFileDescriptor(file, ioFactory);

                    if (desc != null) {
                        if (desc.idx() == idx)
                            archiveIndices.put(idx, desc);
                    }
                    else
                        log.warning("Skip file, failed read file header " + file);
                }
                catch (NumberFormatException | IndexOutOfBoundsException ignore) {
                    log.warning("Skip file " + file);
                }
            }

            if (!archiveIndices.isEmpty()) {
                Long min = archiveIndices.navigableKeySet().first();
                Long max = archiveIndices.navigableKeySet().last();

                if (max - min == archiveIndices.size() - 1)
                    return F.t(min, max); // Short path.

                // Try to find min and max if we have skipped range semgnets in archive. Find firs gap.
                for (Long idx : archiveIndices.descendingKeySet()) {
                    if (!archiveIndices.containsKey(idx - 1))
                        return F.t(idx, max);
                }

                throw new IllegalStateException("Should never happen if archiveIndices TreeMap is valid.");
            }

            // If WAL archive is empty, try to find last not archived segment in work directory and copy to WAL archive.
            TreeMap<Long, FileDescriptor> workIndices = new TreeMap<>();

            for (File file : walWorkDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER)) {
                FileDescriptor desc = readFileDescriptor(file, ioFactory);

                if (desc != null)
                    workIndices.put(desc.idx(), desc);
            }

            if (!workIndices.isEmpty()) {
                FileDescriptor first = workIndices.firstEntry().getValue();
                FileDescriptor last = workIndices.lastEntry().getValue();

                if (first.idx() != last.idx()) {
                    archiveSegment(first.idx());

                    // Use copied segment as min archived segment.
                    return F.t(first.idx(), first.idx());
                }
            }

            return null;
        }

        /**
         * @throws IgniteInterruptedCheckedException If failed to wait for thread shutdown.
         */
        private void shutdown() throws IgniteInterruptedCheckedException {
            synchronized (this) {
                isCancelled.set(true);

                notifyAll();
            }

            U.join(runner());
        }

        /** {@inheritDoc} */
        @Override protected void body() {
            blockingSectionBegin();

            try {
                allocateRemainingFiles();
            }
            catch (StorageException e) {
                synchronized (this) {
                    // Stop the thread and report to starter.
                    cleanErr = e;

                    segmentAware.forceInterrupt();

                    notifyAll();
                }

                cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, e));

                return;
            }
            finally {
                blockingSectionEnd();
            }

            Throwable err = null;

            try {
                blockingSectionBegin();

                try {
                    segmentAware.awaitSegment(0); //wait for init at least one work segments.
                }
                finally {
                    blockingSectionEnd();
                }

                while (!Thread.currentThread().isInterrupted() && !isCancelled()) {
                    long toArchive;

                    blockingSectionBegin();

                    try {
                        toArchive = segmentAware.waitNextSegmentForArchivation();
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    if (isCancelled())
                        break;

                    SegmentArchiveResult res;

                    blockingSectionBegin();

                    try {
                        res = archiveSegment(toArchive);
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    blockingSectionBegin();

                    try {
                        segmentAware.markAsMovedToArchive(toArchive);
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    if (evt.isRecordable(EVT_WAL_SEGMENT_ARCHIVED) && !cctx.kernalContext().recoveryMode()) {
                        evt.record(new WalSegmentArchivedEvent(
                            cctx.discovery().localNode(),
                            res.getAbsIdx(),
                            res.getDstArchiveFile())
                        );
                    }

                    onIdle();
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                Thread.currentThread().interrupt();

                synchronized (this) {
                    isCancelled.set(true);
                }
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                if (err == null && !isCancelled())
                    err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    failureProcessor.process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    failureProcessor.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * Gets the absolute index of the next WAL segment available to write. Blocks till there are available file to
         * write
         *
         * @return Next index (curWalSegmIdx+1) when it is ready to be written.
         * @throws StorageException If exception occurred in the archiver thread.
         */
        private long nextAbsoluteSegmentIndex() throws StorageException, IgniteInterruptedCheckedException {
            if (cleanErr != null)
                throw cleanErr;

            try {
                long nextIdx;

                boolean interrupted = false;

                while (true) {
                    try {
                        nextIdx = segmentAware.nextAbsoluteSegmentIndex();

                        break;
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        if (isCancelled.get()) {
                            // Archiver will soon complete its work, so we can not wait for the segment to be archived.
                            throw e;
                        }
                        else {
                            // It is assumed that the interruption of the thread came for example from a user thread,
                            // which should not interrupt write to the WAL, so we should just try again.
                            interrupted = true;
                        }
                    }
                }

                if (interrupted)
                    Thread.currentThread().interrupt();

                synchronized (this) {
                    // Wait for formatter so that we do not open an empty file in DEFAULT mode.
                    while (nextIdx % dsCfg.getWalSegments() > formatted && cleanErr == null)
                        wait();
                }

                if (cleanErr != null)
                    throw cleanErr;

                return nextIdx;
            }
            catch (IgniteInterruptedCheckedException e) {
                if (cleanErr != null)
                    throw cleanErr;

                throw e;
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedCheckedException(e);
            }
        }

        /**
         * Moves WAL segment from work folder to archive folder. Temp file is used to do movement.
         *
         * @param absIdx Absolute index to archive.
         * @throws StorageException If failed.
         */
        public SegmentArchiveResult archiveSegment(long absIdx) throws StorageException {
            long segIdx = absIdx % dsCfg.getWalSegments();

            File origFile = new File(walWorkDir, fileName(segIdx));

            String name = fileName(absIdx);

            File dstTmpFile = new File(walArchiveDir, name + TMP_SUFFIX);

            File dstFile = new File(walArchiveDir, name);

            if (log.isInfoEnabled()) {
                log.info("Starting to copy WAL segment [absIdx=" + absIdx + ", segIdx=" + segIdx +
                    ", origFile=" + origFile.getAbsolutePath() + ", dstFile=" + dstFile.getAbsolutePath() + ']');
            }

            assert switchSegmentRecordOffset != null;

            long offs = switchSegmentRecordOffset.getAndSet((int)segIdx, 0);
            long origLen = origFile.length();

            long currSize = 0;
            long reservedSize = offs > 0 && offs < origLen ? offs : origLen;

            segmentAware.addSize(absIdx, reservedSize);

            try {
                if (offs > 0 && offs < origLen)
                    GridFileUtils.copy(ioFactory, origFile, ioFactory, dstTmpFile, offs);
                else
                    Files.copy(origFile.toPath(), dstTmpFile.toPath());

                Files.move(dstTmpFile.toPath(), dstFile.toPath());

                if (walCdcDir != null) {
                    if (!cdcDisabled.getOrDefault(false)) {
                        if (checkCdcWalDirectorySize(dstFile.length()))
                            Files.createLink(walCdcDir.toPath().resolve(dstFile.getName()), dstFile.toPath());
                        else {
                            log.error("Creation of segment CDC link skipped. Configured CDC directory " +
                                "maximum size exceeded.");
                        }
                    }
                    else {
                        log.warning("Creation of segment CDC link skipped. " +
                            "'" + CDC_DISABLED + "' distributed property is 'true'.");
                    }
                }

                if (mode != WALMode.NONE) {
                    try (FileIO f0 = ioFactory.create(dstFile, CREATE, READ, WRITE)) {
                        f0.force();
                    }
                }

                currSize = dstFile.length();
                segmentSize.put(absIdx, currSize);
            }
            catch (IOException e) {
                deleteArchiveFiles(dstFile, dstTmpFile);

                throw new StorageException("Failed to archive WAL segment [" +
                    "srcFile=" + origFile.getAbsolutePath() +
                    ", dstFile=" + dstTmpFile.getAbsolutePath() + ']', e);
            }
            finally {
                segmentAware.addSize(absIdx, currSize - reservedSize);
            }

            if (log.isInfoEnabled()) {
                log.info("Copied file [src=" + origFile.getAbsolutePath() +
                    ", dst=" + dstFile.getAbsolutePath() + ']');
            }

            return new SegmentArchiveResult(absIdx, origFile, dstFile);
        }

        /**
         *
         */
        private boolean checkStop() {
            return isCancelled();
        }

        /**
         * Background creation of all segments except first. First segment was created in main thread by {@link
         * FileWriteAheadLogManager#prepareAndCheckWalFiles()}
         */
        private void allocateRemainingFiles() throws StorageException {
            checkFiles(
                1,
                true,
                (IgnitePredicate<Integer>)integer -> !checkStop(),
                (CI1<Integer>)idx -> {
                    synchronized (FileArchiver.this) {
                        formatted = idx;

                        FileArchiver.this.notifyAll();
                    }
                }
            );
        }

        /**
         * Restart worker in IgniteThread.
         */
        public void restart() {
            assert runner() == null : "FileArchiver is still running";

            isCancelled.set(false);

            new IgniteThread(archiver).start();
        }

        /**
         * @param len Length of file to check size.
         * @return {@code True} if the CDC directory size check successful, otherwise {@code false}.
         */
        private boolean checkCdcWalDirectorySize(long len) {
            long maxDirSize = igCfg.getDataStorageConfiguration().getCdcWalDirectoryMaxSize();

            if (maxDirSize <= 0)
                return true;

            long dirSize = Arrays.stream(walCdcDir.listFiles(WAL_SEGMENT_FILE_FILTER)).mapToLong(File::length).sum();

            if (dirSize + len <= maxDirSize)
                return true;

            log.warning("Configured CDC WAL directory maximum size exceeded [curDirSize=" + dirSize +
                ", fileLength=" + len + ", cdcWalDirectoryMaxSize=" + maxDirSize + ']');

            return false;
        }
    }

    /**
     * Responsible for compressing WAL archive segments.
     * Also responsible for deleting raw copies of already compressed WAL archive segments if they are not reserved.
     */
    private class FileCompressor extends FileCompressorWorker {
        /** Workers queue. */
        private final List<FileCompressorWorker> workers = new ArrayList<>();

        /**
         * Constructor.
         *
         * @param log Logger.
         */
        FileCompressor(IgniteLogger log) {
            super(0, log);
        }

        /** */
        private void init() {
            for (int i = 1; i < calculateThreadCount(); i++) {
                FileCompressorWorker worker = new FileCompressorWorker(i, log);

                worker.restart();

                synchronized (this) {
                    workers.add(worker);
                }
            }
        }

        /**
         * Checks if there are already compressed segments and assigns counters if needed.
         */
        private void initAlreadyCompressedSegments() {
            long firstArchivedIdx = -1;
            long lastCompactedIdx = -1;

            for (FileDescriptor segment : walArchiveFiles()) {
                if (segment.isCompressed()) {
                    lastCompactedIdx = segment.idx();

                    metrics.onWalSegmentCompressed(segment.file().length());
                }
                else if (firstArchivedIdx == -1)
                    firstArchivedIdx = segment.idx();
            }

            // We have to set a starting index for the compressor.
            if (lastCompactedIdx >= 0)
                segmentAware.onSegmentCompressed(lastCompactedIdx);
            else if (firstArchivedIdx >= 0)
                segmentAware.onSegmentCompressed(firstArchivedIdx - 1);
        }

        /**
         * Calculate optimal additional compressor worker threads count. If quarter of proc threads greater
         * than WAL_COMPRESSOR_WORKER_THREAD_CNT, use this value. Otherwise, reduce number of threads.
         *
         * @return Optimal number of compressor threads.
         */
        private int calculateThreadCount() {
            int procNum = Runtime.getRuntime().availableProcessors();

            // If quarter of proc threads greater than WAL_COMPRESSOR_WORKER_THREAD_CNT,
            // use this value. Otherwise, reduce number of threads.
            if (procNum >> 2 >= WAL_COMPRESSOR_WORKER_THREAD_CNT)
                return WAL_COMPRESSOR_WORKER_THREAD_CNT;
            else
                return procNum >> 2;
        }


        /** {@inheritDoc} */
        @Override public void body() throws InterruptedException, IgniteInterruptedCheckedException {
            init();

            super.body0();
        }

        /**
         * @throws IgniteInterruptedCheckedException If failed to wait for thread shutdown.
         */
        private void shutdown() throws IgniteInterruptedCheckedException {
            synchronized (this) {
                for (FileCompressorWorker worker: workers)
                    U.cancel(worker);

                for (FileCompressorWorker worker: workers)
                    U.join(worker);

                workers.clear();

                U.cancel(this);
            }

            U.join(this);
        }
    }

    /** */
    private class FileCompressorWorker extends GridWorker {
        /** Last compression error. */
        private volatile Throwable lastCompressionError;

        /** */
        FileCompressorWorker(int idx, IgniteLogger log) {
            super(cctx.igniteInstanceName(), "wal-file-compressor-%" + cctx.igniteInstanceName() + "%-" + idx, log);
        }

        /** */
        void restart() {
            assert runner() == null : "FileCompressorWorker is still running.";

            isCancelled.set(false);

            new IgniteThread(this).start();
        }

        /**
         * Pessimistically tries to reserve segment for compression in order to avoid concurrent truncation.
         * Waits if there's no segment to archive right now.
         */
        private long tryReserveNextSegmentOrWait() throws IgniteInterruptedCheckedException {
            long segmentToCompress = segmentAware.waitNextSegmentToCompress();

            boolean reserved = reserve(new WALPointer(segmentToCompress, 0, 0));

            if (reserved)
                return segmentToCompress;
            else {
                if (log.isDebugEnabled())
                    log.debug("Skipping segment compression [idx=" + segmentToCompress + ']');

                segmentAware.onSegmentCompressed(segmentToCompress);

                return -1;
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            body0();
        }

        /** */
        private void body0() {
            while (!isCancelled()) {
                long segIdx = -1L;

                try {
                    if ((segIdx = tryReserveNextSegmentOrWait()) == -1)
                        continue;

                    String segmentFileName = fileName(segIdx);

                    File tmpZip = new File(walArchiveDir, segmentFileName + ZIP_SUFFIX + TMP_SUFFIX);

                    File zip = new File(walArchiveDir, segmentFileName + ZIP_SUFFIX);

                    File raw = new File(walArchiveDir, segmentFileName);

                    long currSize = 0;
                    long reservedSize = raw.length();

                    segmentAware.addSize(segIdx, reservedSize);

                    try {
                        deleteObsoleteRawSegments();

                        if (!Files.exists(raw.toPath()))
                            throw new IgniteCheckedException("WAL archive segment is missing: " + raw);

                        compressSegmentToFile(segIdx, raw, tmpZip);

                        Files.move(tmpZip.toPath(), zip.toPath());

                        try (FileIO f0 = ioFactory.create(zip, CREATE, READ, WRITE)) {
                            f0.force();
                        }

                        currSize = zip.length();
                        segmentSize.put(segIdx, currSize);

                        metrics.onWalSegmentCompressed(currSize);

                        segmentAware.onSegmentCompressed(segIdx);

                        if (log.isDebugEnabled())
                            log.debug("Segment compressed notification [idx=" + segIdx + ']');

                        if (evt.isRecordable(EVT_WAL_SEGMENT_COMPACTED) && !cctx.kernalContext().recoveryMode())
                            evt.record(new WalSegmentCompactedEvent(cctx.localNode(), segIdx, zip.getAbsoluteFile()));
                    }
                    catch (IgniteCheckedException | IOException e) {
                        deleteArchiveFiles(zip, tmpZip);

                        lastCompressionError = e;

                        U.error(log, "Compression of WAL segment [idx=" + segIdx +
                            "] was skipped due to unexpected error", lastCompressionError);

                        segmentAware.onSegmentCompressed(segIdx);
                    }
                    finally {
                        segmentAware.addSize(segIdx, currSize - reservedSize);
                    }
                }
                catch (IgniteInterruptedCheckedException ignore) {
                    Thread.currentThread().interrupt();
                }
                finally {
                    if (segIdx != -1L)
                        release(new WALPointer(segIdx, 0, 0));
                }
            }
        }

        /**
         * Segment compression.
         *
         * @param idx Segment absolute index.
         * @param raw Raw segment file.
         * @param zip Zip file to writing.
         * @throws IOException If failed.
         * @throws IgniteCheckedException If failed.
         */
        private void compressSegmentToFile(long idx, File raw, File zip) throws IOException, IgniteCheckedException {
            int serializerVer;

            try (FileIO fileIO = ioFactory.create(raw)) {
                serializerVer = readSegmentHeader(new SegmentIO(idx, fileIO), segmentFileInputFactory)
                    .getSerializerVersion();
            }

            try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zip)))) {
                zos.setLevel(dsCfg.getWalCompactionLevel());
                zos.putNextEntry(new ZipEntry(idx + ".wal"));

                ByteBuffer buf = ByteBuffer.allocate(HEADER_RECORD_SIZE);
                buf.order(ByteOrder.nativeOrder());

                zos.write(prepareSerializerVersionBuffer(idx, serializerVer, true, buf).array());

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
                    log, cctx, ioFactory, BUF_SIZE, idx, walArchiveDir, appendToZipC)) {

                    while (iter.hasNextX())
                        iter.nextX();
                }

                RecordSerializer ser = new RecordSerializerFactoryImpl(cctx).createSerializer(serializerVer);

                ByteBuffer heapBuf = prepareSwitchSegmentRecordBuffer(idx, ser);

                zos.write(heapBuf.array());
            }
        }

        /**
         * @param idx Segment index.
         * @param ser Record Serializer.
         */
        private ByteBuffer prepareSwitchSegmentRecordBuffer(
            long idx,
            RecordSerializer ser
        ) throws IgniteCheckedException {
            SwitchSegmentRecord switchRecord = new SwitchSegmentRecord();

            int switchRecordSize = ser.size(switchRecord);
            switchRecord.size(switchRecordSize);

            switchRecord.position(new WALPointer(idx, 0, switchRecordSize));

            ByteBuffer heapBuf = ByteBuffer.allocate(switchRecordSize);

            ser.writeRecord(switchRecord, heapBuf);
            return heapBuf;
        }

        /**
         * Deletes raw WAL segments if they aren't locked and already have compressed copies of themselves.
         */
        private void deleteObsoleteRawSegments() {
            FileDescriptor[] descs = walArchiveFiles();

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

                if (desc.idx < lastCheckpointPtr.index() && duplicateIndices.contains(desc.idx))
                    segmentAware.addSize(desc.idx, -deleteArchiveFiles(desc.file));
            }
        }
    }

    /**
     * Responsible for decompressing previously compressed segments of WAL archive if they are needed for replay.
     */
    private class FileDecompressor extends GridWorker {
        /** Decompression futures. */
        private final Map<Long, GridFutureAdapter<Void>> decompressionFutures = new HashMap<>();

        /** Segments queue. */
        private final PriorityBlockingQueue<Long> segmentsQueue = new PriorityBlockingQueue<>();

        /** Byte array for draining data. */
        private final byte[] arr = new byte[BUF_SIZE];

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

                    blockingSectionBegin();

                    try {
                        segmentToDecompress = segmentsQueue.take();
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    if (isCancelled())
                        break;

                    if (segmentToDecompress == -1)
                        continue;

                    String segmentFileName = fileName(segmentToDecompress);

                    File zip = new File(walArchiveDir, segmentFileName + ZIP_SUFFIX);
                    File unzipTmp = new File(walArchiveDir, segmentFileName + TMP_SUFFIX);
                    File unzip = new File(walArchiveDir, segmentFileName);

                    long currSize = 0;
                    long reservedSize = U.uncompressedSize(zip);

                    segmentAware.addSize(segmentToDecompress, reservedSize);

                    IgniteCheckedException ex = null;

                    try {
                        if (unzip.exists())
                            throw new FileAlreadyExistsException(unzip.getAbsolutePath());

                        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(zip)));
                             FileIO io = ioFactory.create(unzipTmp)) {
                            zis.getNextEntry();

                            while (io.writeFully(arr, 0, zis.read(arr)) > 0)
                                updateHeartbeat();
                        }

                        Files.move(unzipTmp.toPath(), unzip.toPath());

                        currSize = unzip.length();
                    }
                    catch (IOException e) {
                        deleteArchiveFiles(unzipTmp);

                        if (e instanceof FileAlreadyExistsException) {
                            U.error(log, "Can't rename temporary unzipped segment: raw segment is already present " +
                                "[tmp=" + unzipTmp + ", raw=" + unzip + "]", e);
                        }
                        else if (!isCancelled.get()) {
                            ex = new IgniteCheckedException("Error during WAL segment decompression [segmentIdx=" +
                                segmentToDecompress + "]", e);
                        }
                    }
                    finally {
                        segmentAware.addSize(segmentToDecompress, currSize - reservedSize);
                    }

                    updateHeartbeat();

                    synchronized (this) {
                        decompressionFutures.remove(segmentToDecompress).onDone(ex);
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                if (!isCancelled.get())
                    err = e;
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                if (err == null && !isCancelled.get())
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

            File f = new File(walArchiveDir, fileName(idx));

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

        /** Restart worker. */
        void restart() {
            assert runner() == null : "FileDecompressor is still running.";

            isCancelled.set(false);

            new IgniteThread(this).start();
        }
    }

    /**
     * Validate files depending on {@link DataStorageConfiguration#getWalSegments()}  and create if need. Check end
     * when exit condition return false or all files are passed.
     *
     * @param startWith Start with.
     * @param create Flag create file.
     * @param p Predicate Exit condition.
     * @param completionCb Callback after verification segment.
     * @throws StorageException if validation or create file fail.
     */
    private void checkFiles(
        int startWith,
        boolean create,
        @Nullable IgnitePredicate<Integer> p,
        @Nullable IgniteInClosure<Integer> completionCb
    ) throws StorageException {
        for (int i = startWith; i < dsCfg.getWalSegments() && (p == null || p.apply(i)); i++) {
            File checkFile = new File(walWorkDir, fileName(i));

            if (checkFile.exists()) {
                if (checkFile.isDirectory()) {
                    throw new StorageException("Failed to initialize WAL log segment (a directory with " +
                        "the same name already exists): " + checkFile.getAbsolutePath());
                }
                else if (checkFile.length() != dsCfg.getWalSegmentSize() && mode == WALMode.FSYNC) {
                    throw new StorageException("Failed to initialize WAL log segment " +
                        "(WAL segment size change is not supported in 'DEFAULT' WAL mode) " +
                        "[filePath=" + checkFile.getAbsolutePath() +
                        ", fileSize=" + checkFile.length() +
                        ", configSize=" + dsCfg.getWalSegmentSize() + ']');
                }
            }
            else if (create)
                createFile(checkFile);

            if (completionCb != null)
                completionCb.apply(i);
        }
    }

    /**
     * Needs only for WAL compaction.
     *
     * @param idx Index.
     * @param ver Version.
     * @param compacted Compacted flag.
     */
    public static ByteBuffer prepareSerializerVersionBuffer(long idx, int ver, boolean compacted, ByteBuffer buf) {
        // Write record type.
        buf.put((byte)(WALRecord.RecordType.HEADER_RECORD.ordinal() + 1));

        // Write position.
        RecordV1Serializer.putPosition(buf, new WALPointer(idx, 0, 0));

        // Place magic number.
        buf.putLong(compacted ? HeaderRecord.COMPACTED_MAGIC : HeaderRecord.REGULAR_MAGIC);

        // Place serializer version.
        buf.putInt(ver);

        // Place CRC if needed.
        if (!RecordV1Serializer.skipCrc) {
            int curPos = buf.position();

            buf.position(0);

            // This call will move buffer position to the end of the record again.
            int crcVal = FastCrc.calcCrc(buf, curPos);

            buf.putInt(crcVal);
        }
        else
            buf.putInt(0);

        // Write header record through io.
        buf.position(0);

        return buf;
    }

    /**
     *
     */
    public static class ReadFileHandle extends AbstractFileHandle implements AbstractWalRecordsIterator.AbstractReadFileHandle {
        /** Entry serializer. */
        RecordSerializer ser;

        /** */
        FileInput in;

        /** Holder of actual information of latest manipulation on WAL segments. */
        private final SegmentAware segmentAware;

        /**
         * @param fileIO I/O interface for read/write operations of AbstractFileHandle.
         * @param ser Entry serializer.
         * @param in File input.
         * @param aware Segment aware.
         */
        public ReadFileHandle(
            SegmentIO fileIO,
            RecordSerializer ser,
            FileInput in,
            SegmentAware aware) {
            super(fileIO);

            this.ser = ser;
            this.in = in;
            segmentAware = aware;
        }

        /**
         * @throws IgniteCheckedException If failed to close the WAL segment file.
         */
        @Override public void close() throws IgniteCheckedException {
            try {
                fileIO.close();

                in.io().close();
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long idx() {
            return getSegmentId();
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
            return segmentAware != null && segmentAware.lastArchivedAbsoluteIndex() < getSegmentId();
        }
    }

    /**
     * Iterator over WAL-log.
     */
    private static class RecordsIterator extends AbstractWalRecordsIterator {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final File walArchiveDir;

        /** */
        private final File walWorkDir;

        /** See {@link FileWriteAheadLogManager#archiver}. */
        @Nullable private final FileArchiver archiver;

        /** */
        private final FileDecompressor decompressor;

        /** */
        private final DataStorageConfiguration dsCfg;

        /** Optional start pointer. */
        @Nullable private final WALPointer start;

        /** Optional end pointer. */
        @Nullable private final WALPointer end;

        /** Manager of segment location. */
        private final SegmentRouter segmentRouter;

        /** Holder of actual information of latest manipulation on WAL segments. */
        private final SegmentAware segmentAware;

        /**
         * @param cctx Shared context.
         * @param walArchiveDir WAL archive dir.
         * @param walWorkDir WAL dir.
         * @param start Optional start pointer.
         * @param end Optional end pointer.
         * @param dsCfg Database configuration.
         * @param serializerFactory Serializer factory.
         * @param archiver File Archiver.
         * @param decompressor Decompressor.
         * @param log Logger  @throws IgniteCheckedException If failed to initialize WAL segment.
         * @param segmentAware Segment aware.
         * @param segmentRouter Segment router.
         * @param segmentFileInputFactory Factory to provide I/O interfaces for read primitives with files.
         */
        private RecordsIterator(
            GridCacheSharedContext<?, ?> cctx,
            File walArchiveDir,
            File walWorkDir,
            @Nullable WALPointer start,
            @Nullable WALPointer end,
            DataStorageConfiguration dsCfg,
            RecordSerializerFactory serializerFactory,
            FileIOFactory ioFactory,
            @Nullable FileArchiver archiver,
            FileDecompressor decompressor,
            IgniteLogger log,
            SegmentAware segmentAware,
            SegmentRouter segmentRouter,
            SegmentFileInputFactory segmentFileInputFactory
        ) throws IgniteCheckedException {
            super(
                log,
                cctx,
                serializerFactory,
                ioFactory,
                dsCfg.getWalRecordIteratorBufferSize(),
                segmentFileInputFactory
            );

            this.walArchiveDir = walArchiveDir;
            this.walWorkDir = walWorkDir;
            this.archiver = archiver;
            this.start = start;
            this.end = end;
            this.dsCfg = dsCfg;

            this.decompressor = decompressor;
            this.segmentRouter = segmentRouter;
            this.segmentAware = segmentAware;
        }

        /** {@inheritDoc} */
        @Override protected ReadFileHandle initReadHandle(
            AbstractFileDescriptor desc,
            @Nullable WALPointer start
        ) throws IgniteCheckedException, FileNotFoundException {
            AbstractFileDescriptor currDesc = desc;

            if (!desc.file().exists()) {
                FileDescriptor zipFile = new FileDescriptor(
                    new File(walArchiveDir, fileName(desc.idx()) + ZIP_SUFFIX));

                if (!zipFile.file.exists()) {
                    throw new FileNotFoundException("Both compressed and raw segment files are missing in archive " +
                        "[segmentIdx=" + desc.idx() + "]");
                }

                if (decompressor != null)
                    decompressor.decompressFile(desc.idx()).get();
                else
                    currDesc = zipFile;
            }

            return (ReadFileHandle)super.initReadHandle(currDesc, start);
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            super.onClose();

            curRec = null;

            closeCurrentWalSegment();

            curWalSegmIdx = Integer.MAX_VALUE;
        }

        /**
         * @throws IgniteCheckedException If failed to initialize first file handle.
         */
        private void init() throws IgniteCheckedException {
            AbstractFileDescriptor[] descs = loadFileDescriptors(walArchiveDir);

            if (start != null) {
                if (!F.isEmpty(descs)) {
                    if (descs[0].idx() > start.index()) {
                        throw new IgniteCheckedException("WAL history is too short " +
                            "[descs=" + Arrays.asList(descs) + ", start=" + start + ']');
                    }

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

            advance();
        }

        /** {@inheritDoc} */
        @Override protected AbstractReadFileHandle advanceSegment(
            @Nullable final AbstractReadFileHandle curWalSegment
        ) throws IgniteCheckedException {
            if (curWalSegment != null)
                curWalSegment.close();

            // We are past the end marker.
            if (end != null && curWalSegmIdx + 1 > end.index())
                return null; //stop iteration

            curWalSegmIdx++;

            // Segment deletion protection.
            if (!segmentAware.reserve(curWalSegmIdx))
                throw new IgniteCheckedException("Segment does not exist: " + curWalSegmIdx);

            try {
                // Protection against transferring a segment to the archive by #archiver.
                boolean readArchive = archiver != null && !segmentAware.lock(curWalSegmIdx);

                FileDescriptor fd = null;
                ReadFileHandle nextHandle;
                try {
                    fd = segmentRouter.findSegment(curWalSegmIdx);

                    if (log.isDebugEnabled()) {
                        log.debug("Reading next file [absIdx=" + curWalSegmIdx +
                            ", file=" + fd.file.getAbsolutePath() + ']');
                    }

                    nextHandle = initReadHandle(fd, start != null && curWalSegmIdx == start.index() ? start : null);
                }
                catch (FileNotFoundException e) {
                    if (readArchive)
                        throw new IgniteCheckedException("Missing WAL segment in the archive: " + curWalSegment, e);
                    else {
                        // Log only when no segments were read. This will help us avoiding logging on the end of the WAL.
                        if (curRec == null && curWalSegment == null) {
                            File workDirFile = new File(walWorkDir, fileName(curWalSegmIdx % dsCfg.getWalSegments()));
                            File archiveDirFile = new File(walArchiveDir, fileName(curWalSegmIdx));

                            U.warn(
                                log,
                                "Next segment file is not found [" +
                                    "curWalSegmIdx=" + curWalSegmIdx
                                    + ", start=" + start
                                    + ", end=" + end
                                    + ", filePath=" + (fd == null ? "<empty>" : fd.file.getAbsolutePath())
                                    + ", walWorkDir=" + walWorkDir
                                    + ", walWorkDirContent=" + listFileNames(walWorkDir)
                                    + ", walArchiveDir=" + walArchiveDir
                                    + ", walArchiveDirContent=" + listFileNames(walArchiveDir)
                                    + ", workDirFile=" + workDirFile.getName()
                                    + ", exists=" + workDirFile.exists()
                                    + ", archiveDirFile=" + archiveDirFile.getName()
                                    + ", exists=" + archiveDirFile.exists()
                                    + "]",
                                e
                            );
                        }

                        nextHandle = null;
                    }
                }
                finally {
                    if (archiver != null && !readArchive)
                        segmentAware.unlock(curWalSegmIdx);
                }

                curRec = null;

                return nextHandle;
            }
            finally {
                segmentAware.release(curWalSegmIdx);
            }
        }

        /** */
        private static List<String> listFileNames(File dir) {
            File[] files = dir.listFiles();

            if (files == null)
                return Collections.emptyList();

            return Arrays.stream(files).map(File::getName).sorted().collect(toList());
        }

        /** {@inheritDoc} */
        @Override protected IgniteCheckedException handleRecordException(Exception e, @Nullable WALPointer ptr) {
            if (e instanceof IgniteCheckedException && X.hasCause(e, IgniteDataIntegrityViolationException.class)) {
                // This means that there is no explicit last segment, so we iterate until the very end.
                if (end == null) {
                    long nextWalSegmentIdx = curWalSegmIdx + 1;

                    if (archiver == null) {
                        if (canIgnoreCrcError(nextWalSegmentIdx, nextWalSegmentIdx, e, ptr))
                            return null;
                    }
                    else {
                        // Check that we should not look this segment up in archive directory.
                        // Basically the same check as in "advanceSegment" method.

                        // Segment deletion protection.
                        if (segmentAware.reserve(nextWalSegmentIdx)) {
                            try {
                                // Protection against transferring a segment to the archive by #archiver.
                                if (segmentAware.lock(nextWalSegmentIdx)) {
                                    try {
                                        long workIdx = nextWalSegmentIdx % dsCfg.getWalSegments();

                                        if (canIgnoreCrcError(workIdx, nextWalSegmentIdx, e, ptr))
                                            return null;
                                    }
                                    finally {
                                        segmentAware.unlock(nextWalSegmentIdx);
                                    }
                                }
                            }
                            finally {
                                segmentAware.release(nextWalSegmentIdx);
                            }
                        }
                    }
                }
            }

            return super.handleRecordException(e, ptr);
        }

        /**
         * @param workIdx Work index.
         * @param walSegmentIdx Wal segment index.
         * @param e Exception.
         * @param ptr Ptr.
         */
        private boolean canIgnoreCrcError(
            long workIdx,
            long walSegmentIdx,
            Exception e,
            @Nullable WALPointer ptr
        ) {
            FileDescriptor fd = new FileDescriptor(new File(walWorkDir, fileName(workIdx)), walSegmentIdx);

            try {
                if (!fd.file().exists())
                    return true;

                ReadFileHandle nextHandle = initReadHandle(fd, ptr);

                // "nextHandle == null" is true only if current segment is the last one in the
                // whole history. Only in such case we ignore crc validation error and just stop
                // as if we reached the end of the WAL.
                if (nextHandle == null)
                    return true;
            }
            catch (IgniteCheckedException | FileNotFoundException initReadHandleException) {
                e.addSuppressed(initReadHandleException);
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override protected AbstractReadFileHandle createReadFileHandle(SegmentIO fileIO,
            RecordSerializer ser, FileInput in) {
            return new ReadFileHandle(fileIO, ser, in, segmentAware);
        }
    }

    /**
     * Flushes current file handle for {@link WALMode#BACKGROUND} WALMode. Called periodically from scheduler.
     */
    private void doFlush() {
        FileWriteHandle hnd = currentHandle();

        try {
            hnd.flushAll();
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
    public static FileDescriptor[] loadFileDescriptors(final File walFilesDir) throws IgniteCheckedException {
        final File[] files = walFilesDir.listFiles(WAL_SEGMENT_COMPACTED_OR_RAW_FILE_FILTER);

        if (files == null) {
            throw new IgniteCheckedException("WAL files directory does not not denote a " +
                "directory, or if an I/O error occurs: [" + walFilesDir.getAbsolutePath() + "]");
        }
        return scan(files);
    }

    /** {@inheritDoc} */
    @Override public long segmentSize(long idx) {
        return segmentSize.getOrDefault(idx, 0L);
    }

    /** {@inheritDoc} */
    @Override public WALPointer lastWritePointer() {
        return currHnd.position();
    }

    /**
     * Concurrent {@link #currHnd} update.
     *
     * @param n New handle.
     * @param c Current handle, if not {@code null} CAS will be used.
     * @return {@code True} if updated.
     */
    private boolean updateCurrentHandle(FileWriteHandle n, @Nullable FileWriteHandle c) {
        boolean res = true;

        if (c == null)
            currHnd = n;
        else
            res = CURR_HND_UPD.compareAndSet(this, c, n);

        return res;
    }

    /**
     * Check that file name matches segment name.
     *
     * @param name File name.
     * @return {@code True} if file name matches segment name.
     */
    public static boolean isSegmentFileName(@Nullable String name) {
        return name != null && (WAL_NAME_PATTERN.matcher(name).matches() ||
            WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(name).matches());
    }

    /**
     * Getting last truncated segment.
     *
     * @return Absolut segment index.
     */
    public long lastTruncatedSegment() {
        return segmentAware.lastTruncatedArchiveIdx();
    }

    /**
     * Total size of the segments in bytes.
     *
     * @return Size in bytes.
     */
    public static long totalSize(FileDescriptor... fileDescriptors) {
        long len = 0;

        for (FileDescriptor descriptor : fileDescriptors)
            len += descriptor.file.length();

        return len;
    }

    /** @return WAL cdc directory (including consistent ID as subfolder) */
    @Nullable public File walCdcDirectory() {
        return walCdcDir;
    }

    /**
     * Check if WAL archive is unlimited.
     *
     * @return {@code True} if unlimited.
     */
    private boolean walArchiveUnlimited() {
        return maxWalArchiveSize == UNLIMITED_WAL_ARCHIVE;
    }

    /**
     * Removing files from {@link #walArchiveDir}.
     *
     * @param files Files from {@link #walArchiveDir}.
     * @return Total deleted size in bytes.
     */
    private long deleteArchiveFiles(File... files) {
        long size = 0;

        for (File file : files) {
            if (file.exists()) {
                long len = file.length();

                if (file.delete())
                    size += len;
                else if (file.exists()) {
                    U.warn(log, "Unable to delete file from WAL archive" +
                        " (make sure the process has enough rights):  " + file.getAbsolutePath());
                }
            }
        }

        return size;
    }

    /**
     * Worker for an asynchronous WAL archive cleanup that starts when the maximum size is exceeded.
     * {@link SegmentAware#awaitExceedMaxArchiveSize} is used to determine if the maximum is exceeded.
     */
    private class FileCleaner extends GridWorker {
        /**
         * Constructor.
         *
         * @param log Logger.
         */
        public FileCleaner(IgniteLogger log) {
            super(cctx.igniteInstanceName(), "wal-file-cleaner%" + cctx.igniteInstanceName(), log);

            assert !walArchiveUnlimited();
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                while (!isCancelled()) {
                    segmentAware.awaitExceedMaxArchiveSize(minWalArchiveSize);
                    segmentAware.awaitAvailableTruncateArchive();

                    FileDescriptor[] walArchiveFiles = walArchiveFiles();

                    FileDescriptor high = null;

                    long size = 0;
                    long totalSize = totalSize(walArchiveFiles);

                    for (FileDescriptor fileDesc : walArchiveFiles) {
                        if (fileDesc.idx >= lastCheckpointPtr.index() || segmentAware.reserved(fileDesc.idx))
                            break;
                        else {
                            high = fileDesc;

                            // Ensure that there will be exactly removed at least one segment.
                            if (totalSize - (size += fileDesc.file.length()) < minWalArchiveSize)
                                break;
                        }
                    }

                    if (high != null) {
                        WALPointer highPtr = new WALPointer(high.idx + 1, 0, 0);

                        if (log.isInfoEnabled()) {
                            log.info("Starting to clean WAL archive [highIdx=" + highPtr.index()
                                + ", currSize=" + U.humanReadableByteCount(totalSize)
                                + ", maxSize=" + U.humanReadableByteCount(maxWalArchiveSize) + ']');
                        }

                        cctx.database().onWalTruncated(highPtr);

                        int truncated = truncate(highPtr);

                        if (log.isInfoEnabled()) {
                            log.info("Finish clean WAL archive [cleanCnt=" + truncated
                                + ", currSize=" + U.humanReadableByteCount(totalSize(walArchiveFiles()))
                                + ", maxSize=" + U.humanReadableByteCount(maxWalArchiveSize) + ']');
                        }
                    }
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                Thread.currentThread().interrupt();

                isCancelled.set(true);
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                if (err == null && !isCancelled())
                    err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    failureProcessor.process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    failureProcessor.process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * Shutdown worker.
         *
         * @throws IgniteInterruptedCheckedException If failed to wait for worker shutdown.
         */
        private void shutdown() throws IgniteInterruptedCheckedException {
            isCancelled.set(true);

            U.join(this);
        }

        /**
         * Restart worker in IgniteThread.
         */
        public void restart() {
            assert runner() == null : "FileCleaner is still running";

            isCancelled.set(false);

            new IgniteThread(this).start();
        }
    }

    /**
     * Moving working segments to archive, if segments are more than {@link DataStorageConfiguration#getWalSegments()}
     * or index of first segment is not 0. All segments will be moved except for last one,
     * as well as all compressed segments.
     *
     * @throws StorageException If an error occurs while moving.
     */
    private void moveSegmentsToArchive() throws StorageException {
        assert isArchiverEnabled();

        FileDescriptor[] workSegments = scan(walWorkDir.listFiles(WAL_SEGMENT_FILE_FILTER));

        List<FileDescriptor> toMove = new ArrayList<>();

        if (!F.isEmpty(workSegments) && (workSegments.length > dsCfg.getWalSegments() || workSegments[0].idx() != 0))
            toMove.addAll(F.asList(workSegments).subList(0, workSegments.length - 1));

        toMove.addAll(F.asList(scan(walWorkDir.listFiles(WAL_SEGMENT_FILE_COMPACTED_FILTER))));

        if (!toMove.isEmpty()) {
            log.warning("Content of WAL working directory needs rearrangement, some WAL segments will be moved to " +
                "archive: " + walArchiveDir.getAbsolutePath() + ". Segments from " + toMove.get(0).file().getName() +
                " to " + toMove.get(toMove.size() - 1).file().getName() + " will be moved, total number of files: " +
                toMove.size() + ". This operation may take some time.");

            for (int i = 0, j = 0; i < toMove.size(); i++) {
                FileDescriptor fd = toMove.get(i);

                File tmpDst = new File(walArchiveDir, fd.file().getName() + TMP_SUFFIX);
                File dst = new File(walArchiveDir, fd.file().getName());

                try {
                    Files.copy(fd.file().toPath(), tmpDst.toPath());

                    Files.move(tmpDst.toPath(), dst.toPath());

                    Files.delete(fd.file().toPath());

                    if (log.isDebugEnabled()) {
                        log.debug("WAL segment moved [src=" + fd.file().getAbsolutePath() +
                            ", dst=" + dst.getAbsolutePath() + ']');
                    }

                    // Batch output.
                    if (log.isInfoEnabled() && (i == toMove.size() - 1 || (i != 0 && i % 9 == 0))) {
                        log.info("WAL segments moved: " + toMove.get(j).file().getName() +
                            (i == j ? "" : " - " + toMove.get(i).file().getName()));

                        j = i + 1;
                    }
                }
                catch (IOException e) {
                    throw new StorageException("Failed to move WAL segment [src=" + fd.file().getAbsolutePath() +
                        ", dst=" + dst.getAbsolutePath() + ']', e);
                }
            }
        }
    }

    /**
     * Renaming last segment if it is only one and its index is greater than {@link DataStorageConfiguration#getWalSegments()}.
     *
     * @throws StorageException If an error occurs while renaming.
     */
    private void renameLastSegment() throws StorageException {
        assert isArchiverEnabled();

        FileDescriptor[] workSegments = scan(walWorkDir.listFiles(WAL_SEGMENT_FILE_FILTER));

        if (workSegments.length == 1 && workSegments[0].idx() != workSegments[0].idx() % dsCfg.getWalSegments()) {
            FileDescriptor toRen = workSegments[0];

            if (log.isInfoEnabled()) {
                log.info("Last WAL segment file has to be renamed from " + toRen.file().getName() + " to " +
                    fileName(toRen.idx() % dsCfg.getWalSegments()) + '.');
            }

            String toRenFileName = fileName(toRen.idx() % dsCfg.getWalSegments());

            File tmpDst = new File(walWorkDir, toRenFileName + TMP_SUFFIX);
            File dst = new File(walWorkDir, toRenFileName);

            try {
                Files.copy(toRen.file().toPath(), tmpDst.toPath());

                Files.move(tmpDst.toPath(), dst.toPath());

                Files.delete(toRen.file().toPath());

                if (log.isInfoEnabled()) {
                    log.info("WAL segment renamed [src=" + toRen.file().getAbsolutePath() +
                        ", dst=" + dst.getAbsolutePath() + ']');
                }
            }
            catch (IOException e) {
                throw new StorageException("Failed to rename WAL segment [src=" +
                    toRen.file().getAbsolutePath() + ", dst=" + dst.getAbsolutePath() + ']', e);
            }
        }
    }

    /**
     * Formatting working segments to {@link DataStorageConfiguration#getWalSegmentSize()} for work in a mmap or fsync case.
     *
     * @throws StorageException If an error occurs when formatting.
     */
    private void formatWorkSegments() throws StorageException {
        assert isArchiverEnabled();

        if (mode == WALMode.FSYNC || mmap) {
            List<FileDescriptor> toFormat = Arrays.stream(scan(walWorkDir.listFiles(WAL_SEGMENT_FILE_FILTER)))
                .filter(fd -> fd.file().length() < dsCfg.getWalSegmentSize()).collect(toList());

            if (!toFormat.isEmpty()) {
                if (log.isInfoEnabled()) {
                    log.info("WAL segments in working directory should have the same size: '" +
                        U.humanReadableByteCount(dsCfg.getWalSegmentSize()) + "'. Segments that need reformat " +
                        "found: " + F.viewReadOnly(toFormat, fd -> fd.file().getName()) + '.');
                }

                for (int i = 0, j = 0; i < toFormat.size(); i++) {
                    FileDescriptor fd = toFormat.get(i);

                    File tmpDst = new File(fd.file().getAbsolutePath() + TMP_SUFFIX);

                    try {
                        Files.copy(fd.file().toPath(), tmpDst.toPath());

                        if (log.isDebugEnabled()) {
                            log.debug("Start formatting WAL segment [filePath=" + tmpDst.getAbsolutePath() +
                                ", fileSize=" + U.humanReadableByteCount(tmpDst.length()) +
                                ", toSize=" + U.humanReadableByteCount(dsCfg.getWalSegmentSize()) + ']');
                        }

                        try (FileIO fileIO = ioFactory.create(tmpDst, CREATE, READ, WRITE)) {
                            int left = (int)(dsCfg.getWalSegmentSize() - tmpDst.length());

                            fileIO.position(tmpDst.length());

                            while (left > 0)
                                left -= fileIO.writeFully(FILL_BUF, 0, Math.min(FILL_BUF.length, left));

                            fileIO.force();
                        }

                        Files.move(tmpDst.toPath(), fd.file().toPath(), REPLACE_EXISTING, ATOMIC_MOVE);

                        if (log.isDebugEnabled())
                            log.debug("WAL segment formatted: " + fd.file().getAbsolutePath());

                        // Batch output.
                        if (log.isInfoEnabled() && (i == toFormat.size() - 1 || (i != 0 && i % 9 == 0))) {
                            log.info("WAL segments formatted: " + toFormat.get(j).file().getName() +
                                (i == j ? "" : " - " + fileName(i)));

                            j = i + 1;
                        }
                    }
                    catch (IOException e) {
                        throw new StorageException("Failed to format WAL segment: " + fd.file().getAbsolutePath(), e);
                    }
                }
            }
        }
    }

    /**
     * Timeout object for automatically rollover segments if the recording
     * to the WAL was not more than or equal to {@link #walAutoArchiveAfterInactivity}.
     */
    private class TimeoutRollover implements GridTimeoutObject {
        /** ID of timeout object. */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** Timestamp for triggering. */
        private final long endTime;

        /** Cancel flag. */
        private boolean cancel;

        /**
         * Constructor.
         *
         * @param endTime Timestamp for triggering.
         */
        private TimeoutRollover(long endTime) {
            if (log.isDebugEnabled())
                log.debug("Schedule WAL rollover check at " + new Time(endTime).toString());

            this.endTime = endTime;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            assert walAutoArchiveAfterInactivity > 0 || walForceArchiveTimeout > 0;
            assert timeoutRolloverMux != null;

            synchronized (timeoutRolloverMux) {
                timeoutRollover = null;

                if (!cancel) {
                    if (log.isDebugEnabled())
                        log.debug("Checking if WAL rollover required (" + new Time(U.currentTimeMillis()) + ")");

                    checkWalRolloverRequired();

                    scheduleNextRolloverCheck();
                }
            }
        }

        /**
         * Cancel auto rollover.
         */
        public void cancel() {
            assert walAutoArchiveAfterInactivity > 0 || walForceArchiveTimeout > 0;
            assert timeoutRolloverMux != null;

            synchronized (timeoutRolloverMux) {
                if (log.isDebugEnabled())
                    log.debug("Auto rollover is canceled");

                cancel = true;
            }
        }
    }

    /**
     * Stop auto rollover.
     */
    private void stopAutoRollover() {
        if (walAutoArchiveAfterInactivity > 0 || walForceArchiveTimeout > 0) {
            assert timeoutRolloverMux != null;

            synchronized (timeoutRolloverMux) {
                TimeoutRollover timeoutRollover = this.timeoutRollover;

                if (timeoutRollover != null) {
                    timeoutRollover.cancel();

                    cctx.time().removeTimeoutObject(timeoutRollover);

                    this.timeoutRollover = null;
                }
            }
        }
    }

    /**
     * Getting min size(in bytes) of WAL archive directory(greater than 0,
     * or {@link DataStorageConfiguration#UNLIMITED_WAL_ARCHIVE} if max WAL archive size is unlimited).
     *
     * @param dsCfg Memory configuration.
     * @return Min allowed size(in bytes) of WAL archives.
     */
    static long minWalArchiveSize(DataStorageConfiguration dsCfg) {
        long max = dsCfg.getMaxWalArchiveSize();
        long min = dsCfg.getMinWalArchiveSize();

        double percentage = getDouble(IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, -1);

        return max == UNLIMITED_WAL_ARCHIVE ? max : min != HALF_MAX_WAL_ARCHIVE_SIZE ? min :
            percentage == -1 ? max / 2 : (long)(max * percentage);
    }

    /** {@inheritDoc} */
    @Override public void startAutoReleaseSegments() {
        segmentAware.startAutoReleaseSegments();
    }
}
