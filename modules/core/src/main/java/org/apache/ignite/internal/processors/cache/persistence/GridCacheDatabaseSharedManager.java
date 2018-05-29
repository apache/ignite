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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.management.ObjectName;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionDestroyRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionMetaStateRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.PersistentStorageIOException;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperation;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.mxbean.DataStorageMetricsMXBean;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_SKIP_CRC;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_ID;

/**
 *
 */
@SuppressWarnings({"unchecked", "NonPrivateFieldAccessedInSynchronizedContext"})
public class GridCacheDatabaseSharedManager extends IgniteCacheDatabaseSharedManager implements CheckpointWriteProgressSupplier {
    /** */
    public static final String IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC = "IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC";

    /** MemoryPolicyConfiguration name reserved for meta store. */
    private static final String METASTORE_DATA_REGION_NAME = "metastoreMemPlc";

    /** */
    private static final long GB = 1024L * 1024 * 1024;

    /** Minimum checkpointing page buffer size (may be adjusted by Ignite). */
    public static final Long DFLT_MIN_CHECKPOINTING_PAGE_BUFFER_SIZE = GB / 4;

    /** Default minimum checkpointing page buffer size (may be adjusted by Ignite). */
    public static final Long DFLT_MAX_CHECKPOINTING_PAGE_BUFFER_SIZE = 2 * GB;

    /** Skip sync. */
    private final boolean skipSync = IgniteSystemProperties.getBoolean(IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);

    /** */
    private boolean skipCrc = IgniteSystemProperties.getBoolean(IGNITE_PDS_SKIP_CRC, false);

    /** */
    private final int walRebalanceThreshold = IgniteSystemProperties.getInteger(
        IGNITE_PDS_WAL_REBALANCE_THRESHOLD, 500_000);

    /** Value of property for throttling policy override. */
    private final String throttlingPolicyOverride = IgniteSystemProperties.getString(
        IgniteSystemProperties.IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED);

    /** Checkpoint lock hold count. */
    private static final ThreadLocal<Integer> CHECKPOINT_LOCK_HOLD_COUNT = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return 0;
        }
    };

    /** Assertion enabled. */
    private static final boolean ASSERTION_ENABLED = GridCacheDatabaseSharedManager.class.desiredAssertionStatus();

    /** Checkpoint file name pattern. */
    private static final Pattern CP_FILE_NAME_PATTERN = Pattern.compile("(\\d+)-(.*)-(START|END)\\.bin");

    /** Checkpoint file temporary suffix. This is needed to safe writing checkpoint markers through temporary file and renaming. */
    public static final String FILE_TMP_SUFFIX = ".tmp";

    /** Node started file patter. */
    private static final Pattern NODE_STARTED_FILE_NAME_PATTERN = Pattern.compile("(\\d+)-node-started\\.bin");

    /** Node started file suffix. */
    public static final String NODE_STARTED_FILE_NAME_SUFFIX = "-node-started.bin";

    /** */
    private static final FileFilter CP_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File f) {
            return CP_FILE_NAME_PATTERN.matcher(f.getName()).matches();
        }
    };

    /** */
    private static final FileFilter NODE_STARTED_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File f) {
            return f.getName().endsWith(NODE_STARTED_FILE_NAME_SUFFIX);
        }
    };

    /** */
    private static final Comparator<GridDhtLocalPartition> ASC_PART_COMPARATOR = new Comparator<GridDhtLocalPartition>() {
        @Override public int compare(GridDhtLocalPartition a, GridDhtLocalPartition b) {
            return Integer.compare(a.id(), b.id());
        }
    };

    /** */
    private static final Comparator<File> CP_TS_COMPARATOR = new Comparator<File>() {
        /** {@inheritDoc} */
        @Override public int compare(File o1, File o2) {
            Matcher m1 = CP_FILE_NAME_PATTERN.matcher(o1.getName());
            Matcher m2 = CP_FILE_NAME_PATTERN.matcher(o2.getName());

            boolean s1 = m1.matches();
            boolean s2 = m2.matches();

            assert s1 : "Failed to match CP file: " + o1.getAbsolutePath();
            assert s2 : "Failed to match CP file: " + o2.getAbsolutePath();

            long ts1 = Long.parseLong(m1.group(1));
            long ts2 = Long.parseLong(m2.group(1));

            int res = Long.compare(ts1, ts2);

            if (res == 0) {
                CheckpointEntryType type1 = CheckpointEntryType.valueOf(m1.group(3));
                CheckpointEntryType type2 = CheckpointEntryType.valueOf(m2.group(3));

                assert type1 != type2 : "o1=" + o1.getAbsolutePath() + ", o2=" + o2.getAbsolutePath();

                res = type1 == CheckpointEntryType.START ? -1 : 1;
            }

            return res;
        }
    };

    /** */
    private static final String MBEAN_NAME = "DataStorageMetrics";

    /** */
    private static final String MBEAN_GROUP = "Persistent Store";

    /** WAL marker prefix for meta store. */
    private static final String WAL_KEY_PREFIX = "grp-wal-";

    /** WAL marker prefix for meta store. */
    private static final String WAL_GLOBAL_KEY_PREFIX = WAL_KEY_PREFIX + "disabled-";

    /** WAL marker prefix for meta store. */
    private static final String WAL_LOCAL_KEY_PREFIX = WAL_KEY_PREFIX + "local-disabled-";

    /** WAL marker predicate for meta store. */
    private static final IgnitePredicate<String> WAL_KEY_PREFIX_PRED = new IgnitePredicate<String>() {
        @Override public boolean apply(String key) {
            return key.startsWith(WAL_KEY_PREFIX);
        }
    };

    /** Timeout between partition file destroy and checkpoint to handle it. */
    private static final long PARTITION_DESTROY_CHECKPOINT_TIMEOUT = 30 * 1000; // 30 Seconds.

    /** Checkpoint thread. Needs to be volatile because it is created in exchange worker. */
    private volatile Checkpointer checkpointer;

    /** For testing only. */
    private volatile boolean checkpointsEnabled = true;

    /** For testing only. */
    private volatile GridFutureAdapter<Void> enableChangeApplied;

    /** */
    private ReentrantReadWriteLock checkpointLock = new ReentrantReadWriteLock();

    /** */
    private long checkpointFreq;

    /** */
    private FilePageStoreManager storeMgr;

    /** Checkpoint metadata directory ("cp"), contains files with checkpoint start and end */
    private File cpDir;

    /** */
    private volatile boolean printCheckpointStats = true;

    /** Database configuration. */
    private final DataStorageConfiguration persistenceCfg;

    /** */
    private final Collection<DbCheckpointListener> lsnrs = new CopyOnWriteArrayList<>();

    /** Checkpoint history. */
    private final CheckpointHistory checkpointHist = new CheckpointHistory();

    /** */
    private boolean stopping;

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private ExecutorService asyncRunner;

    /** Thread local with buffers for the checkpoint threads. Each buffer represent one page for durable memory. */
    private ThreadLocal<ByteBuffer> threadBuf;

    /** Map from a cacheId to a future indicating that there is an in-progress index rebuild for the given cache. */
    private final ConcurrentMap<Integer, GridFutureAdapter<Void>> idxRebuildFuts = new ConcurrentHashMap<>();

    /**
     * Lock holder for compatible folders mode. Null if lock holder was created at start node. <br>
     * In this case lock is held on PDS resover manager and it is not required to manage locking here
     */
    @Nullable private FileLockHolder fileLockHolder;

    /** Lock wait time. */
    private final long lockWaitTime;

    /** */
    private final int maxCpHistMemSize;

    /** */
    private Map</*grpId*/Integer, Map</*partId*/Integer, T2</*updCntr*/Long, WALPointer>>> reservedForExchange;

    /** */
    private final ConcurrentMap<T2</*grpId*/Integer, /*partId*/Integer>, T2</*updCntr*/Long, WALPointer>> reservedForPreloading = new ConcurrentHashMap<>();

    /** Snapshot manager. */
    private IgniteCacheSnapshotManager snapshotMgr;

    /** */
    private DataStorageMetricsImpl persStoreMetrics;

    /** */
    private ObjectName persistenceMetricsMbeanName;

    /** Counter for written checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger writtenPagesCntr = null;

    /** Counter for fsynced checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger syncedPagesCntr = null;

    /** Counter for evicted checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger evictedPagesCntr = null;

    /** Number of pages in current checkpoint at the beginning of checkpoint. */
    private volatile int currCheckpointPagesCnt;

    /** */
    private MetaStorage metaStorage;

    /** */
    private List<MetastorageLifecycleListener> metastorageLifecycleLsnrs;

    /** Initially disabled cache groups. */
    private Collection<Integer> initiallyGlobalWalDisabledGrps = new HashSet<>();

    /** Initially local wal disabled groups. */
    private Collection<Integer> initiallyLocalWalDisabledGrps = new HashSet<>();

    /** File I/O factory for writing checkpoint markers. */
    private final FileIOFactory ioFactory;

    /**
     * @param ctx Kernal context.
     */
    public GridCacheDatabaseSharedManager(GridKernalContext ctx) {
        IgniteConfiguration cfg = ctx.config();

        persistenceCfg = cfg.getDataStorageConfiguration();

        assert persistenceCfg != null;

        checkpointFreq = persistenceCfg.getCheckpointFrequency();

        lockWaitTime = persistenceCfg.getLockWaitTime();

        persStoreMetrics = new DataStorageMetricsImpl(
            persistenceCfg.isMetricsEnabled(),
            persistenceCfg.getMetricsRateTimeInterval(),
            persistenceCfg.getMetricsSubIntervalCount()
        );

        metastorageLifecycleLsnrs = ctx.internalSubscriptionProcessor().getMetastorageSubscribers();

        maxCpHistMemSize = Math.min(persistenceCfg.getWalHistorySize(),
            IgniteSystemProperties.getInteger(IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, 100));

        ioFactory = persistenceCfg.getFileIOFactory();
    }

    /** */
    private void notifyMetastorageReadyForRead() throws IgniteCheckedException {
        for (MetastorageLifecycleListener lsnr : metastorageLifecycleLsnrs)
            lsnr.onReadyForRead(metaStorage);
    }

    /** */
    private void notifyMetastorageReadyForReadWrite() throws IgniteCheckedException {
        for (MetastorageLifecycleListener lsnr : metastorageLifecycleLsnrs)
            lsnr.onReadyForReadWrite(metaStorage);
    }

    /**
     *
     */
    public Checkpointer getCheckpointer() {
        return checkpointer;
    }

    /**
     * For test use only.
     */
    public IgniteInternalFuture<Void> enableCheckpoints(boolean enable) {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        enableChangeApplied = fut;

        checkpointsEnabled = enable;

        wakeupForCheckpoint("enableCheckpoints()");

        return fut;
    }

    /** {@inheritDoc} */
    @Override protected void initDataRegions(DataStorageConfiguration memCfg) throws IgniteCheckedException {
        super.initDataRegions(memCfg);

        addDataRegion(
            memCfg,
            createDataRegionConfiguration(memCfg),
            false
        );

        persStoreMetrics.regionMetrics(memMetricsMap.values());
    }

    /**
     * @param storageCfg Data storage configuration.
     * @return Data region configuration.
     */
    private DataRegionConfiguration createDataRegionConfiguration(DataStorageConfiguration storageCfg) {
        DataRegionConfiguration cfg = new DataRegionConfiguration();

        cfg.setName(METASTORE_DATA_REGION_NAME);
        cfg.setInitialSize(storageCfg.getSystemRegionInitialSize());
        cfg.setMaxSize(storageCfg.getSystemRegionMaxSize());
        cfg.setPersistenceEnabled(true);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        threadBuf = new ThreadLocal<ByteBuffer>() {
            /** {@inheritDoc} */
            @Override protected ByteBuffer initialValue() {
                ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(pageSize());

                tmpWriteBuf.order(ByteOrder.nativeOrder());

                return tmpWriteBuf;
            }
        };

        snapshotMgr = cctx.snapshot();

        final GridKernalContext kernalCtx = cctx.kernalContext();

        if (!kernalCtx.clientNode()) {
            checkpointer = new Checkpointer(cctx.igniteInstanceName(), "db-checkpoint-thread", log);

            IgnitePageStoreManager store = cctx.pageStore();

            assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

            storeMgr = (FilePageStoreManager)store;

            cpDir = Paths.get(storeMgr.workDir().getAbsolutePath(), "cp").toFile();

            if (!U.mkdirs(cpDir))
                throw new IgniteCheckedException("Could not create directory for checkpoint metadata: " + cpDir);

            cleanupTempCheckpointDirectory();

            final FileLockHolder preLocked = kernalCtx.pdsFolderResolver()
                .resolveFolders()
                .getLockedFileLockHolder();
            if (preLocked == null)
                fileLockHolder = new FileLockHolder(storeMgr.workDir().getPath(), kernalCtx, log);

            persStoreMetrics.wal(cctx.wal());

            // Here we can get data from metastorage
            readMetastore();
        }
    }

    /**
     * Cleanup checkpoint directory from all temporary files {@link #FILE_TMP_SUFFIX}.
     */
    private void cleanupTempCheckpointDirectory() throws IgniteCheckedException {
        try {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(cpDir.toPath(), new DirectoryStream.Filter<Path>() {
                @Override
                public boolean accept(Path path) throws IOException {
                    return path.endsWith(FILE_TMP_SUFFIX);
                }
            })) {
                for (Path path : files)
                    Files.delete(path);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to cleanup checkpoint directory from temporary files: " + cpDir, e);
        }
    }

    /**
     * Cleanup checkpoint directory.
     */
    public void cleanupCheckpointDirectory() throws IgniteCheckedException {
        try {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(cpDir.toPath())) {
                for (Path path : files)
                    Files.delete(path);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to cleanup checkpoint directory: " + cpDir, e);
        }
    }

    /**
     *
     */
    private void initDataBase() {
        if (persistenceCfg.getCheckpointThreads() > 1)
            asyncRunner = new IgniteThreadPoolExecutor(
                "checkpoint-runner",
                cctx.igniteInstanceName(),
                persistenceCfg.getCheckpointThreads(),
                persistenceCfg.getCheckpointThreads(),
                30_000,
                new LinkedBlockingQueue<Runnable>()
            );
    }

    /** */
    private void readMetastore() throws IgniteCheckedException {
        try {
            DataStorageConfiguration memCfg = cctx.kernalContext().config().getDataStorageConfiguration();

            DataRegionConfiguration plcCfg = createDataRegionConfiguration(memCfg);

            File allocPath = buildAllocPath(plcCfg);

            DirectMemoryProvider memProvider = allocPath == null ?
                new UnsafeMemoryProvider(log) :
                new MappedFileMemoryProvider(
                    log,
                    allocPath);

            DataRegionMetricsImpl memMetrics = new DataRegionMetricsImpl(plcCfg);

            PageMemoryEx storePageMem = (PageMemoryEx)createPageMemory(memProvider, memCfg, plcCfg, memMetrics, false);

            DataRegion regCfg = new DataRegion(storePageMem, plcCfg, memMetrics, createPageEvictionTracker(plcCfg, storePageMem));

            CheckpointStatus status = readCheckpointStatus();

            cctx.pageStore().initializeForMetastorage();

            storePageMem.start();

            checkpointReadLock();

            try {
                restoreMemory(status, true, storePageMem);

                metaStorage = new MetaStorage(cctx, regCfg, memMetrics, true);

                metaStorage.init(this);

                applyLastUpdates(status, true);

                fillWalDisabledGroups();

                notifyMetastorageReadyForRead();
            }
            finally {
                checkpointReadUnlock();
            }

            metaStorage = null;

            storePageMem.stop();
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Get checkpoint buffer size for the given configuration.
     *
     * @param regCfg Configuration.
     * @return Checkpoint buffer size.
     */
    public static long checkpointBufferSize(DataRegionConfiguration regCfg) {
        if (!regCfg.isPersistenceEnabled())
            return 0L;

        long res = regCfg.getCheckpointPageBufferSize();

        if (res == 0L) {
            if (regCfg.getMaxSize() < GB)
                res = Math.min(DFLT_MIN_CHECKPOINTING_PAGE_BUFFER_SIZE, regCfg.getMaxSize());
            else if (regCfg.getMaxSize() < 8 * GB)
                res = regCfg.getMaxSize() / 4;
            else
                res = DFLT_MAX_CHECKPOINTING_PAGE_BUFFER_SIZE;
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext ctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate database manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        snapshotMgr = cctx.snapshot();

        if (!cctx.localNode().isClient()) {
            initDataBase();

            registrateMetricsMBean();
        }

        if (checkpointer == null)
            checkpointer = new Checkpointer(cctx.igniteInstanceName(), "db-checkpoint-thread", log);

        super.onActivate(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate database manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        onKernalStop0(false);

        stop0(false);

        /* Must be here, because after deactivate we can invoke activate and file lock must be already configured */
        stopping = false;

        if (!cctx.localNode().isClient()) {
            //we replace lock with new instance (only if we're responsible for locking folders)
            if (fileLockHolder != null)
                fileLockHolder = new FileLockHolder(storeMgr.workDir().getPath(), cctx.kernalContext(), log);
        }
    }

    /**
     * Try to register Metrics MBean.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void registrateMetricsMBean() throws IgniteCheckedException {
        if (U.IGNITE_MBEANS_DISABLED)
            return;

        try {
            persistenceMetricsMbeanName = U.registerMBean(
                cctx.kernalContext().config().getMBeanServer(),
                cctx.kernalContext().igniteInstanceName(),
                MBEAN_GROUP,
                MBEAN_NAME,
                persStoreMetrics,
                DataStorageMetricsMXBean.class);
        }
        catch (Throwable e) {
            throw new IgniteCheckedException("Failed to register " + MBEAN_NAME + " MBean.", e);
        }
    }

    /**
     * Unregister metrics MBean.
     */
    private void unRegistrateMetricsMBean() {
        if (persistenceMetricsMbeanName == null)
            return;

        assert !U.IGNITE_MBEANS_DISABLED;

        try {
            cctx.kernalContext().config().getMBeanServer().unregisterMBean(persistenceMetricsMbeanName);

            persistenceMetricsMbeanName = null;
        }
        catch (Throwable e) {
            U.error(log, "Failed to unregister " + MBEAN_NAME + " MBean.", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteOutClosure<Long> freeSpaceProvider(final DataRegionConfiguration dataRegCfg) {
        if (!dataRegCfg.isPersistenceEnabled())
            return super.freeSpaceProvider(dataRegCfg);

        final String dataRegName = dataRegCfg.getName();

        return new IgniteOutClosure<Long>() {
            @Override public Long apply() {
                long freeSpace = 0L;

                for (CacheGroupContext grpCtx : cctx.cache().cacheGroups()) {
                    if (!grpCtx.dataRegion().config().getName().equals(dataRegName))
                        continue;

                    assert grpCtx.offheap() instanceof GridCacheOffheapManager;

                    freeSpace += ((GridCacheOffheapManager)grpCtx.offheap()).freeSpace();
                }

                return freeSpace;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void readCheckpointAndRestoreMemory(
        List<DynamicCacheDescriptor> cachesToStart
    ) throws IgniteCheckedException {
        assert !cctx.localNode().isClient();

        checkpointReadLock();

        try {
            if (!F.isEmpty(cachesToStart)) {
                for (DynamicCacheDescriptor desc : cachesToStart) {
                    if (CU.affinityNode(cctx.localNode(), desc.cacheConfiguration().getNodeFilter()))
                        storeMgr.initializeForCache(desc.groupDescriptor(), new StoredCacheData(desc.cacheConfiguration()));
                }
            }

            CheckpointStatus status = readCheckpointStatus();

            cctx.pageStore().initializeForMetastorage();

            metaStorage = new MetaStorage(cctx, dataRegionMap.get(METASTORE_DATA_REGION_NAME),
                (DataRegionMetricsImpl)memMetricsMap.get(METASTORE_DATA_REGION_NAME));

            WALPointer restore = restoreMemory(status);

            if (restore == null && !status.endPtr.equals(CheckpointStatus.NULL_PTR)) {
                throw new StorageException("Restore wal pointer = " + restore + ", while status.endPtr = " +
                    status.endPtr + ". Can't restore memory - critical part of WAL archive is missing.");
            }

            // First, bring memory to the last consistent checkpoint state if needed.
            // This method should return a pointer to the last valid record in the WAL.

            cctx.wal().resumeLogging(restore);

            WALPointer ptr = cctx.wal().log(new MemoryRecoveryRecord(U.currentTimeMillis()));

            if (ptr != null) {
                cctx.wal().flush(ptr, true);

                nodeStart(ptr);
            }

            metaStorage.init(this);

            notifyMetastorageReadyForReadWrite();
        }
        catch (StorageException | PersistentStorageIOException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
        finally {
            checkpointReadUnlock();
        }
    }

    /**
     * Creates file with current timestamp and specific "node-started.bin" suffix
     * and writes into memory recovery pointer.
     *
     * @param ptr Memory recovery wal pointer.
     */
    private void nodeStart(WALPointer ptr) throws IgniteCheckedException {
        FileWALPointer p = (FileWALPointer)ptr;

        String fileName = U.currentTimeMillis() + NODE_STARTED_FILE_NAME_SUFFIX;
        String tmpFileName = fileName + FILE_TMP_SUFFIX;

        ByteBuffer buf = ByteBuffer.allocate(20);
        buf.order(ByteOrder.nativeOrder());

        try {
            try (FileIO io = ioFactory.create(Paths.get(cpDir.getAbsolutePath(), tmpFileName).toFile(),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                buf.putLong(p.index());

                buf.putInt(p.fileOffset());

                buf.putInt(p.length());

                buf.flip();

                io.write(buf);

                buf.clear();

                io.force(true);
            }

            Files.move(Paths.get(cpDir.getAbsolutePath(), tmpFileName), Paths.get(cpDir.getAbsolutePath(), fileName));
        }
        catch (IOException e) {
            throw new PersistentStorageIOException("Failed to write node start marker: " + ptr, e);
        }
    }

    /**
     * Collects memory recovery pointers from node started files. See {@link #nodeStart(WALPointer)}.
     * Each pointer associated with timestamp extracted from file.
     * Tuples are sorted by timestamp.
     *
     * @return Sorted list of tuples (node started timestamp, memory recovery pointer).
     * @throws IgniteCheckedException
     */
    public List<T2<Long, WALPointer>> nodeStartedPointers() throws IgniteCheckedException {
        List<T2<Long, WALPointer>> res = new ArrayList<>();

        File[] files = cpDir.listFiles(NODE_STARTED_FILE_FILTER);

        Arrays.sort(files, new Comparator<File>() {
            @Override public int compare(File o1, File o2) {
                String n1 = o1.getName();
                String n2 = o2.getName();

                long ts1 = Long.valueOf(n1.substring(0, n1.length() - NODE_STARTED_FILE_NAME_SUFFIX.length()));
                long ts2 = Long.valueOf(n2.substring(0, n2.length() - NODE_STARTED_FILE_NAME_SUFFIX.length()));

                return Long.compare(ts1, ts2);
            }
        });

        ByteBuffer buf = ByteBuffer.allocate(20);
        buf.order(ByteOrder.nativeOrder());

        for (File f : files){
            String name = f.getName();

            Long ts = Long.valueOf(name.substring(0, name.length() - NODE_STARTED_FILE_NAME_SUFFIX.length()));

            try (FileIO io = ioFactory.create(f, READ)) {
                io.read(buf);

                buf.flip();

                FileWALPointer ptr = new FileWALPointer(
                    buf.getLong(), buf.getInt(), buf.getInt());

                res.add(new T2<Long, WALPointer>(ts, ptr));

                buf.clear();
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to read node started marker file: " + f.getAbsolutePath(), e);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void lock() throws IgniteCheckedException {
        if (fileLockHolder != null) {
            if (log.isDebugEnabled())
                log.debug("Try to capture file lock [nodeId=" +
                    cctx.localNodeId() + " path=" + fileLockHolder.lockPath() + "]");

            fileLockHolder.tryLock(lockWaitTime);
        }
    }

    /** {@inheritDoc} */
    @Override public void unLock() {
        if (fileLockHolder != null) {
            if (log.isDebugEnabled())
                log.debug("Release file lock [nodeId=" +
                    cctx.localNodeId() + " path=" + fileLockHolder.lockPath() + "]");

            fileLockHolder.release();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        checkpointLock.writeLock().lock();

        try {
            stopping = true;
        }
        finally {
            checkpointLock.writeLock().unlock();
        }

        shutdownCheckpointer(cancel);

        lsnrs.clear();

        super.onKernalStop0(cancel);

        if (!cctx.kernalContext().clientNode()) {
            unLock();

            if (fileLockHolder != null)
                fileLockHolder.close();
        }

        unRegistrateMetricsMBean();
    }

    /** */
    private long[] calculateFragmentSizes(int concLvl, long cacheSize, long chpBufSize) {
        if (concLvl < 2)
            concLvl = Runtime.getRuntime().availableProcessors();

        long fragmentSize = cacheSize / concLvl;

        if (fragmentSize < 1024 * 1024)
            fragmentSize = 1024 * 1024;

        long[] sizes = new long[concLvl + 1];

        for (int i = 0; i < concLvl; i++)
            sizes[i] = fragmentSize;

        sizes[concLvl] = chpBufSize;

        return sizes;
    }

    /** {@inheritDoc} */
    @Override protected PageMemory createPageMemory(
        DirectMemoryProvider memProvider,
        DataStorageConfiguration memCfg,
        DataRegionConfiguration plcCfg,
        DataRegionMetricsImpl memMetrics,
        final boolean trackable
    ) {
        if (!plcCfg.isPersistenceEnabled())
            return super.createPageMemory(memProvider, memCfg, plcCfg, memMetrics, trackable);

        memMetrics.persistenceEnabled(true);

        long cacheSize = plcCfg.getMaxSize();

        // Checkpoint buffer size can not be greater than cache size, it does not make sense.
        long chpBufSize = checkpointBufferSize(plcCfg);

        if (chpBufSize > cacheSize) {
            U.quietAndInfo(log,
                "Configured checkpoint page buffer size is too big, setting to the max region size [size="
                    + U.readableSize(cacheSize, false) + ",  memPlc=" + plcCfg.getName() + ']');

            chpBufSize = cacheSize;
        }

        GridInClosure3X<Long, FullPageId, PageMemoryEx> changeTracker;

        if (trackable)
            changeTracker = new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(
                    Long page,
                    FullPageId fullId,
                    PageMemoryEx pageMem
                ) throws IgniteCheckedException {
                    if (trackable)
                        snapshotMgr.onChangeTrackerPage(page, fullId, pageMem);
                }
            };
        else
            changeTracker = null;

        PageMemoryImpl pageMem = new PageMemoryImpl(
            wrapMetricsMemoryProvider(memProvider, memMetrics),
            calculateFragmentSizes(
                memCfg.getConcurrencyLevel(),
                cacheSize,
                chpBufSize
            ),
            cctx,
            memCfg.getPageSize(),
            (fullId, pageBuf, tag) -> {
                memMetrics.onPageWritten();

                // First of all, write page to disk.
                storeMgr.write(fullId.groupId(), fullId.pageId(), pageBuf, tag);

                // Only after write we can write page into snapshot.
                snapshotMgr.flushDirtyPageHandler(fullId, pageBuf, tag);

                AtomicInteger cntr = evictedPagesCntr;

                if (cntr != null)
                    cntr.incrementAndGet();
            },
            changeTracker,
            this,
            memMetrics,
            resolveThrottlingPolicy(),
            this
        );

        memMetrics.pageMemory(pageMem);

        return pageMem;
    }

    /**
     * @param memoryProvider0 Memory provider.
     * @param memMetrics Memory metrics.
     * @return Wrapped memory provider.
     */
    @Override protected DirectMemoryProvider wrapMetricsMemoryProvider(
            final DirectMemoryProvider memoryProvider0,
            final DataRegionMetricsImpl memMetrics
    ) {
        return new DirectMemoryProvider() {
            private AtomicInteger checkPointBufferIdxCnt = new AtomicInteger();

            private final DirectMemoryProvider memProvider = memoryProvider0;

            @Override public void initialize(long[] chunkSizes) {
                memProvider.initialize(chunkSizes);

                checkPointBufferIdxCnt.set(chunkSizes.length);
            }

            @Override public void shutdown() {
                memProvider.shutdown();
            }

            @Override public DirectMemoryRegion nextRegion() {
                DirectMemoryRegion nextMemoryRegion = memProvider.nextRegion();

                if (nextMemoryRegion == null)
                    return null;

                int idx = checkPointBufferIdxCnt.decrementAndGet();

                long chunkSize = nextMemoryRegion.size();

                // Checkpoint chunk last in the long[] chunkSizes.
                if (idx != 0)
                    memMetrics.updateOffHeapSize(chunkSize);
                else
                    memMetrics.updateCheckpointBufferSize(chunkSize);

                return nextMemoryRegion;
            }
        };
    }

    /**
     * Resolves throttling policy according to the settings.
     */
    @NotNull private PageMemoryImpl.ThrottlingPolicy resolveThrottlingPolicy() {
        PageMemoryImpl.ThrottlingPolicy plc = persistenceCfg.isWriteThrottlingEnabled()
            ? PageMemoryImpl.ThrottlingPolicy.SPEED_BASED
            : PageMemoryImpl.ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY;

        if (throttlingPolicyOverride != null) {
            try {
                plc = PageMemoryImpl.ThrottlingPolicy.valueOf(throttlingPolicyOverride.toUpperCase());
            }
            catch (IllegalArgumentException e) {
                log.error("Incorrect value of IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED property: " +
                    throttlingPolicyOverride + ". Default throttling policy " + plc + " will be used.");
            }
        }
        return plc;
    }

    /** {@inheritDoc} */
    @Override protected void checkRegionEvictionProperties(DataRegionConfiguration regCfg, DataStorageConfiguration dbCfg)
        throws IgniteCheckedException {
        if (!regCfg.isPersistenceEnabled())
            super.checkRegionEvictionProperties(regCfg, dbCfg);
        else if (regCfg.getPageEvictionMode() != DataPageEvictionMode.DISABLED) {
            U.warn(log, "Page eviction mode set for [" + regCfg.getName() + "] data will have no effect" +
                " because the oldest pages are evicted automatically if Ignite persistence is enabled.");
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkPageSize(DataStorageConfiguration memCfg) {
        if (memCfg.getPageSize() == 0) {
            try {
                assert cctx.pageStore() instanceof FilePageStoreManager :
                    "Invalid page store manager was created: " + cctx.pageStore();

                Path anyIdxPartFile = IgniteUtils.searchFileRecursively(
                    ((FilePageStoreManager)cctx.pageStore()).workDir().toPath(), FilePageStoreManager.INDEX_FILE_NAME);

                if (anyIdxPartFile != null) {
                    memCfg.setPageSize(resolvePageSizeFromPartitionFile(anyIdxPartFile));

                    return;
                }
            }
            catch (IgniteCheckedException | IOException | IllegalArgumentException e) {
                U.quietAndWarn(log, "Attempt to resolve pageSize from store files failed: " + e.getMessage());

                U.quietAndWarn(log, "Default page size will be used: " + DataStorageConfiguration.DFLT_PAGE_SIZE + " bytes");
            }

            memCfg.setPageSize(DataStorageConfiguration.DFLT_PAGE_SIZE);
        }
    }

    /**
     * @param partFile Partition file.
     */
    private int resolvePageSizeFromPartitionFile(Path partFile) throws IOException, IgniteCheckedException {
        try (FileIO fileIO = persistenceCfg.getFileIOFactory().create(partFile.toFile())) {
            int minimalHdr = FilePageStore.HEADER_SIZE;

            if (fileIO.size() < minimalHdr)
                throw new IgniteCheckedException("Partition file is too small: " + partFile);

            ByteBuffer hdr = ByteBuffer.allocate(minimalHdr).order(ByteOrder.LITTLE_ENDIAN);

            while (hdr.remaining() > 0)
                fileIO.read(hdr);

            hdr.rewind();

            hdr.getLong(); // Read signature.

            hdr.getInt(); // Read version.

            hdr.get(); // Read type.

            int pageSize = hdr.getInt();

            if (pageSize == 2048) {
                U.quietAndWarn(log, "You are currently using persistent store with 2K pages (DataStorageConfiguration#" +
                    "pageSize). If you use SSD disk, consider migrating to 4K pages for better IO performance.");
            }

            return pageSize;
        }
    }

    /**
     * @param cancel Cancel flag.
     */
    @SuppressWarnings("unused")
    private void shutdownCheckpointer(boolean cancel) {
        Checkpointer cp = checkpointer;

        if (cp != null) {
            if (cancel)
                cp.shutdownNow();
            else
                cp.cancel();

            try {
                U.join(cp);

                checkpointer = null;
            }
            catch (IgniteInterruptedCheckedException ignore) {
                U.warn(log, "Was interrupted while waiting for checkpointer shutdown, " +
                    "will not wait for checkpoint to finish.");

                cp.shutdownNow();

                while (true) {
                    try {
                        U.join(cp);

                        checkpointer = null;

                        cp.scheduledCp.cpFinishFut.onDone(
                            new NodeStoppingException("Checkpointer is stopped during node stop."));

                        break;
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        //Ignore
                    }
                }

                Thread.currentThread().interrupt();
            }
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
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        DiscoveryEvent discoEvt = fut.firstEvent();

        boolean joinEvt = discoEvt.type() == EventType.EVT_NODE_JOINED;

        boolean locNode = discoEvt.eventNode().isLocal();

        boolean isSrvNode = !cctx.kernalContext().clientNode();

        boolean clusterInTransitionStateToActive = fut.activateCluster();

        // In case of cluster activation or local join restore, restore whole manager state.
        if (clusterInTransitionStateToActive || (joinEvt && locNode && isSrvNode)) {
            restoreState();
        }
        // In case of starting groups, restore partition states only for these groups.
        else if (fut.exchangeActions() != null && !F.isEmpty(fut.exchangeActions().cacheGroupsToStart())) {
            Set<Integer> restoreGroups = fut.exchangeActions().cacheGroupsToStart().stream()
                .map(actionData -> actionData.descriptor().groupId())
                .collect(Collectors.toSet());

            restorePartitionStates(Collections.emptyMap(), restoreGroups);
        }

        if (cctx.kernalContext().query().moduleEnabled()) {
            ExchangeActions acts = fut.exchangeActions();

            if (acts != null) {
                if (!F.isEmpty(acts.cacheStartRequests())) {
                    for (ExchangeActions.CacheActionData actionData : acts.cacheStartRequests())
                        prepareIndexRebuildFuture(CU.cacheId(actionData.request().cacheName()));
                }
                else if (acts.localJoinContext() != null && !F.isEmpty(acts.localJoinContext().caches())) {
                    for (T2<DynamicCacheDescriptor, NearCacheConfiguration> tup : acts.localJoinContext().caches())
                        prepareIndexRebuildFuture(tup.get1().cacheId());
                }
            }
        }
    }

    /**
     * Creates a new index rebuild future that should be completed later after exchange is done. The future
     * has to be created before exchange is initialized to guarantee that we will capture a correct future
     * after activation or restore completes.
     * If there was an old future for the given ID, it will be completed.
     *
     * @param cacheId Cache ID.
     */
    private void prepareIndexRebuildFuture(int cacheId) {
        GridFutureAdapter<Void> old = idxRebuildFuts.put(cacheId, new GridFutureAdapter<>());

        if (old != null)
            old.onDone();
    }

    /** {@inheritDoc} */
    @Override public void rebuildIndexesIfNeeded(GridDhtPartitionsExchangeFuture fut) {
        if (cctx.kernalContext().query().moduleEnabled()) {
            for (final GridCacheContext cacheCtx : (Collection<GridCacheContext>)cctx.cacheContexts()) {
                if (cacheCtx.startTopologyVersion().equals(fut.initialVersion())) {
                    final int cacheId = cacheCtx.cacheId();
                    final GridFutureAdapter<Void> usrFut = idxRebuildFuts.get(cacheId);

                    if (!cctx.pageStore().hasIndexStore(cacheCtx.groupId()) && cacheCtx.affinityNode()) {
                        IgniteInternalFuture<?> rebuildFut = cctx.kernalContext().query()
                            .rebuildIndexesFromHash(Collections.singletonList(cacheCtx.cacheId()));

                        assert usrFut != null : "Missing user future for cache: " + cacheCtx.name();

                        rebuildFut.listen(new CI1<IgniteInternalFuture>() {
                            @Override public void apply(IgniteInternalFuture igniteInternalFut) {
                                idxRebuildFuts.remove(cacheId, usrFut);

                                usrFut.onDone(igniteInternalFut.error());

                                CacheConfiguration ccfg = cacheCtx.config();

                                if (ccfg != null) {
                                    log().info("Finished indexes rebuilding for cache [name=" + ccfg.getName()
                                        + ", grpName=" + ccfg.getGroupName() + ']');
                                }
                            }
                        });
                    }
                    else {
                        if (usrFut != null) {
                            idxRebuildFuts.remove(cacheId, usrFut);

                            usrFut.onDone();
                        }
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteInternalFuture indexRebuildFuture(int cacheId) {
        return idxRebuildFuts.get(cacheId);
    }

    /** {@inheritDoc} */
    @Override public void onCacheGroupsStopped(
        Collection<IgniteBiTuple<CacheGroupContext, Boolean>> stoppedGrps
    ) {
        Map<PageMemoryEx, Collection<Integer>> destroyed = new HashMap<>();

        for (IgniteBiTuple<CacheGroupContext, Boolean> tup : stoppedGrps) {
            CacheGroupContext gctx = tup.get1();

            if (!gctx.persistenceEnabled())
                continue;

            snapshotMgr.onCacheGroupStop(gctx);

            PageMemoryEx pageMem = (PageMemoryEx)gctx.dataRegion().pageMemory();

            Collection<Integer> grpIds = destroyed.get(pageMem);

            if (grpIds == null) {
                grpIds = new HashSet<>();

                destroyed.put(pageMem, grpIds);
            }

            grpIds.add(tup.get1().groupId());

            pageMem.onCacheGroupDestroyed(tup.get1().groupId());
        }

        Collection<IgniteInternalFuture<Void>> clearFuts = new ArrayList<>(destroyed.size());

        for (Map.Entry<PageMemoryEx, Collection<Integer>> entry : destroyed.entrySet()) {
            final Collection<Integer> grpIds = entry.getValue();

            clearFuts.add(entry.getKey().clearAsync((grpId, pageIdg) -> grpIds.contains(grpId), false));
        }

        for (IgniteInternalFuture<Void> clearFut : clearFuts) {
            try {
                clearFut.get();
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to clear page memory", e);
            }
        }

        if (cctx.pageStore() != null) {
            for (IgniteBiTuple<CacheGroupContext, Boolean> tup : stoppedGrps) {
                CacheGroupContext grp = tup.get1();

                if (grp.affinityNode()) {
                    try {
                        cctx.pageStore().shutdownForCacheGroup(grp, tup.get2());
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to gracefully clean page store resources for destroyed cache " +
                            "[cache=" + grp.cacheOrGroupName() + "]", e);
                    }
                }
            }
        }
    }

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquireSnapshotWorker memory
     * state.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    @Override public void checkpointReadLock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return;

        for (; ; ) {
            checkpointLock.readLock().lock();

            if (stopping) {
                checkpointLock.readLock().unlock();

                throw new RuntimeException("Failed to perform cache update: node is stopping.");
            }

            if (safeToUpdatePageMemories() || checkpointLock.getReadHoldCount() > 1)
                break;
            else {
                checkpointLock.readLock().unlock();

                try {
                    checkpointer.wakeupForCheckpoint(0, "too many dirty pages").cpBeginFut.getUninterruptibly();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to wait for checkpoint begin.", e);
                }
            }
        }

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);
    }

    /** {@inheritDoc} */
    @Override public boolean checkpointLockIsHeldByThread() {
        return !ASSERTION_ENABLED ||
            checkpointLock.isWriteLockedByCurrentThread() ||
            CHECKPOINT_LOCK_HOLD_COUNT.get() > 0;
    }

    /**
     * @return {@code true} if all PageMemory instances are safe to update.
     */
    private boolean safeToUpdatePageMemories() {
        Collection<DataRegion> memPlcs = context().database().dataRegions();

        if (memPlcs == null)
            return true;

        for (DataRegion memPlc : memPlcs) {
            if (!memPlc.config().isPersistenceEnabled())
                continue;

            PageMemoryEx pageMemEx = (PageMemoryEx)memPlc.pageMemory();

            if (!pageMemEx.safeToUpdate())
                return false;
        }

        return true;
    }

    /**
     * Releases the checkpoint read lock.
     */
    @Override public void checkpointReadUnlock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return;

        checkpointLock.readLock().unlock();

        if (checkpointer != null) {
            Collection<DataRegion> dataRegs = context().database().dataRegions();

            if (dataRegs != null) {
                for (DataRegion dataReg : dataRegs) {
                    if (!dataReg.config().isPersistenceEnabled())
                        continue;

                    PageMemoryEx mem = (PageMemoryEx)dataReg.pageMemory();

                    if (mem != null && !mem.safeToUpdate()) {
                        checkpointer.wakeupForCheckpoint(0, "too many dirty pages");

                        break;
                    }
                }
            }
        }

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() - 1);
    }

    /**
     * Restores from last checkpoint and applies WAL changes since this checkpoint.
     *
     * @throws IgniteCheckedException If failed to restore database status from WAL.
     */
    private void restoreState() throws IgniteCheckedException {
        try {
            CheckpointStatus status = readCheckpointStatus();

            checkpointReadLock();

            try {
                applyLastUpdates(status, false);
            }
            finally {
                checkpointReadUnlock();
            }

            snapshotMgr.restoreState();

            new IgniteThread(cctx.igniteInstanceName(), "db-checkpoint-thread", checkpointer).start();

            CheckpointProgressSnapshot chp = checkpointer.wakeupForCheckpoint(0, "node started");

            if (chp != null)
                chp.cpBeginFut.get();
        }
        catch (StorageException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized Map<Integer, Map<Integer, Long>> reserveHistoryForExchange() {
        assert reservedForExchange == null : reservedForExchange;

        reservedForExchange = new HashMap<>();

        Map</*grpId*/Integer, Set</*partId*/Integer>> parts4CheckpointHistSearch = partsForCheckpointHistorySearch();

        Map</*grpId*/Integer, Map</*partId*/Integer, CheckpointEntry>> lastCheckpointEntry4Grp =
            searchLastCheckpointEntryPerPartition(parts4CheckpointHistSearch);

        Map</*grpId*/Integer, Map</*partId*/Integer, /*updCntr*/Long>> grpPartsWithCnts = new HashMap<>();

        try {
            for (Map.Entry<Integer, Map<Integer, CheckpointEntry>> e : lastCheckpointEntry4Grp.entrySet()) {
                Integer grpId = e.getKey();

                for (Map.Entry<Integer, CheckpointEntry> e0 : e.getValue().entrySet()) {
                    CheckpointEntry cpEntry = e0.getValue();

                    Integer partId = e0.getKey();

                    if (cctx.wal().reserve(cpEntry.cpMark)) {
                        Map</*partId*/Integer, T2</*updCntr*/Long, WALPointer>> grpChpState = reservedForExchange.get(grpId);

                        Map<Integer, Long> grpCnts = grpPartsWithCnts.get(grpId);

                        if (grpChpState == null) {
                            reservedForExchange.put(grpId, grpChpState = new HashMap<>());

                            grpPartsWithCnts.put(grpId, grpCnts = new HashMap<>());
                        }

                        Long partCnt = cpEntry.partitionCounter(cctx, grpId, partId);

                        if (partCnt != null) {
                            grpChpState.put(partId, new T2<>(partCnt, cpEntry.cpMark));

                            grpCnts.put(partId, partCnt);
                        }
                        else
                            cctx.wal().release(cpEntry.cpMark);
                    }
                }
            }
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Error while trying to reserve history", ex);
        }

        return grpPartsWithCnts;
    }

    /**
     *
     * @return Map of group id -> Set parts.
     */
    private Map<Integer, Set<Integer>> partsForCheckpointHistorySearch() {
        Map<Integer, Set<Integer>> part4CheckpointHistSearch = new HashMap<>();

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
                if (part.state() != GridDhtPartitionState.OWNING || part.dataStore().fullSize() <= walRebalanceThreshold)
                    continue;

                Set<Integer> parts = part4CheckpointHistSearch.get(grp.groupId());

                if (parts == null)
                    part4CheckpointHistSearch.put(grp.groupId(), parts = new HashSet<>());

                parts.add(part.id());
            }
        }

        return part4CheckpointHistSearch;
    }

    /** {@inheritDoc} */
    @Override public synchronized void releaseHistoryForExchange() {
        if (reservedForExchange == null)
            return;

        for (Map.Entry<Integer, Map<Integer, T2<Long, WALPointer>>> e : reservedForExchange.entrySet()) {
            for (Map.Entry<Integer, T2<Long, WALPointer>> e0 : e.getValue().entrySet()) {
                try {
                    cctx.wal().release(e0.getValue().get2());
                }
                catch (IgniteCheckedException ex) {
                    U.error(log, "Could not release history lock", ex);
                }
            }
        }

        reservedForExchange = null;
    }

    /** {@inheritDoc} */
    @Override public boolean reserveHistoryForPreloading(int grpId, int partId, long cntr) {
        CheckpointEntry cpEntry = searchCheckpointEntry(grpId, partId, cntr);

        if (cpEntry == null)
            return false;

        WALPointer ptr = cpEntry.cpMark;

        if (ptr == null)
            return false;

        boolean reserved;

        try {
            reserved = cctx.wal().reserve(ptr);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Error while trying to reserve history", e);

            reserved = false;
        }

        if (reserved)
            reservedForPreloading.put(new T2<>(grpId, partId), new T2<>(cntr, ptr));

        return reserved;
    }

    /** {@inheritDoc} */
    @Override public void releaseHistoryForPreloading() {
        for (Map.Entry<T2<Integer, Integer>, T2<Long, WALPointer>> e : reservedForPreloading.entrySet()) {
            try {
                cctx.wal().release(e.getValue().get2());
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Could not release WAL reservation", ex);

                throw new IgniteException(ex);
            }
        }

        reservedForPreloading.clear();
    }

    /**
     * For debugging only. TODO: remove.
     */
    public Map<T2<Integer, Integer>, T2<Long, WALPointer>> reservedForPreloading() {
        return reservedForPreloading;
    }

    /**
     *
     */
    @Nullable @Override public IgniteInternalFuture wakeupForCheckpoint(String reason) {
        Checkpointer cp = checkpointer;

        if (cp != null)
            return cp.wakeupForCheckpoint(0, reason).cpBeginFut;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void waitForCheckpoint(String reason) throws IgniteCheckedException {
        Checkpointer cp = checkpointer;

        if (cp == null)
            return;

        CheckpointProgressSnapshot progSnapshot = cp.wakeupForCheckpoint(0, reason);

        IgniteInternalFuture fut1 = progSnapshot.cpFinishFut;

        fut1.get();

        if (!progSnapshot.started)
            return;

        IgniteInternalFuture fut2 = cp.wakeupForCheckpoint(0, reason).cpFinishFut;

        assert fut1 != fut2;

        fut2.get();
    }

    /** {@inheritDoc} */
    @Override public CheckpointFuture forceCheckpoint(String reason) {
        Checkpointer cp = checkpointer;

        if (cp == null)
            return null;

        return cp.wakeupForCheckpoint(0, reason);
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    private Map<Integer, Map<Integer, CheckpointEntry>> searchLastCheckpointEntryPerPartition(
        final Map<Integer, Set<Integer>> part4reserve
    ) {
        final Map<Integer, Map<Integer, CheckpointEntry>> res = new HashMap<>();

        if (F.isEmpty(part4reserve))
            return res;

        for (Long cpTs : checkpointHist.checkpoints()) {
            CheckpointEntry chpEntry = null;

            try {
                chpEntry = checkpointHist.entry(cpTs);

                Map<Integer, CheckpointEntry.GroupState> grpsState = chpEntry.groupState(cctx);

                if (F.isEmpty(grpsState)) {
                    res.clear();

                    continue;
                }

                for (Map.Entry<Integer, Set<Integer>> grps : part4reserve.entrySet()) {
                    Integer grpId = grps.getKey();

                    Map<Integer, CheckpointEntry> partToCheckPntEntry = res.get(grpId);

                    CheckpointEntry.GroupState grpState = grpsState.get(grpId);

                    if (grpState == null) {
                        res.remove(grpId);

                        continue;
                    }

                    if (partToCheckPntEntry == null)
                        res.put(grpId, partToCheckPntEntry = new HashMap<>());

                    for (Integer partId : grps.getValue()) {
                        int idx = grpState.indexByPartition(partId);

                        if (idx < 0)
                            partToCheckPntEntry.remove(partId);
                        else {
                            if (partToCheckPntEntry.containsKey(partId))
                                continue;

                            partToCheckPntEntry.put(partId, chpEntry);
                        }
                    }
                }
            }
            catch (IgniteCheckedException ex) {
                String msg = chpEntry != null ?
                    ", chpId=" + chpEntry.cpId + " ptr=" + chpEntry.cpMark + " ts=" + chpEntry.cpTs : "";

                U.error(log, "Failed to read checkpoint entry" + msg, ex);

                // Treat exception the same way as a gap.
                res.clear();
            }
        }

        return res;
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable public WALPointer searchPartitionCounter(int grpId, int part, @Nullable Long partCntrSince) {
        CheckpointEntry entry = searchCheckpointEntry(grpId, part, partCntrSince);

        if (entry == null)
            return null;

        return entry.cpMark;
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable private CheckpointEntry searchCheckpointEntry(int grpId, int part, @Nullable Long partCntrSince) {
        boolean hasGap = false;
        CheckpointEntry first = null;

        for (Long cpTs : checkpointHist.checkpoints()) {
            try {
                CheckpointEntry entry = checkpointHist.entry(cpTs);

                Long foundCntr = entry.partitionCounter(cctx, grpId, part);

                if (foundCntr != null) {
                    if (partCntrSince == null) {
                        if (hasGap) {
                            first = entry;

                            hasGap = false;
                        }

                        if (first == null)
                            first = entry;
                    }
                    else if (foundCntr <= partCntrSince) {
                        first = entry;

                        hasGap = false;
                    }
                    else
                        return hasGap ? null : first;
                }
                else
                    hasGap = true;
            }
            catch (IgniteCheckedException ignore) {
                // Treat exception the same way as a gap.
                hasGap = true;
            }
        }

        return hasGap ? null : first;
    }

    /**
     * @return Checkpoint history. For tests only.
     */
    public CheckpointHistory checkpointHistory() {
        return checkpointHist;
    }

    /**
     * @return Checkpoint directory.
     */
    public File checkpointDirectory() {
        return cpDir;
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
     * @return Read checkpoint status.
     * @throws IgniteCheckedException If failed to read checkpoint status page.
     */
    @SuppressWarnings("TooBroadScope")
    private CheckpointStatus readCheckpointStatus() throws IgniteCheckedException {
        long lastStartTs = 0;
        long lastEndTs = 0;

        UUID startId = CheckpointStatus.NULL_UUID;
        UUID endId = CheckpointStatus.NULL_UUID;

        File startFile = null;
        File endFile = null;

        WALPointer startPtr = CheckpointStatus.NULL_PTR;
        WALPointer endPtr = CheckpointStatus.NULL_PTR;

        File dir = cpDir;

        if (!dir.exists()) {
            // TODO: remove excessive logging after GG-12116 fix.
            File[] files = dir.listFiles();

            if (files != null && files.length > 0) {
                log.warning("Read checkpoint status: cpDir.exists() is false, cpDir.listFiles() is: " +
                    Arrays.toString(files));
            }

            if (Files.exists(dir.toPath()))
                log.warning("Read checkpoint status: cpDir.exists() is false, Files.exists(cpDir) is true.");

            if (log.isInfoEnabled())
                log.info("Read checkpoint status: checkpoint directory is not found.");

            return new CheckpointStatus(0, startId, startPtr, endId, endPtr);
        }

        File[] files = dir.listFiles();

        for (File file : files) {
            Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

            if (matcher.matches()) {
                long ts = Long.parseLong(matcher.group(1));
                UUID id = UUID.fromString(matcher.group(2));
                CheckpointEntryType type = CheckpointEntryType.valueOf(matcher.group(3));

                if (type == CheckpointEntryType.START && ts > lastStartTs) {
                    lastStartTs = ts;
                    startId = id;
                    startFile = file;
                }
                else if (type == CheckpointEntryType.END && ts > lastEndTs) {
                    lastEndTs = ts;
                    endId = id;
                    endFile = file;
                }
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(20);
        buf.order(ByteOrder.nativeOrder());

        if (startFile != null)
            startPtr = readPointer(startFile, buf);

        if (endFile != null)
            endPtr = readPointer(endFile, buf);

        if (log.isInfoEnabled())
            log.info("Read checkpoint status [startMarker=" + startFile + ", endMarker=" + endFile + ']');

        return new CheckpointStatus(lastStartTs, startId, startPtr, endId, endPtr);
    }

    /**
     * Loads WAL pointer from CP file
     *
     * @param cpMarkerFile Checkpoint mark file.
     * @return WAL pointer.
     * @throws IgniteCheckedException If failed to read mark file.
     */
    private WALPointer readPointer(File cpMarkerFile, ByteBuffer buf) throws IgniteCheckedException {
        buf.position(0);

        try (FileIO io = ioFactory.create(cpMarkerFile, READ)) {
            io.read(buf);

            buf.flip();

            return new FileWALPointer(buf.getLong(), buf.getInt(), buf.getInt());
        }
        catch (IOException e) {
            throw new IgniteCheckedException(
                "Failed to read checkpoint pointer from marker file: " + cpMarkerFile.getAbsolutePath(), e);
        }
    }

    /**
     * @param status Checkpoint status.
     * @throws IgniteCheckedException If failed.
     * @throws StorageException In case I/O error occurred during operations with storage.
     */
    @Nullable private WALPointer restoreMemory(CheckpointStatus status) throws IgniteCheckedException {
        return restoreMemory(status, false, (PageMemoryEx)metaStorage.pageMemory());
    }

    /**
     * @param status Checkpoint status.
     * @param metastoreOnly If {@code True} restores Metastorage only.
     * @param storePageMem Metastore page memory.
     * @throws IgniteCheckedException If failed.
     * @throws StorageException In case I/O error occurred during operations with storage.
     */
    @Nullable private WALPointer restoreMemory(
        CheckpointStatus status,
        boolean metastoreOnly,
        PageMemoryEx storePageMem
    ) throws IgniteCheckedException {
        assert !metastoreOnly || storePageMem != null;

        if (log.isInfoEnabled())
            log.info("Checking memory state [lastValidPos=" + status.endPtr + ", lastMarked="
                + status.startPtr + ", lastCheckpointId=" + status.cpStartId + ']');

        boolean apply = status.needRestoreMemory();

        if (apply) {
            U.quietAndWarn(log, "Ignite node stopped in the middle of checkpoint. Will restore memory state and " +
                "finish checkpoint on node start.");

            cctx.pageStore().beginRecover();
        }
        else
            cctx.wal().allowCompressionUntil(status.startPtr);

        long start = U.currentTimeMillis();
        int applied = 0;
        WALPointer lastRead = null;

        Collection<Integer> ignoreGrps = metastoreOnly ? Collections.emptySet() :
            F.concat(false, initiallyGlobalWalDisabledGrps, initiallyLocalWalDisabledGrps);

        try (WALIterator it = cctx.wal().replay(status.endPtr)) {
            while (it.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

                WALRecord rec = tup.get2();

                lastRead = tup.get1();

                switch (rec.type()) {
                    case CHECKPOINT_RECORD:
                        CheckpointRecord cpRec = (CheckpointRecord)rec;

                        // We roll memory up until we find a checkpoint start record registered in the status.
                        if (F.eq(cpRec.checkpointId(), status.cpStartId)) {
                            log.info("Found last checkpoint marker [cpId=" + cpRec.checkpointId() +
                                ", pos=" + tup.get1() + ']');

                            apply = false;
                        }
                        else if (!F.eq(cpRec.checkpointId(), status.cpEndId))
                            U.warn(log, "Found unexpected checkpoint marker, skipping [cpId=" + cpRec.checkpointId() +
                                ", expCpId=" + status.cpStartId + ", pos=" + tup.get1() + ']');

                        break;

                    case PAGE_RECORD:
                        if (apply) {
                            PageSnapshot pageRec = (PageSnapshot)rec;

                            // Here we do not require tag check because we may be applying memory changes after
                            // several repetitive restarts and the same pages may have changed several times.
                            int grpId = pageRec.fullPageId().groupId();

                            if (metastoreOnly && grpId != METASTORAGE_CACHE_ID)
                                continue;

                            if (!ignoreGrps.contains(grpId)) {
                                long pageId = pageRec.fullPageId().pageId();

                                PageMemoryEx pageMem = grpId == METASTORAGE_CACHE_ID ? storePageMem : getPageMemoryForCacheGroup(grpId);

                                long page = pageMem.acquirePage(grpId, pageId, true);

                                try {
                                    long pageAddr = pageMem.writeLock(grpId, pageId, page);

                                    try {
                                        PageUtils.putBytes(pageAddr, 0, pageRec.pageData());
                                    }
                                    finally {
                                        pageMem.writeUnlock(grpId, pageId, page, null, true, true);
                                    }
                                }
                                finally {
                                    pageMem.releasePage(grpId, pageId, page);
                                }

                                applied++;
                            }
                        }

                        break;

                    case PART_META_UPDATE_STATE:
                        PartitionMetaStateRecord metaStateRecord = (PartitionMetaStateRecord)rec;

                        {
                            int grpId = metaStateRecord.groupId();

                            if (metastoreOnly && grpId != METASTORAGE_CACHE_ID)
                                continue;

                            if (ignoreGrps.contains(grpId))
                                continue;

                            int partId = metaStateRecord.partitionId();

                            GridDhtPartitionState state = GridDhtPartitionState.fromOrdinal(metaStateRecord.state());

                            if (state == null || state == GridDhtPartitionState.EVICTED)
                                schedulePartitionDestroy(grpId, partId);
                            else
                                cancelOrWaitPartitionDestroy(grpId, partId);
                        }

                        break;

                    case PARTITION_DESTROY:
                        PartitionDestroyRecord destroyRecord = (PartitionDestroyRecord)rec;

                        {
                            int grpId = destroyRecord.groupId();

                            if (metastoreOnly && grpId != METASTORAGE_CACHE_ID)
                                continue;

                            if (ignoreGrps.contains(grpId))
                                continue;

                            PageMemoryEx pageMem = grpId == METASTORAGE_CACHE_ID ? storePageMem : getPageMemoryForCacheGroup(grpId);

                            pageMem.invalidate(grpId, destroyRecord.partitionId());

                            schedulePartitionDestroy(grpId, destroyRecord.partitionId());
                        }

                        break;

                    default:
                        if (apply && rec instanceof PageDeltaRecord) {
                            PageDeltaRecord r = (PageDeltaRecord)rec;

                            int grpId = r.groupId();

                            if (metastoreOnly && grpId != METASTORAGE_CACHE_ID)
                                continue;

                            if (!ignoreGrps.contains(grpId)) {
                                long pageId = r.pageId();

                                PageMemoryEx pageMem = grpId == METASTORAGE_CACHE_ID ? storePageMem : getPageMemoryForCacheGroup(grpId);

                                // Here we do not require tag check because we may be applying memory changes after
                                // several repetitive restarts and the same pages may have changed several times.
                                long page = pageMem.acquirePage(grpId, pageId, true);

                                try {
                                    long pageAddr = pageMem.writeLock(grpId, pageId, page);

                                    try {
                                        r.applyDelta(pageMem, pageAddr);
                                    }
                                    finally {
                                        pageMem.writeUnlock(grpId, pageId, page, null, true, true);
                                    }
                                }
                                finally {
                                    pageMem.releasePage(grpId, pageId, page);
                                }

                                applied++;
                            }
                        }
                }
            }
        }

        if (metastoreOnly)
            return null;

        if (status.needRestoreMemory()) {
            if (apply)
                throw new StorageException("Failed to restore memory state (checkpoint marker is present " +
                    "on disk, but checkpoint record is missed in WAL) " +
                    "[cpStatus=" + status + ", lastRead=" + lastRead + "]");

            log.info("Finished applying memory changes [changesApplied=" + applied +
                ", time=" + (U.currentTimeMillis() - start) + "ms]");

            if (applied > 0)
                finalizeCheckpointOnRecovery(status.cpStartTs, status.cpStartId, status.startPtr);
        }

        checkpointHist.loadHistory(cpDir);

        return lastRead == null ? null : lastRead.next();
    }

    /**
     * Obtains PageMemory reference from cache descriptor instead of cache context.
     *
     * @param grpId Cache group id.
     * @return PageMemoryEx instance.
     * @throws IgniteCheckedException if no DataRegion is configured for a name obtained from cache descriptor.
     */
    private PageMemoryEx getPageMemoryForCacheGroup(int grpId) throws IgniteCheckedException {
        // TODO IGNITE-5075: cache descriptor can be removed.
        GridCacheSharedContext sharedCtx = context();

        CacheGroupDescriptor desc = sharedCtx.cache().cacheGroupDescriptors().get(grpId);

        if (desc == null)
            throw new IgniteCheckedException("Failed to find cache group descriptor [grpId=" + grpId + ']');

        String memPlcName = desc.config().getDataRegionName();

        return (PageMemoryEx)sharedCtx.database().dataRegion(memPlcName).pageMemory();
    }

    /**
     * Apply update from some iterator and with specific filters.
     *
     * @param it WalIterator.
     * @param recPredicate Wal record filter.
     * @param entryPredicate Entry filter.
     * @param partStates Partition to restore state.
     */
    public void applyUpdatesOnRecovery(
        @Nullable WALIterator it,
        IgnitePredicate<IgniteBiTuple<WALPointer, WALRecord>> recPredicate,
        IgnitePredicate<DataEntry> entryPredicate,
        Map<T2<Integer, Integer>, T2<Integer, Long>> partStates
    ) throws IgniteCheckedException {
        if (it != null) {
            while (it.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = it.nextX();

                WALRecord rec = next.get2();

                if (!recPredicate.apply(next))
                    break;

                switch (rec.type()) {
                    case DATA_RECORD:
                        checkpointReadLock();

                        try {
                            DataRecord dataRec = (DataRecord)rec;

                            for (DataEntry dataEntry : dataRec.writeEntries()) {
                                if (entryPredicate.apply(dataEntry)) {
                                    checkpointReadLock();

                                    try {
                                        int cacheId = dataEntry.cacheId();

                                        GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                                        if (cacheCtx != null)
                                            applyUpdate(cacheCtx, dataEntry);
                                        else if (log != null)
                                            log.warning("Cache (cacheId=" + cacheId + ") is not started, can't apply updates.");
                                    }
                                    finally {
                                        checkpointReadUnlock();
                                    }
                                }
                            }
                        }
                        finally {
                            checkpointReadUnlock();
                        }

                        break;

                    default:
                        // Skip other records.
                }
            }
        }

        checkpointReadLock();

        try {
            restorePartitionStates(partStates, null);
        }
        finally {
            checkpointReadUnlock();
        }
    }

    /**
     * @param status Last registered checkpoint status.
     * @throws IgniteCheckedException If failed to apply updates.
     * @throws StorageException If IO exception occurred while reading write-ahead log.
     */
    private void applyLastUpdates(CheckpointStatus status, boolean metastoreOnly) throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Applying lost cache updates since last checkpoint record [lastMarked="
                + status.startPtr + ", lastCheckpointId=" + status.cpStartId + ']');

        if (!metastoreOnly)
            cctx.kernalContext().query().skipFieldLookup(true);

        long start = U.currentTimeMillis();
        int applied = 0;

        Collection<Integer> ignoreGrps = metastoreOnly ? Collections.emptySet() :
            F.concat(false, initiallyGlobalWalDisabledGrps, initiallyLocalWalDisabledGrps);

        try (WALIterator it = cctx.wal().replay(status.startPtr)) {
            Map<T2<Integer, Integer>, T2<Integer, Long>> partStates = new HashMap<>();

            while (it.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = it.nextX();

                WALRecord rec = next.get2();

                switch (rec.type()) {
                    case DATA_RECORD:
                        if (metastoreOnly)
                            continue;

                        DataRecord dataRec = (DataRecord)rec;

                        for (DataEntry dataEntry : dataRec.writeEntries()) {
                            int cacheId = dataEntry.cacheId();

                            int grpId = cctx.cache().cacheDescriptor(cacheId).groupId();

                            if (!ignoreGrps.contains(grpId)) {
                                GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                                applyUpdate(cacheCtx, dataEntry);

                                applied++;
                            }
                        }

                        break;

                    case PART_META_UPDATE_STATE:
                        if (metastoreOnly)
                            continue;

                        PartitionMetaStateRecord metaStateRecord = (PartitionMetaStateRecord)rec;

                        if (!ignoreGrps.contains(metaStateRecord.groupId())) {
                            partStates.put(new T2<>(metaStateRecord.groupId(), metaStateRecord.partitionId()),
                                new T2<>((int)metaStateRecord.state(), metaStateRecord.updateCounter()));
                        }

                        break;

                    case METASTORE_DATA_RECORD:
                        MetastoreDataRecord metastoreDataRecord = (MetastoreDataRecord)rec;

                        metaStorage.applyUpdate(metastoreDataRecord.key(), metastoreDataRecord.value());

                        break;

                    case META_PAGE_UPDATE_NEXT_SNAPSHOT_ID:
                    case META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID:
                    case META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID:
                        if (metastoreOnly)
                            continue;

                        PageDeltaRecord rec0 = (PageDeltaRecord) rec;

                        PageMemoryEx pageMem = getPageMemoryForCacheGroup(rec0.groupId());

                        long page = pageMem.acquirePage(rec0.groupId(), rec0.pageId(), true);

                        try {
                            long addr = pageMem.writeLock(rec0.groupId(), rec0.pageId(), page, true);

                            try {
                                rec0.applyDelta(pageMem, addr);
                            }
                            finally {
                                pageMem.writeUnlock(rec0.groupId(), rec0.pageId(), page, null, true, true);
                            }
                        }
                        finally {
                            pageMem.releasePage(rec0.groupId(), rec0.pageId(), page);
                        }

                        break;

                    default:
                        // Skip other records.
                }
            }

            if (!metastoreOnly)
                restorePartitionStates(partStates, null);
        }
        finally {
            if (!metastoreOnly)
                cctx.kernalContext().query().skipFieldLookup(false);
        }

        if (log.isInfoEnabled())
            log.info("Finished applying WAL changes [updatesApplied=" + applied +
                ", time=" + (U.currentTimeMillis() - start) + "ms]");
    }

    /**
     * Initializes not empty partitions and restores their state from page memory or WAL.
     * Partition states presented in page memory may be overriden by states restored from WAL {@code partStates}.
     *
     * @param partStates Partition states restored from WAL.
     * @param onlyForGroups If not {@code null} restore states only for specified cache groups.
     * @throws IgniteCheckedException If failed to restore partition states.
     */
    private void restorePartitionStates(Map<T2<Integer, Integer>, T2<Integer, Long>> partStates,
                                       @Nullable Set<Integer> onlyForGroups) throws IgniteCheckedException {
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal() || !grp.affinityNode()) {
                // Local cache has no partitions and its states.
                continue;
            }

            if (!grp.dataRegion().config().isPersistenceEnabled())
                continue;

            if (onlyForGroups != null && !onlyForGroups.contains(grp.groupId()))
                continue;

            int grpId = grp.groupId();

            PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

            for (int i = 0; i < grp.affinity().partitions(); i++) {
                T2<Integer, Long> restore = partStates.get(new T2<>(grpId, i));

                if (storeMgr.exists(grpId, i)) {
                    storeMgr.ensure(grpId, i);

                    if (storeMgr.pages(grpId, i) <= 1)
                        continue;

                    GridDhtLocalPartition part = grp.topology().forceCreatePartition(i);

                    assert part != null;

                    // TODO: https://issues.apache.org/jira/browse/IGNITE-6097
                    grp.offheap().onPartitionInitialCounterUpdated(i, 0);

                    checkpointReadLock();

                    try {
                        long partMetaId = pageMem.partitionMetaPageId(grpId, i);
                        long partMetaPage = pageMem.acquirePage(grpId, partMetaId);

                        try {
                            long pageAddr = pageMem.writeLock(grpId, partMetaId, partMetaPage);

                            boolean changed = false;

                            try {
                                PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(pageAddr);

                                if (restore != null) {
                                    int stateId = restore.get1();

                                    io.setPartitionState(pageAddr, (byte)stateId);

                                    changed = updateState(part, stateId);

                                    if (stateId == GridDhtPartitionState.OWNING.ordinal()
                                        || (stateId == GridDhtPartitionState.MOVING.ordinal()
                                        && part.initialUpdateCounter() < restore.get2())) {
                                        part.initialUpdateCounter(restore.get2());

                                        changed = true;
                                    }
                                }
                                else
                                    updateState(part, (int)io.getPartitionState(pageAddr));
                            }
                            finally {
                                pageMem.writeUnlock(grpId, partMetaId, partMetaPage, null, changed);
                            }
                        }
                        finally {
                            pageMem.releasePage(grpId, partMetaId, partMetaPage);
                        }
                    }
                    finally {
                        checkpointReadUnlock();
                    }
                }
                else if (restore != null) {
                    GridDhtLocalPartition part = grp.topology().forceCreatePartition(i);

                    assert part != null;

                    // TODO: https://issues.apache.org/jira/browse/IGNITE-6097
                    grp.offheap().onPartitionInitialCounterUpdated(i, 0);

                    updateState(part, restore.get1());
                }
            }

            // After partition states are restored, it is necessary to update internal data structures in topology.
            grp.topology().afterStateRestored(grp.topology().lastTopologyChangeVersion());
        }
    }

    /**
     * Wal truncate callBack.
     *
     * @param highBound WALPointer.
     */
    public void onWalTruncated(WALPointer highBound) {
        checkpointHist.onWalTruncated(highBound);
    }

    /**
     * @param part Partition to restore state for.
     * @param stateId State enum ordinal.
     * @return Updated flag.
     */
    private boolean updateState(GridDhtLocalPartition part, int stateId) {
        if (stateId != -1) {
            GridDhtPartitionState state = GridDhtPartitionState.fromOrdinal(stateId);

            assert state != null;

            part.restoreState(state == GridDhtPartitionState.EVICTED ? GridDhtPartitionState.RENTING : state);

            return true;
        }

        return false;
    }

    /**
     * @param cacheCtx Cache context to apply an update.
     * @param dataEntry Data entry to apply.
     * @throws IgniteCheckedException If failed to restore.
     */
    private void applyUpdate(GridCacheContext cacheCtx, DataEntry dataEntry) throws IgniteCheckedException {
        int partId = dataEntry.partitionId();

        if (partId == -1)
            partId = cacheCtx.affinity().partition(dataEntry.key());

        GridDhtLocalPartition locPart = cacheCtx.topology().forceCreatePartition(partId);

        switch (dataEntry.op()) {
            case CREATE:
            case UPDATE:
                cacheCtx.offheap().update(
                    cacheCtx,
                    dataEntry.key(),
                    dataEntry.value(),
                    dataEntry.writeVersion(),
                    0L,
                    locPart,
                    null);

                if (dataEntry.partitionCounter() != 0)
                    cacheCtx.offheap().onPartitionInitialCounterUpdated(partId, dataEntry.partitionCounter());

                break;

            case DELETE:
                cacheCtx.offheap().remove(cacheCtx, dataEntry.key(), partId, locPart);

                if (dataEntry.partitionCounter() != 0)
                    cacheCtx.offheap().onPartitionInitialCounterUpdated(partId, dataEntry.partitionCounter());

                break;

            case READ:
                // do nothing
                break;

            default:
                throw new IgniteCheckedException("Invalid operation for WAL entry update: " + dataEntry.op());
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void finalizeCheckpointOnRecovery(long cpTs, UUID cpId, WALPointer walPtr) throws IgniteCheckedException {
        assert cpTs != 0;

        ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(pageSize());

        long start = System.currentTimeMillis();

        Collection<DataRegion> memPolicies = context().database().dataRegions();

        List<IgniteBiTuple<PageMemory, Collection<FullPageId>>> cpEntities = new ArrayList<>(memPolicies.size());

        for (DataRegion memPlc : memPolicies) {
            if (memPlc.config().isPersistenceEnabled()) {
                PageMemoryEx pageMem = (PageMemoryEx)memPlc.pageMemory();

                cpEntities.add(new IgniteBiTuple<PageMemory, Collection<FullPageId>>(
                    pageMem, (pageMem).beginCheckpoint()));
            }
        }

        tmpWriteBuf.order(ByteOrder.nativeOrder());

        // Identity stores set.
        Collection<PageStore> updStores = new HashSet<>();

        int cpPagesCnt = 0;

        for (IgniteBiTuple<PageMemory, Collection<FullPageId>> e : cpEntities) {
            PageMemoryEx pageMem = (PageMemoryEx)e.get1();

            Collection<FullPageId> cpPages = e.get2();

            cpPagesCnt += cpPages.size();

            for (FullPageId fullId : cpPages) {
                tmpWriteBuf.rewind();

                Integer tag = pageMem.getForCheckpoint(fullId, tmpWriteBuf, null);

                if (tag != null) {
                    tmpWriteBuf.rewind();

                    PageStore store = storeMgr.writeInternal(fullId.groupId(), fullId.pageId(), tmpWriteBuf, tag, true);

                    tmpWriteBuf.rewind();

                    updStores.add(store);
                }
            }
        }

        long written = U.currentTimeMillis();

        for (PageStore updStore : updStores)
            updStore.sync();

        long fsync = U.currentTimeMillis();

        for (IgniteBiTuple<PageMemory, Collection<FullPageId>> e : cpEntities)
            ((PageMemoryEx)e.get1()).finishCheckpoint();

        writeCheckpointEntry(
            tmpWriteBuf,
            cpTs,
            cpId,
            walPtr,
            null,
            CheckpointEntryType.END);

        cctx.pageStore().finishRecover();

        if (log.isInfoEnabled())
            log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                    "pagesWrite=%dms, fsync=%dms, total=%dms]",
                cpId,
                cpPagesCnt,
                walPtr,
                written - start,
                fsync - written,
                fsync - start));
    }

    /**
     * Writes into specified file checkpoint entry containing WAL pointer to checkpoint record.
     *
     * @param cpId Checkpoint ID.
     * @param ptr Wal pointer of current checkpoint.
     */
    private CheckpointEntry writeCheckpointEntry(
        ByteBuffer tmpWriteBuf,
        long cpTs,
        UUID cpId,
        WALPointer ptr,
        CheckpointRecord rec,
        CheckpointEntryType type
    ) throws IgniteCheckedException {
        assert ptr instanceof FileWALPointer;

        FileWALPointer filePtr = (FileWALPointer)ptr;

        String fileName = checkpointFileName(cpTs, cpId, type);
        String tmpFileName = fileName + FILE_TMP_SUFFIX;

        try {
            try (FileIO io = ioFactory.create(Paths.get(cpDir.getAbsolutePath(), skipSync ? fileName : tmpFileName).toFile(),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {

                tmpWriteBuf.rewind();

                tmpWriteBuf.putLong(filePtr.index());

                tmpWriteBuf.putInt(filePtr.fileOffset());

                tmpWriteBuf.putInt(filePtr.length());

                tmpWriteBuf.flip();

                io.write(tmpWriteBuf);

                tmpWriteBuf.clear();

                if (!skipSync)
                    io.force(true);
            }

            if (!skipSync)
                Files.move(Paths.get(cpDir.getAbsolutePath(), tmpFileName), Paths.get(cpDir.getAbsolutePath(), fileName));

            return createCheckPointEntry(cpTs, ptr, cpId, rec, type);
        }
        catch (IOException e) {
            throw new PersistentStorageIOException("Failed to write checkpoint entry [ptr=" + filePtr
                    + ", cpTs=" + cpTs
                    + ", cpId=" + cpId
                    + ", type=" + type + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public AtomicInteger writtenPagesCounter() {
        return writtenPagesCntr;
    }

    /** {@inheritDoc} */
    @Override public AtomicInteger syncedPagesCounter() {
        return syncedPagesCntr;
    }

    /** {@inheritDoc} */
    @Override public AtomicInteger evictedPagesCntr() {
        return evictedPagesCntr;
    }

    /** {@inheritDoc} */
    @Override public int currentCheckpointPagesCount() {
        return currCheckpointPagesCnt;
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
     * Replace thread local with buffers. Thread local should provide direct buffer with one page in length.
     *
     * @param threadBuf new thread-local with buffers for the checkpoint threads.
     */
    public void setThreadBuf(final ThreadLocal<ByteBuffer> threadBuf) {
        this.threadBuf = threadBuf;
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @param ptr Wal pointer of checkpoint.
     * @param cpId Checkpoint ID.
     * @param rec Checkpoint record.
     * @param type Checkpoint type.
     *
     * @return Checkpoint entry.
     */
    private CheckpointEntry createCheckPointEntry(
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

        if (type != CheckpointEntryType.START)
            return null;

        Map<Integer, CacheState> cacheGrpStates = null;

        // Create lazy checkpoint entry.
        if ((checkpointHist.histMap.size() + 1 < maxCpHistMemSize) && rec != null)
            cacheGrpStates = rec.cacheGroupStates();

        return new CheckpointEntry(cpTs, ptr, cpId, cacheGrpStates);
    }

    /**
     * Adds given partition to checkpointer destroy queue.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public void schedulePartitionDestroy(int grpId, int partId) {
        Checkpointer cp = checkpointer;

        if (cp != null)
            cp.schedulePartitionDestroy(cctx.cache().cacheGroup(grpId), grpId, partId);
    }

    /**
     * Cancels or wait for partition destroy.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @throws IgniteCheckedException If failed.
     */
    public void cancelOrWaitPartitionDestroy(int grpId, int partId) throws IgniteCheckedException {
        Checkpointer cp = checkpointer;

        if (cp != null)
            cp.cancelOrWaitPartitionDestroy(grpId, partId);
    }

    /**
     * Partition destroy queue.
     */
    private static class PartitionDestroyQueue {
        /** */
        private final ConcurrentMap<T2<Integer, Integer>, PartitionDestroyRequest> pendingReqs =
            new ConcurrentHashMap<>();

        /**
         * @param grpCtx Group context.
         * @param partId Partition ID to destroy.
         */
        private void addDestroyRequest(@Nullable CacheGroupContext grpCtx, int grpId, int partId) {
            PartitionDestroyRequest req = new PartitionDestroyRequest(grpId, partId);

            PartitionDestroyRequest old = pendingReqs.putIfAbsent(new T2<>(grpId, partId), req);

            assert old == null || grpCtx == null : "Must wait for old destroy request to finish before adding a new one "
                + "[grpId=" + grpId
                + ", grpName=" + grpCtx.cacheOrGroupName()
                + ", partId=" + partId + ']';
        }

        /**
         * @param destroyId Destroy ID.
         * @return Destroy request to complete if was not concurrently cancelled.
         */
        private PartitionDestroyRequest beginDestroy(T2<Integer, Integer> destroyId) {
            PartitionDestroyRequest rmvd = pendingReqs.remove(destroyId);

            return rmvd == null ? null : rmvd.beginDestroy() ? rmvd : null;
        }

        /**
         * @param grpId Group ID.
         * @param partId Partition ID.
         * @return Destroy request to wait for if destroy has begun.
         */
        private PartitionDestroyRequest cancelDestroy(int grpId, int partId) {
            PartitionDestroyRequest rmvd = pendingReqs.remove(new T2<>(grpId, partId));

            return rmvd == null ? null : !rmvd.cancel() ? rmvd : null;
        }
    }

    /**
     * Partition destroy request.
     */
    private static class PartitionDestroyRequest {
        /** */
        private final int grpId;

        /** */
        private final int partId;

        /** Destroy cancelled flag. */
        private boolean cancelled;

        /** Destroy future. Not null if partition destroy has begun. */
        private GridFutureAdapter<Void> destroyFut;

        /**
         * @param grpId Group ID.
         * @param partId Partition ID.
         */
        private PartitionDestroyRequest(int grpId, int partId) {
            this.grpId = grpId;
            this.partId = partId;
        }

        /**
         * Cancels partition destroy request.
         *
         * @return {@code False} if this request needs to be waited for.
         */
        private synchronized boolean cancel() {
            if (destroyFut != null) {
                assert !cancelled;

                return false;
            }

            cancelled = true;

            return true;
        }

        /**
         * Initiates partition destroy.
         *
         * @return {@code True} if destroy request should be executed, {@code false} otherwise.
         */
        private synchronized boolean beginDestroy() {
            if (cancelled) {
                assert destroyFut == null;

                return false;
            }

            if (destroyFut != null)
                return false;

            destroyFut = new GridFutureAdapter<>();

            return true;
        }

        /**
         *
         */
        private synchronized void onDone(Throwable err) {
            assert destroyFut != null;

            destroyFut.onDone(err);
        }

        /**
         *
         */
        private void waitCompleted() throws IgniteCheckedException {
            GridFutureAdapter<Void> fut;

            synchronized (this) {
                assert destroyFut != null;

                fut = destroyFut;
            }

            fut.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PartitionDestroyRequest [grpId=" + grpId + ", partId=" + partId + ']';
        }
    }

    /**
     * Checkpointer object is used for notification on checkpoint begin, predicate is {@link #scheduledCp}<code>.nextCpTs - now
     * > 0 </code>. Method {@link #wakeupForCheckpoint} uses notify, {@link #waitCheckpointEvent} uses wait
     */
    @SuppressWarnings("NakedNotify")
    public class Checkpointer extends GridWorker {
        /** Temporary write buffer. */
        private final ByteBuffer tmpWriteBuf;

        /** Next scheduled checkpoint progress. */
        private volatile CheckpointProgress scheduledCp;

        /** Current checkpoint. This field is updated only by checkpoint thread. */
        @Nullable private volatile CheckpointProgress curCpProgress;

        /** Shutdown now. */
        private volatile boolean shutdownNow;

        /** */
        private long lastCpTs;

        /**
         * @param gridName Grid name.
         * @param name Thread name.
         * @param log Logger.
         */
        protected Checkpointer(@Nullable String gridName, String name, IgniteLogger log) {
            super(gridName, name, log);

            scheduledCp = new CheckpointProgress(U.currentTimeMillis() + checkpointFreq);

            tmpWriteBuf = ByteBuffer.allocateDirect(pageSize());

            tmpWriteBuf.order(ByteOrder.nativeOrder());
        }

        /** {@inheritDoc} */
        @Override protected void body() {
            Throwable err = null;

            try {
                while (!isCancelled()) {
                    waitCheckpointEvent();

                    GridFutureAdapter<Void> enableChangeApplied = GridCacheDatabaseSharedManager.this.enableChangeApplied;

                    if (enableChangeApplied != null) {
                        enableChangeApplied.onDone();

                        GridCacheDatabaseSharedManager.this.enableChangeApplied = null;
                    }

                    if (checkpointsEnabled)
                        doCheckpoint();
                    else {
                        synchronized (this) {
                            scheduledCp.nextCpTs = U.currentTimeMillis() + checkpointFreq;
                        }
                    }
                }
            }
            catch (Throwable t) {
                err = t;

                scheduledCp.cpFinishFut.onDone(t);

                throw t;
            }
            finally {
                if (err == null && !(stopping && isCancelled))
                    err = new IllegalStateException("Thread " + name() + " is terminated unexpectedly");

                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }

            // Final run after the cancellation.
            if (checkpointsEnabled && !shutdownNow) {
                try {
                    doCheckpoint();

                    scheduledCp.cpFinishFut.onDone(new NodeStoppingException("Node is stopping."));
                }
                catch (Throwable e) {
                    scheduledCp.cpFinishFut.onDone(e);
                }
            }
        }

        /**
         *
         */
        private CheckpointProgressSnapshot wakeupForCheckpoint(long delayFromNow, String reason) {
            CheckpointProgress sched = scheduledCp;

            long next = U.currentTimeMillis() + delayFromNow;

            if (sched.nextCpTs <= next)
                return new CheckpointProgressSnapshot(sched);

            CheckpointProgressSnapshot ret;

            synchronized (this) {
                sched = scheduledCp;

                if (sched.nextCpTs > next) {
                    sched.reason = reason;

                    sched.nextCpTs = next;
                }

                ret = new CheckpointProgressSnapshot(sched);

                notifyAll();
            }

            return ret;
        }

        /**
         * @param snapshotOperation Snapshot operation.
         */
        public IgniteInternalFuture wakeupForSnapshotCreation(SnapshotOperation snapshotOperation) {
            GridFutureAdapter<Object> ret;

            synchronized (this) {
                scheduledCp.nextCpTs = U.currentTimeMillis();

                scheduledCp.reason = "snapshot";

                scheduledCp.nextSnapshot = true;

                scheduledCp.snapshotOperation = snapshotOperation;

                ret = scheduledCp.cpBeginFut;

                notifyAll();
            }

            return ret;
        }

        /**
         *
         */
        private void doCheckpoint() {
            try {
                CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

                Checkpoint chp;

                try {
                    chp = markCheckpointBegin(tracker);
                }
                catch (IgniteCheckedException e) {
                    if (curCpProgress != null)
                        curCpProgress.cpFinishFut.onDone(e);

                    // In case of checkpoint initialization error node should be invalidated and stopped.
                    cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

                    return;
                }

                currCheckpointPagesCnt = chp.pagesSize;

                writtenPagesCntr = new AtomicInteger();
                syncedPagesCntr = new AtomicInteger();
                evictedPagesCntr = new AtomicInteger();

                boolean success = false;

                int destroyedPartitionsCnt;

                try {
                    if (chp.hasDelta()) {
                        // Identity stores set.
                        ConcurrentLinkedHashMap<PageStore, LongAdder> updStores = new ConcurrentLinkedHashMap<>();

                        CountDownFuture doneWriteFut = new CountDownFuture(
                            asyncRunner == null ? 1 : chp.cpPages.collectionsSize());

                        tracker.onPagesWriteStart();

                        final int totalPagesToWriteCnt = chp.cpPages.size();

                        if (asyncRunner != null) {
                            for (int i = 0; i < chp.cpPages.collectionsSize(); i++) {
                                Runnable write = new WriteCheckpointPages(
                                    tracker,
                                    chp.cpPages.innerCollection(i),
                                    updStores,
                                    doneWriteFut,
                                    totalPagesToWriteCnt
                                );

                                try {
                                    asyncRunner.execute(write);
                                }
                                catch (RejectedExecutionException ignore) {
                                    // Run the task synchronously.
                                    write.run();
                                }
                            }
                        }
                        else {
                            // Single-threaded checkpoint.
                            Runnable write = new WriteCheckpointPages(tracker,
                                chp.cpPages,
                                updStores,
                                doneWriteFut,
                                totalPagesToWriteCnt);

                            write.run();
                        }

                        // Wait and check for errors.
                        try {
                            doneWriteFut.get();
                        } catch (IgniteCheckedException e) {
                            chp.progress.cpFinishFut.onDone(e);

                            // In case of checkpoint writing error node should be invalidated and stopped.
                            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

                            return;
                        }

                        // Must re-check shutdown flag here because threads may have skipped some pages.
                        // If so, we should not put finish checkpoint mark.
                        if (shutdownNow) {
                            chp.progress.cpFinishFut.onDone(new NodeStoppingException("Node is stopping."));

                            return;
                        }

                        tracker.onFsyncStart();

                        if (!skipSync) {
                            for (Map.Entry<PageStore, LongAdder> updStoreEntry : updStores.entrySet()) {
                                if (shutdownNow) {
                                    chp.progress.cpFinishFut.onDone(new NodeStoppingException("Node is stopping."));

                                    return;
                                }

                                updStoreEntry.getKey().sync();

                                syncedPagesCntr.addAndGet(updStoreEntry.getValue().intValue());
                            }
                        }
                    }
                    else {
                        tracker.onPagesWriteStart();
                        tracker.onFsyncStart();
                    }

                    snapshotMgr.afterCheckpointPageWritten();

                    try {
                        destroyedPartitionsCnt = destroyEvictedPartitions();
                    }
                    catch (IgniteCheckedException e) {
                        chp.progress.cpFinishFut.onDone(e);

                        cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

                        return;
                    }

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
                        if (log.isInfoEnabled())
                            log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                                    "walSegmentsCleared=%d, markDuration=%dms, pagesWrite=%dms, fsync=%dms, " +
                                    "total=%dms]",
                                chp.cpEntry != null ? chp.cpEntry.checkpointId() : "",
                                chp.pagesSize,
                                chp.cpEntry != null ? chp.cpEntry.checkpointMark() : "",
                                chp.walFilesDeleted,
                                tracker.markDuration(),
                                tracker.pagesWriteDuration(),
                                tracker.fsyncDuration(),
                                tracker.totalDuration()));
                    }

                    persStoreMetrics.onCheckpoint(
                        tracker.lockWaitDuration(),
                        tracker.markDuration(),
                        tracker.pagesWriteDuration(),
                        tracker.fsyncDuration(),
                        tracker.totalDuration(),
                        chp.pagesSize,
                        tracker.dataPagesWritten(),
                        tracker.cowPagesWritten());
                }
                else {
                    persStoreMetrics.onCheckpoint(
                        tracker.lockWaitDuration(),
                        tracker.markDuration(),
                        tracker.pagesWriteDuration(),
                        tracker.fsyncDuration(),
                        tracker.totalDuration(),
                        chp.pagesSize,
                        tracker.dataPagesWritten(),
                        tracker.cowPagesWritten());
                }
            }
            catch (IgniteCheckedException e) {
                // TODO-ignite-db how to handle exception?
                U.error(log, "Failed to create checkpoint.", e);
            }
        }

        /**
         * Processes all evicted partitions scheduled for destroy.
         *
         * @throws IgniteCheckedException If failed.
         *
         * @return The number of destroyed partition files.
         */
        private int destroyEvictedPartitions() throws IgniteCheckedException {
            PartitionDestroyQueue destroyQueue = curCpProgress.destroyQueue;

            if (destroyQueue.pendingReqs.isEmpty())
                return 0;

            List<PartitionDestroyRequest> reqs = null;

            for (final PartitionDestroyRequest req : destroyQueue.pendingReqs.values()) {
                if (!req.beginDestroy())
                    continue;

                final int grpId = req.grpId;
                final int partId = req.partId;

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                assert grp != null
                    : "Cache group is not initialized [grpId=" + grpId + "]";
                assert grp.offheap() instanceof GridCacheOffheapManager
                    : "Destroying partition files when persistence is off " + grp.offheap();

                final GridCacheOffheapManager offheap = (GridCacheOffheapManager) grp.offheap();

                Runnable destroyPartTask = () -> {
                    try {
                        offheap.destroyPartitionStore(grpId, partId);

                        req.onDone(null);

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

            destroyQueue.pendingReqs.clear();

            return reqs != null ? reqs.size() : 0;
        }

        /**
         * @param grpCtx Group context. Can be {@code null} in case of crash recovery.
         * @param grpId Group ID.
         * @param partId Partition ID.
         */
        private void schedulePartitionDestroy(@Nullable CacheGroupContext grpCtx, int grpId, int partId) {
            synchronized (this) {
                scheduledCp.destroyQueue.addDestroyRequest(grpCtx, grpId, partId);
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
        private void cancelOrWaitPartitionDestroy(int grpId, int partId) throws IgniteCheckedException {
            PartitionDestroyRequest req;

            synchronized (this) {
                req = scheduledCp.destroyQueue.cancelDestroy(grpId, partId);
            }

            if (req != null)
                req.waitCompleted();

            CheckpointProgress cur;

            synchronized (this) {
                cur = curCpProgress;

                if (cur != null)
                    req = cur.destroyQueue.cancelDestroy(grpId, partId);
            }

            if (req != null)
                req.waitCompleted();

            if (log.isDebugEnabled())
                log.debug("Partition file destroy has cancelled [grpId=" + grpId + ", partId=" + partId + "]");
        }

        /**
         *
         */
        @SuppressWarnings("WaitNotInLoop")
        private void waitCheckpointEvent() {
            boolean cancel = false;

            try {
                long now = U.currentTimeMillis();

                synchronized (this) {
                    long remaining;

                    while ((remaining = scheduledCp.nextCpTs - now) > 0 && !isCancelled()) {
                        wait(remaining);

                        now = U.currentTimeMillis();
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
         *
         */
        @SuppressWarnings("TooBroadScope")
        private Checkpoint markCheckpointBegin(CheckpointMetricsTracker tracker) throws IgniteCheckedException {
            CheckpointRecord cpRec = new CheckpointRecord(null);

            WALPointer cpPtr = null;

            final CheckpointProgress curr;

            IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> cpPagesTuple;

            tracker.onLockWaitStart();

            boolean hasPages;

            IgniteFuture snapFut = null;

            checkpointLock.writeLock().lock();

            try {
                tracker.onMarkStart();

                synchronized (this) {
                    curr = scheduledCp;

                    curr.started = true;

                    if (curr.reason == null)
                        curr.reason = "timeout";

                    // It is important that we assign a new progress object before checkpoint mark in page memory.
                    scheduledCp = new CheckpointProgress(U.currentTimeMillis() + checkpointFreq);

                    curCpProgress = curr;
                }

                final PartitionAllocationMap map = new PartitionAllocationMap();

                DbCheckpointListener.Context ctx0 = new DbCheckpointListener.Context() {
                    @Override public boolean nextSnapshot() {
                        return curr.nextSnapshot;
                    }

                    /** {@inheritDoc} */
                    @Override public PartitionAllocationMap partitionStatMap() {
                        return map;
                    }

                    @Override public boolean needToSnapshot(String cacheOrGrpName) {
                        return curr.snapshotOperation.cacheGroupIds().contains(CU.cacheId(cacheOrGrpName));
                    }
                };

                // Listeners must be invoked before we write checkpoint record to WAL.
                for (DbCheckpointListener lsnr : lsnrs)
                    lsnr.onCheckpointBegin(ctx0);

                if (curr.nextSnapshot)
                    snapFut = snapshotMgr.onMarkCheckPointBegin(curr.snapshotOperation, map);

                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    if (grp.isLocal() || !grp.walEnabled())
                        continue;

                    ArrayList<GridDhtLocalPartition> parts = new ArrayList<>();

                    for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
                        parts.add(part);

                    CacheState state = new CacheState(parts.size());

                    for (GridDhtLocalPartition part : parts) {
                        state.addPartitionState(
                            part.id(),
                            part.dataStore().fullSize(),
                            part.updateCounter(),
                            (byte)part.state().ordinal()
                        );
                    }

                    cpRec.addCacheGroupState(grp.groupId(), state);
                }

                cpPagesTuple = beginAllCheckpoints();

                hasPages = hasPageForWrite(cpPagesTuple.get1());

                if (hasPages || curr.nextSnapshot || !curr.destroyQueue.pendingReqs.isEmpty()) {
                    // No page updates for this checkpoint are allowed from now on.
                    cpPtr = cctx.wal().log(cpRec);

                    if (cpPtr == null)
                        cpPtr = CheckpointStatus.NULL_PTR;
                }
            }
            finally {
                checkpointLock.writeLock().unlock();

                tracker.onLockRelease();
            }

            curr.cpBeginFut.onDone();

            if (snapFut != null) {
                try {
                    snapFut.get();
                }
                catch (IgniteException e) {
                    U.error(log, "Failed to wait for snapshot operation initialization: " +
                        curr.snapshotOperation + "]", e);
                }
            }

            if (hasPages || !curr.destroyQueue.pendingReqs.isEmpty()) {
                assert cpPtr != null;

                tracker.onWalCpRecordFsyncStart();

                // Sync log outside the checkpoint write lock.
                cctx.wal().flush(cpPtr, true);

                tracker.onWalCpRecordFsyncEnd();

                long cpTs = System.currentTimeMillis();

                // This can happen in an unlikely event of two checkpoints happening
                // within a currentTimeMillis() granularity window.
                if (cpTs == lastCpTs)
                    cpTs++;

                lastCpTs = cpTs;

                CheckpointEntry cpEntry = writeCheckpointEntry(
                    tmpWriteBuf,
                    cpTs,
                    cpRec.checkpointId(),
                    cpPtr,
                    cpRec,
                    CheckpointEntryType.START);

                checkpointHist.addCheckpointEntry(cpEntry);

                GridMultiCollectionWrapper<FullPageId> cpPages = splitAndSortCpPagesIfNeeded(cpPagesTuple);

                if (printCheckpointStats)
                    if (log.isInfoEnabled())
                        log.info(String.format("Checkpoint started [checkpointId=%s, startPtr=%s, checkpointLockWait=%dms, " +
                                "checkpointLockHoldTime=%dms, walCpRecordFsyncDuration=%dms, pages=%d, reason='%s']",
                            cpRec.checkpointId(),
                            cpPtr,
                            tracker.lockWaitDuration(),
                            tracker.lockHoldDuration(),
                            tracker.walCpRecordFsyncDuration(),
                            cpPages.size(),
                            curr.reason)
                        );

                return new Checkpoint(cpEntry, cpPages, curr);
            }
            else {
                if (curr.nextSnapshot)
                    cctx.wal().flush(null, true);

                if (printCheckpointStats) {
                    if (log.isInfoEnabled())
                        LT.info(log, String.format("Skipping checkpoint (no pages were modified) [" +
                                "checkpointLockWait=%dms, checkpointLockHoldTime=%dms, reason='%s']",
                            tracker.lockWaitDuration(),
                            tracker.lockHoldDuration(),
                            curr.reason));
                }

                GridMultiCollectionWrapper<FullPageId> wrapper = new GridMultiCollectionWrapper<>(new Collection[0]);

                return new Checkpoint(null, wrapper, curr);
            }
        }

        /**
         * Check that at least one collection is not empty.
         *
         * @param cpPagesCollWrapper Collection of {@link GridMultiCollectionWrapper} checkpoint pages.
         */
        private boolean hasPageForWrite(Collection<GridMultiCollectionWrapper<FullPageId>> cpPagesCollWrapper) {
            boolean hasPages = false;

            for (Collection c : cpPagesCollWrapper)
                if (!c.isEmpty()) {
                    hasPages = true;

                    break;
                }

            return hasPages;
        }

        /**
         * @return tuple with collections of FullPageIds obtained from each PageMemory and overall number of dirty
         * pages.
         */
        private IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> beginAllCheckpoints() {
            Collection<GridMultiCollectionWrapper<FullPageId>> res = new ArrayList(dataRegions().size());

            int pagesNum = 0;

            for (DataRegion memPlc : dataRegions()) {
                if (!memPlc.config().isPersistenceEnabled())
                    continue;

                GridMultiCollectionWrapper<FullPageId> nextCpPagesCol = ((PageMemoryEx)memPlc.pageMemory()).beginCheckpoint();

                pagesNum += nextCpPagesCol.size();

                res.add(nextCpPagesCol);
            }

            currCheckpointPagesCnt = pagesNum;

            return new IgniteBiTuple<>(res, pagesNum);
        }

        /**
         * @param chp Checkpoint snapshot.
         */
        private void markCheckpointEnd(Checkpoint chp) throws IgniteCheckedException {
            synchronized (this) {
                writtenPagesCntr = null;
                syncedPagesCntr = null;
                evictedPagesCntr = null;

                for (DataRegion memPlc : dataRegions()) {
                    if (!memPlc.config().isPersistenceEnabled())
                        continue;

                    ((PageMemoryEx)memPlc.pageMemory()).finishCheckpoint();
                }

                if (chp.hasDelta())
                    writeCheckpointEntry(
                        tmpWriteBuf,
                        chp.cpEntry.checkpointTimestamp(),
                        chp.cpEntry.checkpointId(),
                        chp.cpEntry.checkpointMark(),
                        null,
                        CheckpointEntryType.END);

                currCheckpointPagesCnt = 0;
            }

            checkpointHist.onCheckpointFinished(chp);

            if (chp.progress != null)
                chp.progress.cpFinishFut.onDone();
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
         *
         */
        public void shutdownNow() {
            shutdownNow = true;

            if (!isCancelled)
                cancel();
        }
    }

    /**
     * Reorders list of checkpoint pages and splits them into needed number of sublists according to
     * {@link DataStorageConfiguration#getCheckpointThreads()} and
     * {@link DataStorageConfiguration#getCheckpointWriteOrder()}.
     *
     * @param cpPagesTuple Checkpoint pages tuple.
     */
    private GridMultiCollectionWrapper<FullPageId> splitAndSortCpPagesIfNeeded(
        IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> cpPagesTuple
    ) {
        List<FullPageId> cpPagesList = new ArrayList<>(cpPagesTuple.get2());

        for (GridMultiCollectionWrapper<FullPageId> col : cpPagesTuple.get1()) {
            for (int i = 0; i < col.collectionsSize(); i++)
                cpPagesList.addAll(col.innerCollection(i));
        }

        if (persistenceCfg.getCheckpointWriteOrder() == CheckpointWriteOrder.SEQUENTIAL) {
            FullPageId[] objects = cpPagesList.toArray(new FullPageId[cpPagesList.size()]);

            Arrays.parallelSort(objects, new Comparator<FullPageId>() {
                @Override public int compare(FullPageId o1, FullPageId o2) {
                    int cmp = Long.compare(o1.groupId(), o2.groupId());
                    if (cmp != 0)
                        return cmp;

                    return Long.compare(PageIdUtils.effectivePageId(o1.pageId()),
                        PageIdUtils.effectivePageId(o2.pageId()));
                }
            });

            cpPagesList = Arrays.asList(objects);
        }

        int cpThreads = persistenceCfg.getCheckpointThreads();

        int pagesSubLists = cpThreads == 1 ? 1 : cpThreads * 4;
        // Splitting pages to (threads * 4) subtasks. If any thread will be faster, it will help slower threads.

        Collection[] pagesSubListArr = new Collection[pagesSubLists];

        for (int i = 0; i < pagesSubLists; i++) {
            int totalSize = cpPagesList.size();

            int from = totalSize * i / (pagesSubLists);

            int to = totalSize * (i + 1) / (pagesSubLists);

            pagesSubListArr[i] = cpPagesList.subList(from, to);
        }

        return new GridMultiCollectionWrapper<FullPageId>(pagesSubListArr);
    }

    /** Pages write task */
    private class WriteCheckpointPages implements Runnable {
        /** */
        private CheckpointMetricsTracker tracker;

        /** Collection of page IDs to write under this task. Overall pages to write may be greater than this collection */
        private Collection<FullPageId> writePageIds;

        /** */
        private ConcurrentLinkedHashMap<PageStore, LongAdder> updStores;

        /** */
        private CountDownFuture doneFut;

        /** Total pages to write, counter may be greater than {@link #writePageIds} size */
        private final int totalPagesToWrite;

        /**
         * Creates task for write pages
         *
         * @param tracker
         * @param writePageIds Collection of page IDs to write.
         * @param updStores
         * @param doneFut
         * @param totalPagesToWrite total pages to be written under this checkpoint
         */
        private WriteCheckpointPages(
            final CheckpointMetricsTracker tracker,
            final Collection<FullPageId> writePageIds,
            final ConcurrentLinkedHashMap<PageStore, LongAdder> updStores,
            final CountDownFuture doneFut,
            final int totalPagesToWrite) {
            this.tracker = tracker;
            this.writePageIds = writePageIds;
            this.updStores = updStores;
            this.doneFut = doneFut;
            this.totalPagesToWrite = totalPagesToWrite;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ByteBuffer tmpWriteBuf = threadBuf.get();

            long writeAddr = GridUnsafe.bufferAddress(tmpWriteBuf);

            snapshotMgr.beforeCheckpointPageWritten();

            try {
                for (FullPageId fullId : writePageIds) {
                    if (checkpointer.shutdownNow)
                        break;

                    tmpWriteBuf.rewind();

                    snapshotMgr.beforePageWrite(fullId);

                    int grpId = fullId.groupId();

                    PageMemoryEx pageMem;

                    if (grpId != MetaStorage.METASTORAGE_CACHE_ID) {
                        CacheGroupContext grp = context().cache().cacheGroup(grpId);

                        if (grp == null)
                            continue;

                        if (!grp.dataRegion().config().isPersistenceEnabled())
                            continue;

                        pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
                    }
                    else
                        pageMem = (PageMemoryEx)metaStorage.pageMemory();


                    Integer tag = pageMem.getForCheckpoint(
                        fullId, tmpWriteBuf, persStoreMetrics.metricsEnabled() ? tracker : null);

                    if (tag != null) {
                        assert PageIO.getType(tmpWriteBuf) != 0 : "Invalid state. Type is 0! pageId = " + U.hexLong(fullId.pageId());
                        assert PageIO.getVersion(tmpWriteBuf) != 0 : "Invalid state. Version is 0! pageId = " + U.hexLong(fullId.pageId());

                        tmpWriteBuf.rewind();

                        if (persStoreMetrics.metricsEnabled()) {
                            int pageType = PageIO.getType(tmpWriteBuf);

                            if (PageIO.isDataPageType(pageType))
                                tracker.onDataPageWritten();
                        }

                        if (!skipCrc) {
                            PageIO.setCrc(writeAddr, PureJavaCrc32.calcCrc32(tmpWriteBuf, pageSize()));

                            tmpWriteBuf.rewind();
                        }

                        int curWrittenPages = writtenPagesCntr.incrementAndGet();

                        snapshotMgr.onPageWrite(fullId, tmpWriteBuf, curWrittenPages, totalPagesToWrite);

                        tmpWriteBuf.rewind();

                        PageStore store = storeMgr.writeInternal(grpId, fullId.pageId(), tmpWriteBuf, tag, false);

                        updStores.computeIfAbsent(store, k -> new LongAdder()).increment();
                    }
                }

                doneFut.onDone((Void)null);
            }
            catch (Throwable e) {
                doneFut.onDone(e);
            }
        }
    }

    /**
     *
     */
    private enum CheckpointEntryType {
        /** */
        START,

        /** */
        END
    }

    /**
     *
     */
    private static class Checkpoint {
        /** Checkpoint entry. */
        @Nullable private final CheckpointEntry cpEntry;

        /** Checkpoint pages. */
        private final GridMultiCollectionWrapper<FullPageId> cpPages;

        /** */
        private final CheckpointProgress progress;

        /** Number of deleted WAL files. */
        private int walFilesDeleted;

        /** */
        private final int pagesSize;

        /**
         * @param cpEntry Checkpoint entry.
         * @param cpPages Pages to write to the page store.
         * @param progress Checkpoint progress status.
         */
        private Checkpoint(
            @Nullable CheckpointEntry cpEntry,
            @NotNull GridMultiCollectionWrapper<FullPageId> cpPages,
            CheckpointProgress progress
        ) {
            this.cpEntry = cpEntry;
            this.cpPages = cpPages;
            this.progress = progress;

            pagesSize = cpPages.size();
        }

        /**
         * @return {@code true} if this checkpoint contains at least one dirty page.
         */
        private boolean hasDelta() {
            return pagesSize != 0;
        }
    }

    /**
     *
     */
    private static class CheckpointStatus {
        /** Null checkpoint UUID. */
        private static final UUID NULL_UUID = new UUID(0L, 0L);

        /** Null WAL pointer. */
        private static final WALPointer NULL_PTR = new FileWALPointer(0, 0, 0);

        /** */
        private long cpStartTs;

        /** */
        private UUID cpStartId;

        /** */
        @GridToStringInclude
        private WALPointer startPtr;

        /** */
        private UUID cpEndId;

        /** */
        @GridToStringInclude
        private WALPointer endPtr;

        /**
         * @param cpStartId Checkpoint start ID.
         * @param startPtr Checkpoint start pointer.
         * @param cpEndId Checkpoint end ID.
         * @param endPtr Checkpoint end pointer.
         */
        private CheckpointStatus(long cpStartTs, UUID cpStartId, WALPointer startPtr, UUID cpEndId, WALPointer endPtr) {
            this.cpStartTs = cpStartTs;
            this.cpStartId = cpStartId;
            this.startPtr = startPtr;
            this.cpEndId = cpEndId;
            this.endPtr = endPtr;
        }

        /**
         * @return {@code True} if need to apply page log to restore tree structure.
         */
        public boolean needRestoreMemory() {
            return !F.eq(cpStartId, cpEndId) && !F.eq(NULL_UUID, cpStartId);
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(CheckpointStatus.class, this);
        }
    }

    /**
     *
     */
    private static class CheckpointProgress {
        /** */
        private volatile long nextCpTs;

        /** */
        private GridFutureAdapter cpBeginFut = new GridFutureAdapter<>();

        /** */
        private GridFutureAdapter cpFinishFut = new GridFutureAdapter<Void>() {
            @Override protected boolean onDone(@Nullable Void res, @Nullable Throwable err, boolean cancel) {
                if (err != null && !cpBeginFut.isDone())
                    cpBeginFut.onDone(err);

                return super.onDone(res, err, cancel);
            }
        };

        /** */
        private volatile boolean nextSnapshot;

        /** */
        private volatile boolean started;

        /** */
        private volatile SnapshotOperation snapshotOperation;

        /** Partitions destroy queue. */
        private final PartitionDestroyQueue destroyQueue = new PartitionDestroyQueue();

        /** Wakeup reason. */
        private String reason;

        /**
         * @param nextCpTs Next checkpoint timestamp.
         */
        private CheckpointProgress(long nextCpTs) {
            this.nextCpTs = nextCpTs;
        }
    }

    /**
     *
     */
    private static class CheckpointProgressSnapshot implements CheckpointFuture {
        /** */
        private final boolean started;

        /** */
        private final GridFutureAdapter<Object> cpBeginFut;

        /** */
        private final GridFutureAdapter<Object> cpFinishFut;

        /** */
        CheckpointProgressSnapshot(CheckpointProgress cpProgress) {
            started = cpProgress.started;
            cpBeginFut = cpProgress.cpBeginFut;
            cpFinishFut = cpProgress.cpFinishFut;
        }

        /** {@inheritDoc} */
        @Override public GridFutureAdapter beginFuture() {
            return cpBeginFut;
        }

        /** {@inheritDoc} */
        @Override public GridFutureAdapter finishFuture() {
            return cpFinishFut;
        }
    }

    /**
     * Checkpoint history. Holds chronological ordered map with {@link GridCacheDatabaseSharedManager.CheckpointEntry
     * CheckpointEntries}. Data is loaded from corresponding checkpoint directory. This directory holds files for
     * checkpoint start and end.
     */
    @SuppressWarnings("PublicInnerClass")
    public class CheckpointHistory {
        /**
         * Maps checkpoint's timestamp (from CP file name) to CP entry.
         * Using TS provides historical order of CP entries in map ( first is oldest )
         */
        private final NavigableMap<Long, CheckpointEntry> histMap = new ConcurrentSkipListMap<>();

        /**
         * Load history form checkpoint directory.
         *
         * @param dir Checkpoint state dir.
         */
        private void loadHistory(File dir) throws IgniteCheckedException {
            if (!dir.exists())
                return;

            File[] files = dir.listFiles(CP_FILE_FILTER);

            if (!F.isEmpty(files)) {
                Arrays.sort(files, CP_TS_COMPARATOR);

                ByteBuffer buf = ByteBuffer.allocate(16);
                buf.order(ByteOrder.nativeOrder());

                for (File file : files) {
                    Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

                    if (matcher.matches()) {
                        CheckpointEntryType type = CheckpointEntryType.valueOf(matcher.group(3));

                        if (type == CheckpointEntryType.START) {
                            long cpTs = Long.parseLong(matcher.group(1));
                            UUID cpId = UUID.fromString(matcher.group(2));

                            WALPointer ptr = readPointer(file, buf);

                            if (ptr == null)
                                continue;

                            CheckpointEntry entry = createCheckPointEntry(cpTs, ptr, cpId, null, type);

                            histMap.put(cpTs, entry);
                        }
                    }
                }
            }
        }

        /**
         * @param cpTs Checkpoint timestamp.
         * @return Initialized entry.
         * @throws IgniteCheckedException If failed to initialize entry.
         */
        private CheckpointEntry entry(Long cpTs) throws IgniteCheckedException {
            CheckpointEntry entry = histMap.get(cpTs);

            if (entry == null)
                throw new IgniteCheckedException("Checkpoint entry was removed: " + cpTs);

            return entry;
        }

        /**
         * @return First checkpoint entry if exists. Otherwise {@code null}.
         */
        private CheckpointEntry firstEntry() {
            Map.Entry<Long,CheckpointEntry> entry = histMap.firstEntry();

            return entry != null ? entry.getValue() : null;
        }

        /**
         * Get WAL pointer to low checkpoint bound.
         *
         * @return WAL pointer to low checkpoint bound.
         */
        public WALPointer lowCheckpointBound() {
            CheckpointEntry entry = firstEntry();

            return entry != null ? entry.cpMark : null;
        }

        /**
         * @return Collection of checkpoint timestamps.
         */
        public Collection<Long> checkpoints() {
            return histMap.keySet();
        }

        /**
         * Adds checkpoint entry after the corresponding WAL record has been written to WAL. The checkpoint itself
         * is not finished yet.
         *
         * @param entry Entry to ad.
         */
        private void addCheckpointEntry(CheckpointEntry entry) {
            histMap.put(entry.checkpointTimestamp(), entry);
        }

        /**
         * Callback on truncate wal.
         */
        private void onWalTruncated(WALPointer ptr) {
            FileWALPointer highBound = (FileWALPointer)ptr;

            List<CheckpointEntry> cpToRemove = new ArrayList<>();

            for (CheckpointEntry cpEntry : histMap.values()) {
                FileWALPointer cpPnt = (FileWALPointer)cpEntry.checkpointMark();

                if (highBound.compareTo(cpPnt) <= 0)
                    break;

                if (cctx.wal().reserved(cpEntry.checkpointMark())) {
                    U.warn(log, "Could not clear historyMap due to WAL reservation on cpEntry " + cpEntry.cpId +
                        ", history map size is " + histMap.size());

                    break;
                }

                if (!removeCheckpointFiles(cpEntry))
                    cpToRemove.add(cpEntry);
            }

            for (CheckpointEntry cpEntry : cpToRemove)
                histMap.remove(cpEntry.cpTs);
        }

        /**
         * Clears checkpoint history.
         */
        private void onCheckpointFinished(Checkpoint chp) {
            int deleted = 0;

            boolean dropWal = persistenceCfg.getWalHistorySize() != Integer.MAX_VALUE;

            while (histMap.size() > maxCpHistMemSize) {
                Map.Entry<Long, CheckpointEntry> entry = histMap.firstEntry();

                CheckpointEntry cpEntry = entry.getValue();

                if (cctx.wal().reserved(cpEntry.checkpointMark())) {
                    U.warn(log, "Could not clear historyMap due to WAL reservation on cpEntry " + cpEntry.cpId +
                        ", history map size is " + histMap.size());

                    break;
                }

                boolean fail = removeCheckpointFiles(cpEntry);

                if (!fail) {
                    if (dropWal)
                        deleted += cctx.wal().truncate(null, cpEntry.checkpointMark());

                    histMap.remove(entry.getKey());
                }
                else
                    break;
            }

            chp.walFilesDeleted = deleted;

            if (!chp.cpPages.isEmpty())
                cctx.wal().allowCompressionUntil(chp.cpEntry.checkpointMark());
        }

        /**
         * @param cpEntry Checkpoint entry.
         * @return {True} if delete fail.
         */
        private boolean removeCheckpointFiles(CheckpointEntry cpEntry) {
            File startFile = new File(cpDir.getAbsolutePath(), cpEntry.startFile());
            File endFile = new File(cpDir.getAbsolutePath(), cpEntry.endFile());

            boolean rmvdStart = !startFile.exists() || startFile.delete();
            boolean rmvdEnd = !endFile.exists() || endFile.delete();

            boolean fail = !rmvdStart || !rmvdEnd;

            if (fail) {
                U.warn(log, "Failed to remove stale checkpoint files [startFile=" + startFile.getAbsolutePath() +
                    ", endFile=" + endFile.getAbsolutePath() + ']');

                if (histMap.size() > 2 * maxCpHistMemSize) {
                    U.error(log, "Too many stale checkpoint entries in the map, will truncate WAL archive anyway.");

                    fail = false;
                }
            }

            return fail;
        }
    }

    /**
     * Checkpoint entry.
     */
    private static class CheckpointEntry {
        /** Checkpoint timestamp. */
        private long cpTs;

        /** Checkpoint end mark. */
        private WALPointer cpMark;

        /** Checkpoint ID. */
        private UUID cpId;

        /** */
        private volatile SoftReference<GroupStateLazyStore> grpStateLazyStore;

        /**
         * Checkpoint entry constructor.
         *
         * If {@code grpStates} is null then it will be inited lazy from wal pointer.
         *
         * @param cpTs Checkpoint timestamp.
         * @param cpMark Checkpoint mark pointer.
         * @param cpId Checkpoint ID.
         * @param cacheGrpStates Cache groups states.
         */
        private CheckpointEntry(
            long cpTs,
            WALPointer cpMark,
            UUID cpId,
            @Nullable Map<Integer, CacheState> cacheGrpStates
        ) {
            this.cpTs = cpTs;
            this.cpMark = cpMark;
            this.cpId = cpId;
            this.grpStateLazyStore = new SoftReference<>(new GroupStateLazyStore(cacheGrpStates));
        }

        /**
         * @return Checkpoint timestamp.
         */
        private long checkpointTimestamp() {
            return cpTs;
        }

        /**
         * @return Checkpoint ID.
         */
        private UUID checkpointId() {
            return cpId;
        }

        /**
         * @return Checkpoint mark.
         */
        private WALPointer checkpointMark() {
            return cpMark;
        }

        /**
         * @return Start file name.
         */
        private String startFile() {
            return checkpointFileName(cpTs, cpId, CheckpointEntryType.START);
        }

        /**
         * @return End file name.
         */
        private String endFile() {
            return checkpointFileName(cpTs, cpId, CheckpointEntryType.END);
        }

        /**
         * @param cctx Cache shred context.
         */
        public Map<Integer, GroupState> groupState(
            GridCacheSharedContext cctx
        ) throws IgniteCheckedException {
            GroupStateLazyStore store = initIfNeeded(cctx);

            return store.grpStates;
        }

        /**
         * @param cctx Cache shred context.
         * @return Group lazy store.
         */
        private GroupStateLazyStore initIfNeeded(GridCacheSharedContext cctx) throws IgniteCheckedException {
            GroupStateLazyStore store = grpStateLazyStore.get();

            if (store == null) {
                store = new GroupStateLazyStore();

                grpStateLazyStore = new SoftReference<>(store);
            }

            store.initIfNeeded(cctx, cpMark);

            return store;
        }

        /**
         * @param cctx Cache shared context.
         * @param grpId Cache group ID.
         * @param part Partition ID.
         * @return Partition counter or {@code null} if not found.
         */
        private Long partitionCounter(GridCacheSharedContext cctx, int grpId, int part) {
            GroupStateLazyStore store;

            try {
                store = initIfNeeded(cctx);
            }
            catch (IgniteCheckedException e) {
                return null;
            }

            return store.partitionCounter(grpId, part);
        }

        /**
         *
         */
        private static class GroupState {
            /** */
            private int[] parts;

            /** */
            private long[] cnts;

            /** */
            private int idx;

            /**
             * @param partsCnt Partitions count.
             */
            private GroupState(int partsCnt) {
                parts = new int[partsCnt];
                cnts = new long[partsCnt];
            }

            /**
             * @param partId Partition ID to add.
             * @param cntr Partition counter.
             */
            public void addPartitionCounter(int partId, long cntr) {
                if (idx == parts.length)
                    throw new IllegalStateException("Failed to add new partition to the partitions state " +
                        "(no enough space reserved) [partId=" + partId + ", reserved=" + parts.length + ']');

                if (idx > 0) {
                    if (parts[idx - 1] >= partId)
                        throw new IllegalStateException("Adding partition in a wrong order [prev=" + parts[idx - 1] +
                            ", cur=" + partId + ']');
                }

                parts[idx] = partId;

                cnts[idx] = cntr;

                idx++;
            }

            /**
             * Gets partition counter by partition ID.
             *
             * @param partId Partition ID.
             * @return Partition update counter (will return {@code -1} if partition is not present in the record).
             */
            public long counterByPartition(int partId) {
                int idx = indexByPartition(partId);

                return idx >= 0 ? cnts[idx] : 0;
            }

            public long size(){
                return idx;
            }

            /**
             * @param partId Partition ID to search.
             * @return Non-negative index of partition if found or negative value if not found.
             */
            private int indexByPartition(int partId) {
                return Arrays.binarySearch(parts, 0, idx, partId);
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return "GroupState [cap=" + parts.length + ", size=" + idx + ']';
            }
        }

        /**
         *  Group state lazy store.
         */
        private static class GroupStateLazyStore {
            /** */
            private static final AtomicIntegerFieldUpdater<GroupStateLazyStore> initGuardUpdater =
                AtomicIntegerFieldUpdater.newUpdater(GroupStateLazyStore.class, "initGuard");

            /** Cache states. Initialized lazily. */
            private volatile Map<Integer, GroupState> grpStates;

            /** */
            private final CountDownLatch latch;

            /** */
            @SuppressWarnings("unused")
            private volatile int initGuard;

            /** Initialization exception. */
            private IgniteCheckedException initEx;

            /**
             * Default constructor.
             */
            private GroupStateLazyStore() {
                this(null);
            }

            /**
             * @param cacheGrpStates Cache group state.
             */
            private GroupStateLazyStore(Map<Integer, CacheState> cacheGrpStates) {
                CountDownLatch latch;

                if (cacheGrpStates != null) {
                    initGuard = 1;

                    this.latch = new CountDownLatch(0);
                }
                else
                    this.latch = new CountDownLatch(1);

                this.grpStates = remap(cacheGrpStates);
            }

            /**
             * @param stateRec Cache group state.
             */
            private Map<Integer, GroupState> remap(Map<Integer, CacheState> stateRec) {
                if (stateRec == null)
                    return null;

                Map<Integer, GroupState> grpStates = new HashMap<>(stateRec.size());

                for (Integer grpId : stateRec.keySet()) {
                    CacheState recState = stateRec.get(grpId);

                    GroupState groupState = new GroupState(recState.size());

                    for (int i = 0; i < recState.size(); i++) {
                        groupState.addPartitionCounter(
                            recState.partitionByIndex(i),
                            recState.partitionCounterByIndex(i)
                        );
                    }

                    grpStates.put(grpId, groupState);
                }

                return grpStates;
            }

            /**
             * @param grpId Group id.
             * @param part Partition id.
             * @return Partition counter.
             */
            private Long partitionCounter(int grpId, int part) {
                assert initGuard != 0 : initGuard;

                if (initEx != null || grpStates == null)
                    return null;

                GroupState state = grpStates.get(grpId);

                if (state != null) {
                    long cntr = state.counterByPartition(part);

                    return cntr < 0 ? null : cntr;
                }

                return null;
            }

            /**
             * @param cctx Cache shared context.
             * @param ptr Checkpoint wal pointer.
             * @throws IgniteCheckedException If failed to read WAL entry.
             */
            private void initIfNeeded(
                GridCacheSharedContext cctx,
                WALPointer ptr
            ) throws IgniteCheckedException {
                if (initGuardUpdater.compareAndSet(this, 0, 1)) {
                    try (WALIterator it = cctx.wal().replay(ptr)) {
                        if (it.hasNextX()) {
                            IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

                            CheckpointRecord rec = (CheckpointRecord)tup.get2();

                            Map<Integer, CacheState> stateRec = rec.cacheGroupStates();

                            if (stateRec != null)
                                this.grpStates = remap(stateRec);
                            else
                                grpStates = Collections.emptyMap();
                        }
                        else
                            initEx = new IgniteCheckedException(
                                "Failed to find checkpoint record at the given WAL pointer: " + ptr);
                    }
                    catch (IgniteCheckedException e) {
                        initEx = e;

                        throw e;
                    }
                    finally {
                        latch.countDown();
                    }
                }
                else {
                    U.await(latch);

                    if (initEx != null)
                        throw initEx;
                }
            }
        }
    }

    /**
     *
     */
    public static class FileLockHolder implements AutoCloseable {
        /** Lock file name. */
        private static final String lockFileName = "lock";

        /** File. */
        private File file;

        /** Channel. */
        private RandomAccessFile lockFile;

        /** Lock. */
        private FileLock lock;

        /** Kernal context to generate Id of locked node in file. */
        @NotNull private GridKernalContext ctx;

        /** Logger. */
        private IgniteLogger log;

        /**
         * @param path Path.
         */
        public FileLockHolder(String path, @NotNull GridKernalContext ctx, IgniteLogger log) {
            try {
                file = Paths.get(path, lockFileName).toFile();

                lockFile = new RandomAccessFile(file, "rw");

                this.ctx = ctx;
                this.log = log;
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @param lockWaitTimeMillis During which time thread will try capture file lock.
         * @throws IgniteCheckedException If failed to capture file lock.
         */
        public void tryLock(long lockWaitTimeMillis) throws IgniteCheckedException {
            assert lockFile != null;

            FileChannel ch = lockFile.getChannel();

            SB sb = new SB();

            //write node id
            sb.a("[").a(ctx.localNodeId().toString()).a("]");

            //write ip addresses
            final GridDiscoveryManager discovery = ctx.discovery();

            if (discovery != null) { //discovery may be not up and running
                final ClusterNode node = discovery.localNode();

                if (node != null)
                    sb.a(node.addresses());
            }

            //write ports
            sb.a("[");
            Iterator<GridPortRecord> it = ctx.ports().records().iterator();

            while (it.hasNext()) {
                GridPortRecord rec = it.next();

                sb.a(rec.protocol()).a(":").a(rec.port());

                if (it.hasNext())
                    sb.a(", ");
            }

            sb.a("]");

            String failMsg;

            try {
                String content = null;

                // Try to get lock, if not available wait 1 sec and re-try.
                for (int i = 0; i < lockWaitTimeMillis; i += 1000) {
                    try {
                        lock = ch.tryLock(0, 1, false);
                        if (lock != null && lock.isValid()) {
                            writeContent(sb.toString());

                            return;
                        }
                    }
                    catch (OverlappingFileLockException ignore) {
                        if (content == null)
                            content = readContent();

                        log.warning("Failed to acquire file lock (local nodeId:" + ctx.localNodeId()
                            + ", already locked by " + content + "), will try again in 1s: "
                            + file.getAbsolutePath());
                    }

                    U.sleep(1000);
                }

                if (content == null)
                    content = readContent();

                failMsg = "Failed to acquire file lock during " + (lockWaitTimeMillis / 1000) +
                    " sec, (locked by " + content + "): " + file.getAbsolutePath();
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }

            if (failMsg != null)
                throw new IgniteCheckedException(failMsg);
        }

        /**
         * Write node id (who captured lock) into lock file.
         *
         * @param content Node id.
         * @throws IOException if some fail while write node it.
         */
        private void writeContent(String content) throws IOException {
            FileChannel ch = lockFile.getChannel();

            byte[] bytes = content.getBytes();

            ByteBuffer buf = ByteBuffer.allocate(bytes.length);
            buf.put(bytes);

            buf.flip();

            ch.write(buf, 1);

            ch.force(false);
        }

        /**
         *
         */
        private String readContent() throws IOException {
            FileChannel ch = lockFile.getChannel();

            ByteBuffer buf = ByteBuffer.allocate((int)(ch.size() - 1));

            ch.read(buf, 1);

            String content = new String(buf.array());

            buf.clear();

            return content;
        }

        /** Releases file lock */
        public void release() {
            U.releaseQuiet(lock);
        }

        /** Closes file channel */
        public void close() {
            U.closeQuiet(lockFile);
        }

        /**
         * @return Absolute path to lock file.
         */
        private String lockPath() {
            return file.getAbsolutePath();
        }
    }

    /** {@inheritDoc} */
    @Override public DataStorageMetrics persistentStoreMetrics() {
        return new DataStorageMetricsSnapshot(persStoreMetrics);
    }

    /**
     *
     */
    public DataStorageMetricsImpl persistentStoreMetricsImpl() {
        return persStoreMetrics;
    }

    /** {@inheritDoc} */
    @Override public MetaStorage metaStorage() {
        return metaStorage;
    }

    /** {@inheritDoc} */
    @Override public boolean walEnabled(int grpId, boolean local) {
        if (local)
            return !initiallyLocalWalDisabledGrps.contains(grpId);
        else
            return !initiallyGlobalWalDisabledGrps.contains(grpId);
    }

    /** {@inheritDoc} */
    @Override public void walEnabled(int grpId, boolean enabled, boolean local) {
        String key = walGroupIdToKey(grpId, local);

        checkpointReadLock();

        try {
            if (enabled)
                metaStorage.remove(key);
            else
                metaStorage.write(key, true);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to write cache group WAL state [grpId=" + grpId +
                ", enabled=" + enabled + ']', e);
        }
        finally {
            checkpointReadUnlock();
        }
    }

    /**
     *
     */
    private void fillWalDisabledGroups() {
        MetaStorage meta = cctx.database().metaStorage();

        try {
            Set<String> keys = meta.readForPredicate(WAL_KEY_PREFIX_PRED).keySet();

            if (keys.isEmpty())
                return;

            for (String key : keys) {
                T2<Integer, Boolean> t2 = walKeyToGroupIdAndLocalFlag(key);

                if (t2.get2())
                    initiallyLocalWalDisabledGrps.add(t2.get1());
                else
                    initiallyGlobalWalDisabledGrps.add(t2.get1());
            }

        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to read cache groups WAL state.", e);
        }
    }

    /**
     * Convert cache group ID to WAL state key.
     *
     * @param grpId Group ID.
     * @return Key.
     */
    private static String walGroupIdToKey(int grpId, boolean local) {
        if (local)
            return WAL_LOCAL_KEY_PREFIX + grpId;
        else
            return WAL_GLOBAL_KEY_PREFIX + grpId;
    }

    /**
     * Convert WAL state key to cache group ID.
     *
     * @param key Key.
     * @return Group ID.
     */
    private static T2<Integer, Boolean> walKeyToGroupIdAndLocalFlag(String key) {
        if (key.startsWith(WAL_LOCAL_KEY_PREFIX))
            return new T2<>(Integer.parseInt(key.substring(WAL_LOCAL_KEY_PREFIX.length())), true);
        else
            return new T2<>(Integer.parseInt(key.substring(WAL_GLOBAL_KEY_PREFIX.length())), false);
    }
}
