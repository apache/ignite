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
import java.io.IOException;
import java.io.RandomAccessFile;
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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.DataRegionMetricsProvider;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.LongJVMPauseDetector;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MasterKeyChangeRecord;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MvccDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.MvccTxRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.RollbackRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WalRecordCacheGroupAware;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionDestroyRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionMetaStateRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntry;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointEntryType;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperation;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridCountDownCallback;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.GridReadOnlyArrayView;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.TimeBag;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.mxbean.DataStorageMetricsMXBean;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static java.nio.file.StandardOpenOption.READ;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_RECOVERY_SEMAPHORE_PERMITS;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.LongJVMPauseDetector.DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.CHECKPOINT_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.MASTER_KEY_CHANGE_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.METASTORE_DATA_RECORD;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.fromOrdinal;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.MARKER_STORED_TO_DISK;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.TMP_FILE_MATCHER;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;
import static org.apache.ignite.internal.util.IgniteUtils.checkpointBufferSize;
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;

/**
 *
 */
@SuppressWarnings({"unchecked", "NonPrivateFieldAccessedInSynchronizedContext"})
public class GridCacheDatabaseSharedManager extends IgniteCacheDatabaseSharedManager implements CheckpointWriteProgressSupplier {
    /** */
    public static final String IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC = "IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC";

    /** */
    public static final String IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP = "IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP";

    /** Log read lock holders. */
    public static final String IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS = "IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS";

    /** MemoryPolicyConfiguration name reserved for meta store. */
    public static final String METASTORE_DATA_REGION_NAME = "metastoreMemPlc";

    /**
     * Threshold to calculate limit for pages list on-heap caches.
     * <p>
     * Note: When a checkpoint is triggered, we need some amount of page memory to store pages list on-heap cache.
     * If a checkpoint is triggered by "too many dirty pages" reason and pages list cache is rather big, we can get
     * {@code IgniteOutOfMemoryException}. To prevent this, we can limit the total amount of cached page list buckets,
     * assuming that checkpoint will be triggered if no more then 3/4 of pages will be marked as dirty (there will be
     * at least 1/4 of clean pages) and each cached page list bucket can be stored to up to 2 pages (this value is not
     * static, but depends on PagesCache.MAX_SIZE, so if PagesCache.MAX_SIZE > PagesListNodeIO#getCapacity it can take
     * more than 2 pages). Also some amount of page memory needed to store page list metadata.
     */
    private static final double PAGE_LIST_CACHE_LIMIT_THRESHOLD = 0.1;

    /** Skip sync. */
    private final boolean skipSync = getBoolean(IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);

    /** */
    private final int walRebalanceThreshold = getInteger(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, 500_000);

    /** Value of property for throttling policy override. */
    private final String throttlingPolicyOverride = IgniteSystemProperties.getString(
        IgniteSystemProperties.IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED);

    /** */
    private final boolean skipCheckpointOnNodeStop = getBoolean(IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, false);

    /** */
    private final boolean logReadLockHolders = getBoolean(IGNITE_PDS_LOG_CP_READ_LOCK_HOLDERS);

    /**
     * Starting from this number of dirty pages in checkpoint, array will be sorted with
     * {@link Arrays#parallelSort(Comparable[])} in case of {@link CheckpointWriteOrder#SEQUENTIAL}.
     */
    private final int parallelSortThreshold = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.CHECKPOINT_PARALLEL_SORT_THRESHOLD, 512 * 1024);

    /** Checkpoint lock hold count. */
    private static final ThreadLocal<Integer> CHECKPOINT_LOCK_HOLD_COUNT = ThreadLocal.withInitial(() -> 0);

    /** Assertion enabled. */
    private static final boolean ASSERTION_ENABLED = GridCacheDatabaseSharedManager.class.desiredAssertionStatus();

    /** Checkpoint file name pattern. */
    public static final Pattern CP_FILE_NAME_PATTERN = Pattern.compile("(\\d+)-(.*)-(START|END)\\.bin");

    /** */
    private static final String MBEAN_NAME = "DataStorageMetrics";

    /** */
    private static final String MBEAN_GROUP = "Persistent Store";

    /** WAL marker prefix for meta store. */
    private static final String WAL_KEY_PREFIX = "grp-wal-";

    /** Prefix for meta store records which means that WAL was disabled globally for some group. */
    private static final String WAL_GLOBAL_KEY_PREFIX = WAL_KEY_PREFIX + "disabled-";

    /** Prefix for meta store records which means that WAL was disabled locally for some group. */
    private static final String WAL_LOCAL_KEY_PREFIX = WAL_KEY_PREFIX + "local-disabled-";

    /** Prefix for meta store records which means that checkpoint entry for some group is not applicable for WAL rebalance. */
    private static final String CHECKPOINT_INAPPLICABLE_FOR_REBALANCE = "cp-wal-rebalance-inapplicable-";

    /** Timeout between partition file destroy and checkpoint to handle it. */
    private static final long PARTITION_DESTROY_CHECKPOINT_TIMEOUT = 30 * 1000; // 30 Seconds.

    /** */
    private static final String CHECKPOINT_RUNNER_THREAD_PREFIX = "checkpoint-runner";

    /** This number of threads will be created and used for parallel sorting. */
    private static final int PARALLEL_SORT_THREADS = Math.min(Runtime.getRuntime().availableProcessors(), 8);

    /** Checkpoint thread. Needs to be volatile because it is created in exchange worker. */
    private volatile Checkpointer checkpointer;

    /** Checkpointer thread instance. */
    private volatile IgniteThread checkpointerThread;

    /** For testing only. */
    private volatile boolean checkpointsEnabled = true;

    /** For testing only. */
    private volatile GridFutureAdapter<Void> enableChangeApplied;

    /** Checkpont lock. */
    ReentrantReadWriteLock checkpointLock = new ReentrantReadWriteLock();

    /** */
    private long checkpointFreq;

    /** */
    private CheckpointHistory cpHistory;

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

    /** */
    private boolean stopping;

    /**
     * The position of last seen WAL pointer. Used for resumming logging from this pointer.
     *
     * If binary memory recovery pefrormed on node start, the checkpoint END pointer will store
     * not the last WAL pointer and can't be used for resumming logging.
     */
    private volatile WALPointer walTail;

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private IgniteThreadPoolExecutor asyncRunner;

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
    private final boolean truncateWalOnCpFinish;

    /** */
    private Map</*grpId*/Integer, Map</*partId*/Integer, T2</*updCntr*/Long, WALPointer>>> reservedForExchange;

    /** */
    private final ConcurrentMap<T2</*grpId*/Integer, /*partId*/Integer>, T2</*updCntr*/Long, WALPointer>> reservedForPreloading = new ConcurrentHashMap<>();

    /** Snapshot manager. */
    private IgniteCacheSnapshotManager snapshotMgr;

    /** */
    private DataStorageMetricsImpl persStoreMetrics;

    /** Counter for written checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger writtenPagesCntr = null;

    /** Counter for fsynced checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger syncedPagesCntr = null;

    /** Counter for evicted checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger evictedPagesCntr = null;

    /** Number of pages in current checkpoint at the beginning of checkpoint. */
    private volatile int currCheckpointPagesCnt;

    /**
     * MetaStorage instance. Value {@code null} means storage not initialized yet.
     * Guarded by {@link GridCacheDatabaseSharedManager#checkpointReadLock()}
     */
    private MetaStorage metaStorage;

    /** Temporary metastorage to migration of index partition. {@see IGNITE-8735}. */
    private MetaStorage.TmpStorage tmpMetaStorage;

    /** */
    private List<MetastorageLifecycleListener> metastorageLifecycleLsnrs;

    /** Initially disabled cache groups. */
    private Collection<Integer> initiallyGlobalWalDisabledGrps = new HashSet<>();

    /** Initially local wal disabled groups. */
    private Collection<Integer> initiallyLocalWalDisabledGrps = new HashSet<>();

    /** File I/O factory for writing checkpoint markers. */
    private final FileIOFactory ioFactory;

    /** Timeout for checkpoint read lock acquisition in milliseconds. */
    private volatile long checkpointReadLockTimeout;

    /** Flag allows to log additional information about partitions during recovery phases. */
    private final boolean recoveryVerboseLogging =
        getBoolean(IgniteSystemProperties.IGNITE_RECOVERY_VERBOSE_LOGGING, false);

    /** Pointer to a memory recovery record that should be included into the next checkpoint record. */
    private volatile WALPointer memoryRecoveryRecordPtr;

    /** Page list cache limits per data region. */
    private final Map<String, AtomicLong> pageListCacheLimits = new ConcurrentHashMap<>();

    /** Lock for releasing history for preloading. */
    private ReentrantLock releaseHistForPreloadingLock = new ReentrantLock();

    /**
     * @param ctx Kernal context.
     */
    public GridCacheDatabaseSharedManager(GridKernalContext ctx) {
        IgniteConfiguration cfg = ctx.config();

        persistenceCfg = cfg.getDataStorageConfiguration();

        assert persistenceCfg != null;

        checkpointFreq = persistenceCfg.getCheckpointFrequency();

        truncateWalOnCpFinish = persistenceCfg.isWalHistorySizeParameterUsed()
            ? persistenceCfg.getWalHistorySize() != Integer.MAX_VALUE
            : persistenceCfg.getMaxWalArchiveSize() != Long.MAX_VALUE;

        lockWaitTime = persistenceCfg.getLockWaitTime();

        persStoreMetrics = new DataStorageMetricsImpl(
            ctx.metric(),
            persistenceCfg.isMetricsEnabled(),
            persistenceCfg.getMetricsRateTimeInterval(),
            persistenceCfg.getMetricsSubIntervalCount()
        );

        ioFactory = persistenceCfg.getFileIOFactory();

        Long cfgCheckpointReadLockTimeout = ctx.config().getDataStorageConfiguration() != null
            ? ctx.config().getDataStorageConfiguration().getCheckpointReadLockTimeout()
            : null;

        checkpointReadLockTimeout = IgniteSystemProperties.getLong(IGNITE_CHECKPOINT_READ_LOCK_TIMEOUT,
            cfgCheckpointReadLockTimeout != null
                ? cfgCheckpointReadLockTimeout
                : (ctx.workersRegistry() != null
                    ? ctx.workersRegistry().getSystemWorkerBlockedTimeout()
                    : ctx.config().getFailureDetectionTimeout()));
    }

    /**
     * @return File store manager.
     */
    public FilePageStoreManager getFileStoreManager() {
        return storeMgr;
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
     *
     * @return Checkpointer thread instance.
     */
    public IgniteThread checkpointerThread() {
        return checkpointerThread;
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
    @Override protected void initDataRegions0(DataStorageConfiguration memCfg) throws IgniteCheckedException {
        super.initDataRegions0(memCfg);

        addDataRegion(
            memCfg,
            createMetastoreDataRegionConfig(memCfg),
            false
        );

        persStoreMetrics.regionMetrics(memMetricsMap.values());
    }

    /**
     * Create metastorage data region configuration with enabled persistence by default.
     *
     * @param storageCfg Data storage configuration.
     * @return Data region configuration.
     */
    private DataRegionConfiguration createMetastoreDataRegionConfig(DataStorageConfiguration storageCfg) {
        DataRegionConfiguration cfg = new DataRegionConfiguration();

        cfg.setName(METASTORE_DATA_REGION_NAME);
        cfg.setInitialSize(storageCfg.getSystemRegionInitialSize());
        cfg.setMaxSize(storageCfg.getSystemRegionMaxSize());
        cfg.setPersistenceEnabled(true);
        cfg.setLazyMemoryAllocation(false);

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

        if (logReadLockHolders)
            checkpointLock = new U.ReentrantReadWriteLockTracer(checkpointLock, kernalCtx, 5_000);

        if (!kernalCtx.clientNode()) {
            kernalCtx.internalSubscriptionProcessor().registerDatabaseListener(new MetastorageRecoveryLifecycle());

            checkpointer = new Checkpointer(cctx.igniteInstanceName(), "db-checkpoint-thread", log);

            cpHistory = new CheckpointHistory(kernalCtx);

            IgnitePageStoreManager store = cctx.pageStore();

            assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

            storeMgr = (FilePageStoreManager)store;

            cpDir = Paths.get(storeMgr.workDir().getAbsolutePath(), "cp").toFile();

            if (!U.mkdirs(cpDir))
                throw new IgniteCheckedException("Could not create directory for checkpoint metadata: " + cpDir);

            final FileLockHolder preLocked = kernalCtx.pdsFolderResolver()
                    .resolveFolders()
                    .getLockedFileLockHolder();

            acquireFileLock(preLocked);

            cleanupTempCheckpointDirectory();

            persStoreMetrics.wal(cctx.wal());
        }
    }

    /**
     * Cleanup checkpoint directory from all temporary files.
     */
    @Override public void cleanupTempCheckpointDirectory() throws IgniteCheckedException {
        try {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(cpDir.toPath(), TMP_FILE_MATCHER::matches)) {
                for (Path path : files)
                    Files.delete(path);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to cleanup checkpoint directory from temporary files: " + cpDir, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cleanupRestoredCaches() {
        if (dataRegionMap.isEmpty())
            return;

        boolean hasMvccCache = false;

        for (CacheGroupDescriptor grpDesc : cctx.cache().cacheGroupDescriptors().values()) {
            hasMvccCache |= grpDesc.config().getAtomicityMode() == TRANSACTIONAL_SNAPSHOT;

            String regionName = grpDesc.config().getDataRegionName();

            DataRegion region = regionName != null ? dataRegionMap.get(regionName) : dfltDataRegion;

            if (region == null)
                continue;

            if (log.isInfoEnabled())
                log.info("Page memory " + region.config().getName() + " for " + grpDesc + " has invalidated.");

            int partitions = grpDesc.config().getAffinity().partitions();

            if (region.pageMemory() instanceof PageMemoryEx) {
                PageMemoryEx memEx = (PageMemoryEx)region.pageMemory();

                for (int partId = 0; partId < partitions; partId++)
                    memEx.invalidate(grpDesc.groupId(), partId);

                memEx.invalidate(grpDesc.groupId(), PageIdAllocator.INDEX_PARTITION);
            }
        }

        if (!hasMvccCache && dataRegionMap.containsKey(TxLog.TX_LOG_CACHE_NAME)) {
            PageMemory memory = dataRegionMap.get(TxLog.TX_LOG_CACHE_NAME).pageMemory();

            if (memory instanceof PageMemoryEx)
                ((PageMemoryEx)memory).invalidate(TxLog.TX_LOG_CACHE_ID, PageIdAllocator.INDEX_PARTITION);
        }

        final boolean hasMvccCache0 = hasMvccCache;

        storeMgr.cleanupPageStoreIfMatch(
            new Predicate<Integer>() {
                @Override public boolean test(Integer grpId) {
                    return MetaStorage.METASTORAGE_CACHE_ID != grpId &&
                        (TxLog.TX_LOG_CACHE_ID != grpId || !hasMvccCache0);
                }
            },
            true);
    }

    /** {@inheritDoc} */
    @Override public void cleanupCheckpointDirectory() throws IgniteCheckedException {
        if (cpHistory != null)
            cpHistory = new CheckpointHistory(cctx.kernalContext());

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
     * @param preLocked Pre-locked file lock holder.
     */
    private void acquireFileLock(FileLockHolder preLocked) throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode())
            return;

        fileLockHolder = preLocked == null ?
            new FileLockHolder(storeMgr.workDir().getPath(), cctx.kernalContext(), log) : preLocked;

        if (!fileLockHolder.isLocked()) {
            if (log.isDebugEnabled())
                log.debug("Try to capture file lock [nodeId=" +
                    cctx.localNodeId() + " path=" + fileLockHolder.lockPath() + "]");

            fileLockHolder.tryLock(lockWaitTime);
        }
    }

    /**
     *
     */
    private void releaseFileLock() {
        if (cctx.kernalContext().clientNode() || fileLockHolder == null)
            return;

        if (log.isDebugEnabled())
            log.debug("Release file lock [nodeId=" +
                cctx.localNodeId() + " path=" + fileLockHolder.lockPath() + "]");

        fileLockHolder.close();
    }

    /**
     * Retreives checkpoint history form specified {@code dir}.
     *
     * @return List of checkpoints.
     */
    private List<CheckpointEntry> retreiveHistory() throws IgniteCheckedException {
        if (!cpDir.exists())
            return Collections.emptyList();

        try (DirectoryStream<Path> cpFiles = Files.newDirectoryStream(
            cpDir.toPath(),
            path -> CP_FILE_NAME_PATTERN.matcher(path.toFile().getName()).matches())
        ) {
            List<CheckpointEntry> checkpoints = new ArrayList<>();

            ByteBuffer buf = ByteBuffer.allocate(FileWALPointer.POINTER_SIZE);
            buf.order(ByteOrder.nativeOrder());

            for (Path cpFile : cpFiles) {
                CheckpointEntry cp = parseFromFile(buf, cpFile.toFile());

                if (cp != null)
                    checkpoints.add(cp);
            }

            return checkpoints;
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to load checkpoint history.", e);
        }
    }

    /**
     * Parses checkpoint entry from given file.
     *
     * @param buf Temporary byte buffer.
     * @param file Checkpoint file.
     */
    @Nullable private CheckpointEntry parseFromFile(ByteBuffer buf, File file) throws IgniteCheckedException {
        Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

        if (!matcher.matches())
            return null;

        CheckpointEntryType type = CheckpointEntryType.valueOf(matcher.group(3));

        if (type != CheckpointEntryType.START)
            return null;

        long cpTs = Long.parseLong(matcher.group(1));
        UUID cpId = UUID.fromString(matcher.group(2));

        WALPointer ptr = readPointer(file, buf);

        return createCheckPointEntry(cpTs, ptr, cpId, null, CheckpointEntryType.START);
    }

    /**
     * Removes checkpoint start/end files belongs to given {@code cpEntry}.
     *
     * @param cpEntry Checkpoint entry.
     *
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

    /** */
    private void readMetastore() throws IgniteCheckedException {
        try {
            CheckpointStatus status = readCheckpointStatus();

            checkpointReadLock();

            try {
                dataRegion(METASTORE_DATA_REGION_NAME).pageMemory().start();

                performBinaryMemoryRestore(status, onlyMetastorageGroup(), physicalRecords(), false);

                metaStorage = createMetastorage(true);

                applyLogicalUpdates(status, onlyMetastorageGroup(), onlyMetastorageAndEncryptionRecords(), false);

                fillWalDisabledGroups();

                notifyMetastorageReadyForRead();
            }
            finally {
                metaStorage = null;

                dataRegion(METASTORE_DATA_REGION_NAME).pageMemory().stop(false);

                cctx.pageStore().cleanupPageStoreIfMatch(new Predicate<Integer>() {
                    @Override public boolean test(Integer grpId) {
                        return MetaStorage.METASTORAGE_CACHE_ID == grpId;
                    }
                }, false);

                checkpointReadUnlock();
            }
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext ctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate database manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        snapshotMgr = cctx.snapshot();

        if (!cctx.kernalContext().clientNode() && checkpointer == null)
            checkpointer = new Checkpointer(cctx.igniteInstanceName(), "db-checkpoint-thread", log);

        super.onActivate(ctx);

        if (!cctx.kernalContext().clientNode()) {
            initializeCheckpointPool();

            finishRecovery();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate database manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        onKernalStop0(false);

        super.onDeActivate(kctx);

        /* Must be here, because after deactivate we can invoke activate and file lock must be already configured */
        stopping = false;
    }

    /**
     *
     */
    private void initializeCheckpointPool() {
        if (persistenceCfg.getCheckpointThreads() > 1)
            asyncRunner = new IgniteThreadPoolExecutor(
                CHECKPOINT_RUNNER_THREAD_PREFIX,
                cctx.igniteInstanceName(),
                persistenceCfg.getCheckpointThreads(),
                persistenceCfg.getCheckpointThreads(),
                30_000,
                new LinkedBlockingQueue<Runnable>()
            );
    }

    /** {@inheritDoc} */
    @Override protected void registerMetricsMBeans(IgniteConfiguration cfg) {
        super.registerMetricsMBeans(cfg);

        registerMetricsMBean(
            cctx.kernalContext().config(),
            MBEAN_GROUP,
            MBEAN_NAME,
            persStoreMetrics,
            DataStorageMetricsMXBean.class
        );
    }

    /** {@inheritDoc} */
    @Deprecated
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
    @Override protected DataRegionMetricsProvider dataRegionMetricsProvider(final DataRegionConfiguration dataRegCfg) {
        if (!dataRegCfg.isPersistenceEnabled())
            return super.dataRegionMetricsProvider(dataRegCfg);

        final String dataRegName = dataRegCfg.getName();

        return new DataRegionMetricsProvider() {
            @Override public long partiallyFilledPagesFreeSpace() {
                long freeSpace = 0L;

                for (CacheGroupContext grpCtx : cctx.cache().cacheGroups()) {
                    if (!grpCtx.dataRegion().config().getName().equals(dataRegName))
                        continue;

                    assert grpCtx.offheap() instanceof GridCacheOffheapManager;

                    freeSpace += ((GridCacheOffheapManager)grpCtx.offheap()).freeSpace();
                }

                return freeSpace;
            }

            @Override public long emptyDataPages() {
                long emptyDataPages = 0L;

                for (CacheGroupContext grpCtx : cctx.cache().cacheGroups()) {
                    if (!grpCtx.dataRegion().config().getName().equals(dataRegName))
                        continue;

                    assert grpCtx.offheap() instanceof GridCacheOffheapManager;

                    emptyDataPages += ((GridCacheOffheapManager)grpCtx.offheap()).emptyDataPages();
                }

                return emptyDataPages;
            }
        };
    }

    /**
     * Restores last valid WAL pointer and resumes logging from that pointer.
     * Re-creates metastorage if needed.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void finishRecovery() throws IgniteCheckedException {
        assert !cctx.kernalContext().clientNode();

        long time = System.currentTimeMillis();

        CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);

        try {
            for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
                lsnr.beforeResumeWalLogging(this);

            // Try to resume logging since last finished checkpoint if possible.
            if (walTail == null) {
                CheckpointStatus status = readCheckpointStatus();

                walTail = CheckpointStatus.NULL_PTR.equals(status.endPtr) ? null : status.endPtr;
            }

            cctx.wal().resumeLogging(walTail);

            walTail = null;

            // Recreate metastorage to refresh page memory state after deactivation.
            if (metaStorage == null)
                metaStorage = createMetastorage(false);

            notifyMetastorageReadyForReadWrite();

            U.log(log, "Finish recovery performed in " + (System.currentTimeMillis() - time) + " ms.");
        }
        catch (IgniteCheckedException e) {
            if (X.hasCause(e, StorageException.class, IOException.class))
                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
        finally {
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() - 1);
        }
    }

    /**
     * @param readOnly Metastorage read-only mode.
     * @return Instance of Metastorage.
     * @throws IgniteCheckedException If failed to create metastorage.
     */
    private MetaStorage createMetastorage(boolean readOnly) throws IgniteCheckedException {
        cctx.pageStore().initializeForMetastorage();

        MetaStorage storage = new MetaStorage(
            cctx,
            dataRegion(METASTORE_DATA_REGION_NAME),
            (DataRegionMetricsImpl) memMetricsMap.get(METASTORE_DATA_REGION_NAME),
            readOnly
        );

        storage.init(this);

        return storage;
    }

    /**
     * @param cacheGroupsPredicate Cache groups to restore.
     * @param recordTypePredicate Filter records by type.
     * @return Last seen WAL pointer during binary memory recovery.
     * @throws IgniteCheckedException If failed.
     */
    private RestoreBinaryState restoreBinaryMemory(
        IgnitePredicate<Integer> cacheGroupsPredicate,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordTypePredicate
    ) throws IgniteCheckedException {
        long time = System.currentTimeMillis();

        try {
            log.info("Starting binary memory restore for: " + cctx.cache().cacheGroupDescriptors().keySet());

            for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
                lsnr.beforeBinaryMemoryRestore(this);

            CheckpointStatus status = readCheckpointStatus();

            // First, bring memory to the last consistent checkpoint state if needed.
            // This method should return a pointer to the last valid record in the WAL.
            RestoreBinaryState binaryState = performBinaryMemoryRestore(
                status,
                cacheGroupsPredicate,
                recordTypePredicate,
                true
            );

            WALPointer restored = binaryState.lastReadRecordPointer();

            if(restored.equals(CheckpointStatus.NULL_PTR))
                restored = null; // This record is first
            else
                restored = restored.next();

            if (restored == null && !status.endPtr.equals(CheckpointStatus.NULL_PTR)) {
                throw new StorageException("The memory cannot be restored. The critical part of WAL archive is missing " +
                    "[tailWalPtr=" + restored + ", endPtr=" + status.endPtr + ']');
            }
            else if (restored != null)
                U.log(log, "Binary memory state restored at node startup [restoredPtr=" + restored + ']');

            // Wal logging is now available.
            cctx.wal().resumeLogging(restored);

            // Log MemoryRecoveryRecord to make sure that old physical records are not replayed during
            // next physical recovery.
            memoryRecoveryRecordPtr = cctx.wal().log(new MemoryRecoveryRecord(U.currentTimeMillis()));

            for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
                lsnr.afterBinaryMemoryRestore(this, binaryState);

            if (log.isInfoEnabled())
                log.info("Binary recovery performed in " + (System.currentTimeMillis() - time) + " ms.");

            return binaryState;
        }
        catch (IgniteCheckedException e) {
            if (X.hasCause(e, StorageException.class, IOException.class))
                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
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

        unregisterMetricsMBean(
            cctx.gridConfig(),
            MBEAN_GROUP,
            MBEAN_NAME
        );

        metaStorage = null;
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        releaseFileLock();
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

                // We can write only page from disk into snapshot.
                snapshotMgr.beforePageWrite(fullId);

                // Write page to disk.
                storeMgr.write(fullId.groupId(), fullId.pageId(), pageBuf, tag);

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

            @Override public void shutdown(boolean deallocate) {
                memProvider.shutdown(deallocate);
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
                log.error("Incorrect value of IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED property. " +
                    "The default throttling policy will be used [plc=" + throttlingPolicyOverride +
                    ", defaultPlc=" + plc + ']');
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
            U.warn(log, "Page eviction mode will have no effect because the oldest pages are evicted automatically " +
                "if Ignite persistence is enabled: " + regCfg.getName());
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
        try (FileIO fileIO = ioFactory.create(partFile.toFile())) {
            int minimalHdr = FilePageStore.HEADER_SIZE;

            if (fileIO.size() < minimalHdr)
                throw new IgniteCheckedException("Partition file is too small: " + partFile);

            ByteBuffer hdr = ByteBuffer.allocate(minimalHdr).order(ByteOrder.nativeOrder());

            fileIO.readFully(hdr);

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

                        cp.scheduledCp.fail(new NodeStoppingException("Checkpointer is stopped during node stop."));

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
        // Try to restore partition states.
        if (fut.localJoinExchange() || fut.activateCluster()
            || (fut.exchangeActions() != null && !F.isEmpty(fut.exchangeActions().cacheGroupsToStart()))) {
            U.doInParallel(
                cctx.kernalContext().getSystemExecutorService(),
                cctx.cache().cacheGroups(),
                cacheGroup -> {
                    if (cacheGroup.isLocal())
                        return null;

                    cctx.database().checkpointReadLock();

                    try {
                        cacheGroup.offheap().restorePartitionStates(Collections.emptyMap());

                        if (cacheGroup.localStartVersion().equals(fut.initialVersion()))
                            cacheGroup.topology().afterStateRestored(fut.initialVersion());

                        fut.timeBag().finishLocalStage("Restore partition states " +
                            "[grp=" + cacheGroup.cacheOrGroupName() + "]");
                    }
                    finally {
                        cctx.database().checkpointReadUnlock();
                    }

                    return null;
                }
            );

            fut.timeBag().finishGlobalStage("Restore partition states");
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
    @Override public void rebuildIndexesIfNeeded(GridDhtPartitionsExchangeFuture exchangeFut) {
        GridQueryProcessor qryProc = cctx.kernalContext().query();

        if (!qryProc.moduleEnabled())
            return;

        Collection<GridCacheContext> cacheContexts = cctx.cacheContexts();

        GridCountDownCallback rebuildIndexesCompleteCntr = new GridCountDownCallback(
            cacheContexts.size(),
            () -> {
                if (log.isInfoEnabled())
                    log.info("Indexes rebuilding completed for all caches.");
            },
            1  //need at least 1 index rebuilded to print message about rebuilding completion
        );

        for (GridCacheContext cacheCtx : cacheContexts) {
            if (!cacheCtx.startTopologyVersion().equals(exchangeFut.initialVersion()))
                continue;

            int cacheId = cacheCtx.cacheId();
            GridFutureAdapter<Void> usrFut = idxRebuildFuts.get(cacheId);

            IgniteInternalFuture<?> rebuildFut = qryProc.rebuildIndexesFromHash(cacheCtx);

            if (nonNull(rebuildFut)) {
                if (log.isInfoEnabled())
                    log.info("Started indexes rebuilding for cache [" + cacheInfo(cacheCtx) + ']');

                assert nonNull(usrFut) : "Missing user future for cache: " + cacheCtx.name();

                rebuildFut.listen(fut -> {
                    idxRebuildFuts.remove(cacheId, usrFut);

                    Throwable err = fut.error();

                    usrFut.onDone(err);

                    if (isNull(err)) {
                        if (log.isInfoEnabled())
                            log.info("Finished indexes rebuilding for cache [" + cacheInfo(cacheCtx) + ']');
                    }
                    else {
                        if (!(err instanceof NodeStoppingException))
                            log.error("Failed to rebuild indexes for cache [" + cacheInfo(cacheCtx) + ']', err);
                    }

                    rebuildIndexesCompleteCntr.countDown(true);
                });
            }
            else if (nonNull(usrFut)) {
                idxRebuildFuts.remove(cacheId, usrFut);

                usrFut.onDone();

                rebuildIndexesCompleteCntr.countDown(false);
            }
        }
    }

    /**
     * Return short information about cache.
     *
     * @param cacheCtx Cache context.
     * @return Short cache info.
     */
    private String cacheInfo(GridCacheContext cacheCtx) {
        assert nonNull(cacheCtx);

        return "name=" + cacheCtx.name() + ", grpName=" + cacheCtx.group().name();
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

            snapshotMgr.onCacheGroupStop(gctx, tup.get2());

            PageMemoryEx pageMem = (PageMemoryEx)gctx.dataRegion().pageMemory();

            Collection<Integer> grpIds = destroyed.computeIfAbsent(pageMem, k -> new HashSet<>());

            grpIds.add(tup.get1().groupId());

            pageMem.onCacheGroupDestroyed(tup.get1().groupId());

            if (tup.get2())
                cctx.kernalContext().encryption().onCacheGroupDestroyed(gctx.groupId());
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

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquireSnapshotWorker memory
     * state.
     * @throws IgniteException If failed.
     */
    @Override public void checkpointReadLock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return;

        long timeout = checkpointReadLockTimeout;

        long start = U.currentTimeMillis();

        boolean interruped = false;

        try {
            for (; ; ) {
                try {
                    if (timeout > 0 && (U.currentTimeMillis() - start) >= timeout)
                        failCheckpointReadLock();

                    try {
                        if (timeout > 0) {
                            if (!checkpointLock.readLock().tryLock(timeout - (U.currentTimeMillis() - start),
                                TimeUnit.MILLISECONDS))
                                failCheckpointReadLock();
                        }
                        else
                            checkpointLock.readLock().lock();
                    }
                    catch (InterruptedException e) {
                        interruped = true;

                        continue;
                    }

                    if (stopping) {
                        checkpointLock.readLock().unlock();

                        throw new IgniteException(new NodeStoppingException("Failed to perform cache update: node is stopping."));
                    }

                    if (checkpointLock.getReadHoldCount() > 1 || safeToUpdatePageMemories() || checkpointerThread == null)
                        break;
                    else {
                        checkpointLock.readLock().unlock();

                        if (timeout > 0 && U.currentTimeMillis() - start >= timeout)
                            failCheckpointReadLock();

                        try {
                            checkpointer.wakeupForCheckpoint(0, "too many dirty pages")
                                .futureFor(LOCK_RELEASED)
                                .getUninterruptibly();
                        }
                        catch (IgniteFutureTimeoutCheckedException e) {
                            failCheckpointReadLock();
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException("Failed to wait for checkpoint begin.", e);
                        }
                    }
                }
                catch (CheckpointReadLockTimeoutException e) {
                    log.error(e.getMessage(), e);

                    timeout = 0;
                }
            }
        }
        finally {
            if (interruped)
                Thread.currentThread().interrupt();
        }

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);
    }

    /**
     * Invokes critical failure processing. Always throws.
     *
     * @throws CheckpointReadLockTimeoutException If node was not invalidated as result of handling.
     * @throws IgniteException If node was invalidated as result of handling.
     */
    private void failCheckpointReadLock() throws CheckpointReadLockTimeoutException, IgniteException {
        String msg = "Checkpoint read lock acquisition has been timed out.";

        IgniteException e = new IgniteException(msg);

        if (cctx.kernalContext().failure().process(new FailureContext(SYSTEM_CRITICAL_OPERATION_TIMEOUT, e)))
            throw e;

        throw new CheckpointReadLockTimeoutException(msg);
    }

    /** {@inheritDoc} */
    @Override public boolean checkpointLockIsHeldByThread() {
        return !ASSERTION_ENABLED ||
            checkpointLock.isWriteLockedByCurrentThread() ||
            CHECKPOINT_LOCK_HOLD_COUNT.get() > 0 ||
            Thread.currentThread().getName().startsWith(CHECKPOINT_RUNNER_THREAD_PREFIX);
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

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() - 1);
    }

    /** {@inheritDoc} */
    @Override public synchronized Map<Integer, Map<Integer, Long>> reserveHistoryForExchange() {
        assert reservedForExchange == null : reservedForExchange;

        reservedForExchange = new HashMap<>();

        Map</*grpId*/Integer, Set</*partId*/Integer>> applicableGroupsAndPartitions = partitionsApplicableForWalRebalance();

        Map</*grpId*/Integer, Map</*partId*/Integer, CheckpointEntry>> earliestValidCheckpoints;

        checkpointReadLock();

        try {
            earliestValidCheckpoints = cpHistory.searchAndReserveCheckpoints(applicableGroupsAndPartitions);
        }
        finally {
            checkpointReadUnlock();
        }

        Map</*grpId*/Integer, Map</*partId*/Integer, /*updCntr*/Long>> grpPartsWithCnts = new HashMap<>();

        for (Map.Entry<Integer, Map<Integer, CheckpointEntry>> e : earliestValidCheckpoints.entrySet()) {
            int grpId = e.getKey();

            for (Map.Entry<Integer, CheckpointEntry> e0 : e.getValue().entrySet()) {
                CheckpointEntry cpEntry = e0.getValue();

                int partId = e0.getKey();

                assert cctx.wal().reserved(cpEntry.checkpointMark())
                    : "WAL segment for checkpoint " + cpEntry + " has not reserved";

                Long updCntr = cpEntry.partitionCounter(cctx, grpId, partId);

                if (updCntr != null) {
                    reservedForExchange.computeIfAbsent(grpId, k -> new HashMap<>())
                        .put(partId, new T2<>(updCntr, cpEntry.checkpointMark()));

                    grpPartsWithCnts.computeIfAbsent(grpId, k -> new HashMap<>()).put(partId, updCntr);
                }
            }
        }

        return grpPartsWithCnts;
    }

    /**
     * @return Map of group id -> Set of partitions which can be used as suppliers for WAL rebalance.
     */
    private Map<Integer, Set<Integer>> partitionsApplicableForWalRebalance() {
        Map<Integer, Set<Integer>> res = new HashMap<>();

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            for (GridDhtLocalPartition locPart : grp.topology().currentLocalPartitions()) {
                if (locPart.state() == GridDhtPartitionState.OWNING && locPart.fullSize() > walRebalanceThreshold)
                    res.computeIfAbsent(grp.groupId(), k -> new HashSet<>()).add(locPart.id());
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public synchronized void releaseHistoryForExchange() {
        if (reservedForExchange == null)
            return;

        FileWALPointer earliestPtr = null;

        for (Map.Entry<Integer, Map<Integer, T2<Long, WALPointer>>> e : reservedForExchange.entrySet()) {
            for (Map.Entry<Integer, T2<Long, WALPointer>> e0 : e.getValue().entrySet()) {
                FileWALPointer ptr = (FileWALPointer) e0.getValue().get2();

                if (earliestPtr == null || ptr.index() < earliestPtr.index())
                    earliestPtr = ptr;
            }
        }

        reservedForExchange = null;

        if (earliestPtr == null)
            return;

        assert cctx.wal().reserved(earliestPtr)
            : "Earliest checkpoint WAL pointer is not reserved for exchange: " + earliestPtr;

        try {
            cctx.wal().release(earliestPtr);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to release earliest checkpoint WAL pointer: " + earliestPtr, e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean reserveHistoryForPreloading(int grpId, int partId, long cntr) {
        CheckpointEntry cpEntry = cpHistory.searchCheckpointEntry(grpId, partId, cntr);

        if (cpEntry == null)
            return false;

        WALPointer ptr = cpEntry.checkpointMark();

        if (ptr == null)
            return false;

        boolean reserved = cctx.wal().reserve(ptr);

        if (reserved)
            reservedForPreloading.put(new T2<>(grpId, partId), new T2<>(cntr, ptr));

        return reserved;
    }

    /** {@inheritDoc} */
    @Override public void releaseHistoryForPreloading() {
        releaseHistForPreloadingLock.lock();

        try {
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
        finally {
            releaseHistForPreloadingLock.unlock();
        }
    }

    /**
     *
     */
    @Nullable @Override public IgniteInternalFuture wakeupForCheckpoint(String reason) {
        Checkpointer cp = checkpointer;

        if (cp != null)
            return cp.wakeupForCheckpoint(0, reason).futureFor(LOCK_RELEASED);

        return null;
    }

    /** {@inheritDoc} */
    @Override public <R> void waitForCheckpoint(String reason, IgniteInClosure<? super IgniteInternalFuture<R>> lsnr)
        throws IgniteCheckedException {
        Checkpointer cp = checkpointer;

        if (cp == null)
            return;

        cp.wakeupForCheckpoint(0, reason, lsnr).futureFor(FINISHED).get();
    }

    /** {@inheritDoc} */
    @Override public CheckpointProgress forceCheckpoint(String reason) {
        Checkpointer cp = checkpointer;

        if (cp == null)
            return null;

        return cp.wakeupForCheckpoint(0, reason);
    }

    /** {@inheritDoc} */
    @Override public WALPointer lastCheckpointMarkWalPointer() {
        CheckpointEntry lastCheckpointEntry = cpHistory == null ? null : cpHistory.lastCheckpoint();

        return lastCheckpointEntry == null ? null : lastCheckpointEntry.checkpointMark();
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
            log.warning("Read checkpoint status: checkpoint directory is not found.");

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

        ByteBuffer buf = ByteBuffer.allocate(FileWALPointer.POINTER_SIZE);
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
            io.readFully(buf);

            buf.flip();

            return new FileWALPointer(buf.getLong(), buf.getInt(), buf.getInt());
        }
        catch (IOException e) {
            throw new IgniteCheckedException(
                "Failed to read checkpoint pointer from marker file: " + cpMarkerFile.getAbsolutePath(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void startMemoryRestore(GridKernalContext kctx, TimeBag startTimer) throws IgniteCheckedException {
        if (kctx.clientNode())
            return;

        checkpointReadLock();

        try {
            // Preform early regions startup before restoring state.
            initAndStartRegions(kctx.config().getDataStorageConfiguration());

            startTimer.finishGlobalStage("Init and start regions");

            // Restore binary memory for all not WAL disabled cache groups.
            restoreBinaryMemory(
                groupsWithEnabledWal(),
                physicalRecords()
            );

            if (recoveryVerboseLogging && log.isInfoEnabled()) {
                log.info("Partition states information after BINARY RECOVERY phase:");

                dumpPartitionsInfo(cctx, log);
            }

            startTimer.finishGlobalStage("Restore binary memory");

            CheckpointStatus status = readCheckpointStatus();

            RestoreLogicalState logicalState = applyLogicalUpdates(
                status,
                groupsWithEnabledWal(),
                logicalRecords(),
                true
            );

            if (recoveryVerboseLogging && log.isInfoEnabled()) {
                log.info("Partition states information after LOGICAL RECOVERY phase:");

                dumpPartitionsInfo(cctx, log);
            }

            startTimer.finishGlobalStage("Restore logical state");

            walTail = tailPointer(logicalState);

            cctx.wal().onDeActivate(kctx);
        }
        catch (IgniteCheckedException e) {
            releaseFileLock();

            throw e;
        }
        finally {
            checkpointReadUnlock();
        }
    }

    /**
     * @param f Consumer.
     * @return Accumulated result for all page stores.
     */
    public long forAllPageStores(ToLongFunction<PageStore> f) {
        long res = 0;

        for (CacheGroupContext gctx : cctx.cache().cacheGroups())
            res += forGroupPageStores(gctx, f);

        return res;
    }

    /**
     * @param grpId Cache group id.
     * @param partId Partition ID.
     * @return Page store.
     * @throws IgniteCheckedException If failed.
     */
    public PageStore getPageStore(int grpId, int partId) throws IgniteCheckedException {
        return storeMgr.getStore(grpId, partId);
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

    /**
     * Calculates tail pointer for WAL at the end of logical recovery.
     *
     * @param logicalState State after logical recovery.
     * @return Tail pointer.
     * @throws IgniteCheckedException If failed.
     */
    private WALPointer tailPointer(RestoreLogicalState logicalState) throws IgniteCheckedException {
        // Should flush all data in buffers before read last WAL pointer.
        // Iterator read records only from files.
        WALPointer lastFlushPtr = cctx.wal().flush(null, true);

        // We must return null for NULL_PTR record, because FileWriteAheadLogManager.resumeLogging
        // can't write header without that condition.
        WALPointer lastReadPtr = logicalState.lastReadRecordPointer();

        if (lastFlushPtr != null && lastReadPtr == null)
            return lastFlushPtr;

        if (lastFlushPtr == null && lastReadPtr != null)
            return lastReadPtr;

        if (lastFlushPtr != null && lastReadPtr != null) {
            FileWALPointer lastFlushPtr0 = (FileWALPointer)lastFlushPtr;
            FileWALPointer lastReadPtr0 = (FileWALPointer)lastReadPtr;

            return lastReadPtr0.compareTo(lastFlushPtr0) >= 0 ? lastReadPtr : lastFlushPtr0;
        }

        return null;
    }

    /**
     * Called when all partitions have been fully restored and pre-created on node start.
     *
     * Starts checkpointing process and initiates first checkpoint.
     *
     * @throws IgniteCheckedException If first checkpoint has failed.
     */
    @Override public void onStateRestored(AffinityTopologyVersion topVer) throws IgniteCheckedException {
        IgniteThread cpThread = new IgniteThread(cctx.igniteInstanceName(), "db-checkpoint-thread", checkpointer);

        cpThread.start();

        checkpointerThread = cpThread;

        CheckpointProgress chp = checkpointer.wakeupForCheckpoint(0, "node started");

        if (chp != null)
            chp.futureFor(LOCK_RELEASED).get();
    }

    /**
     * @param status Checkpoint status.
     * @param cacheGroupsPredicate Cache groups to restore.
     * @throws IgniteCheckedException If failed.
     * @throws StorageException In case I/O error occurred during operations with storage.
     */
    private RestoreBinaryState performBinaryMemoryRestore(
        CheckpointStatus status,
        IgnitePredicate<Integer> cacheGroupsPredicate,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordTypePredicate,
        boolean finalizeState
    ) throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Checking memory state [lastValidPos=" + status.endPtr + ", lastMarked="
                + status.startPtr + ", lastCheckpointId=" + status.cpStartId + ']');

        WALPointer recPtr = status.endPtr;

        boolean apply = status.needRestoreMemory();

        try {
            WALRecord startRec = !CheckpointStatus.NULL_PTR.equals(status.startPtr) || apply ? cctx.wal().read(status.startPtr) : null;

            if (apply) {
                if (finalizeState)
                    U.quietAndWarn(log, "Ignite node stopped in the middle of checkpoint. Will restore memory state and " +
                        "finish checkpoint on node start.");

            cctx.cache().cacheGroupDescriptors().forEach((grpId, desc) -> {
                if (!cacheGroupsPredicate.apply(grpId))
                    return;

                try {
                    DataRegion region = cctx.database().dataRegion(desc.config().getDataRegionName());

                    if (region == null || !cctx.isLazyMemoryAllocation(region))
                        return;

                    region.pageMemory().start();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            });

            cctx.pageStore().beginRecover();

                if (!(startRec instanceof CheckpointRecord))
                    throw new StorageException("Checkpoint marker doesn't point to checkpoint record " +
                        "[ptr=" + status.startPtr + ", rec=" + startRec + "]");

                WALPointer cpMark = ((CheckpointRecord)startRec).checkpointMark();

                if (cpMark != null) {
                    log.info("Restoring checkpoint after logical recovery, will start physical recovery from " +
                        "back pointer: " + cpMark);

                    recPtr = cpMark;
                }
            }
            else
                cctx.wal().notchLastCheckpointPtr(status.startPtr);
        }
        catch (NoSuchElementException e) {
            throw new StorageException("Failed to read checkpoint record from WAL, persistence consistency " +
                "cannot be guaranteed. Make sure configuration points to correct WAL folders and WAL folder is " +
                "properly mounted [ptr=" + status.startPtr + ", walPath=" + persistenceCfg.getWalPath() +
                ", walArchive=" + persistenceCfg.getWalArchivePath() + "]");
        }

        AtomicReference<IgniteCheckedException> applyError = new AtomicReference<>();

        StripedExecutor exec = cctx.kernalContext().getStripedExecutorService();

        Semaphore semaphore = new Semaphore(semaphorePertmits(exec));

        long start = U.currentTimeMillis();

        long lastArchivedSegment = cctx.wal().lastArchivedSegment();

        WALIterator it = cctx.wal().replay(recPtr, recordTypePredicate);

        RestoreBinaryState restoreBinaryState = new RestoreBinaryState(status, it, lastArchivedSegment, cacheGroupsPredicate);

        AtomicLong applied = new AtomicLong();

        try {
            while (it.hasNextX()) {
                if (applyError.get() != null)
                    break;

                WALRecord rec = restoreBinaryState.next();

                if (rec == null)
                    break;

                switch (rec.type()) {
                    case PAGE_RECORD:
                        if (restoreBinaryState.needApplyBinaryUpdate()) {
                            PageSnapshot pageSnapshot = (PageSnapshot)rec;

                            // Here we do not require tag check because we may be applying memory changes after
                            // several repetitive restarts and the same pages may have changed several times.
                            int groupId = pageSnapshot.fullPageId().groupId();
                            int partId = partId(pageSnapshot.fullPageId().pageId());

                            if (skipRemovedIndexUpdates(groupId, partId))
                                break;

                            stripedApplyPage((pageMem) -> {
                                    try {
                                        applyPageSnapshot(pageMem, pageSnapshot);

                                        applied.incrementAndGet();
                                    }
                                    catch (Throwable t) {
                                        U.error(log, "Failed to apply page snapshot. rec=[" + pageSnapshot + ']');

                                        applyError.compareAndSet(
                                            null,
                                            (t instanceof IgniteCheckedException)?
                                                (IgniteCheckedException)t:
                                                new IgniteCheckedException("Failed to apply page snapshot", t));
                                    }
                                }, groupId, partId, exec, semaphore
                            );
                        }

                        break;

                    case PART_META_UPDATE_STATE:
                        PartitionMetaStateRecord metaStateRecord = (PartitionMetaStateRecord)rec;

                    {
                        int groupId = metaStateRecord.groupId();
                        int partId = metaStateRecord.partitionId();

                        stripedApplyPage((pageMem) -> {
                            GridDhtPartitionState state = fromOrdinal(metaStateRecord.state());

                            if (state == null || state == GridDhtPartitionState.EVICTED)
                                schedulePartitionDestroy(groupId, partId);
                            else {
                                try {
                                    cancelOrWaitPartitionDestroy(groupId, partId);
                                }
                                catch (Throwable t) {
                                    U.error(log, "Failed to cancel or wait partition destroy. rec=[" + metaStateRecord + ']');

                                    applyError.compareAndSet(
                                        null,
                                        (t instanceof IgniteCheckedException) ?
                                            (IgniteCheckedException)t :
                                            new IgniteCheckedException("Failed to cancel or wait partition destroy", t));
                                }
                            }
                        }, groupId, partId, exec, semaphore);
                    }

                    break;

                    case PARTITION_DESTROY:
                        PartitionDestroyRecord destroyRecord = (PartitionDestroyRecord)rec;

                    {
                        int groupId = destroyRecord.groupId();
                        int partId = destroyRecord.partitionId();

                        stripedApplyPage((pageMem) -> {
                            pageMem.invalidate(groupId, partId);

                            schedulePartitionDestroy(groupId, partId);
                        }, groupId, partId, exec, semaphore);

                    }
                    break;

                    default:
                        if (restoreBinaryState.needApplyBinaryUpdate() && rec instanceof PageDeltaRecord) {
                            PageDeltaRecord pageDelta = (PageDeltaRecord)rec;

                            int groupId = pageDelta.groupId();
                            int partId = partId(pageDelta.pageId());

                            if (skipRemovedIndexUpdates(groupId, partId))
                                break;

                            stripedApplyPage((pageMem) -> {
                                try {
                                    applyPageDelta(pageMem, pageDelta, true);

                                    applied.incrementAndGet();
                                }
                                catch (Throwable t) {
                                    U.error(log, "Failed to apply page delta. rec=[" + pageDelta + ']');

                                    applyError.compareAndSet(
                                        null,
                                        (t instanceof IgniteCheckedException) ?
                                            (IgniteCheckedException)t :
                                            new IgniteCheckedException("Failed to apply page delta", t));
                                }
                            }, groupId, partId, exec, semaphore);
                        }
                }
            }
        }
        finally {
            it.close();

            awaitApplyComplete(exec, applyError);
        }

        if (!finalizeState)
            return null;

        FileWALPointer lastReadPtr = restoreBinaryState.lastReadRecordPointer();

        if (status.needRestoreMemory()) {
            if (restoreBinaryState.needApplyBinaryUpdate())
                throw new StorageException("Failed to restore memory state (checkpoint marker is present " +
                    "on disk, but checkpoint record is missed in WAL) " +
                    "[cpStatus=" + status + ", lastRead=" + lastReadPtr + "]");

            log.info("Finished applying memory changes [changesApplied=" + applied +
                ", time=" + (U.currentTimeMillis() - start) + " ms]");

            finalizeCheckpointOnRecovery(status.cpStartTs, status.cpStartId, status.startPtr, exec);
        }

        cpHistory.initialize(retreiveHistory());

        return restoreBinaryState;
    }

    /**
     * Calculate the maximum number of concurrent tasks for apply through the striped executor.
     *
     * @param exec Striped executor.
     * @return Number of permits.
     */
    private int semaphorePertmits(StripedExecutor exec) {
        // 4 task per-stripe by default.
        int permits = exec.stripesCount() * 4;

        long maxMemory = Runtime.getRuntime().maxMemory();

        // Heuristic calculation part of heap size as a maximum number of concurrent tasks.
        int permits0 = (int)((maxMemory * 0.2) / (4096 * 2));

        // May be for small heap. Get a low number of permits.
        if (permits0 < permits)
            permits = permits0;

        // Property for override any calculation.
        return getInteger(IGNITE_RECOVERY_SEMAPHORE_PERMITS, permits);
    }

    /**
     * @param exec Striped executor.
     * @param applyError Check error reference.
     */
    private void awaitApplyComplete(
        StripedExecutor exec,
        AtomicReference<IgniteCheckedException> applyError
    ) throws IgniteCheckedException {
        try {
            // Await completion apply tasks in all stripes.
            exec.awaitComplete();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }

        // Checking error after all task applied.
        if (applyError.get() != null)
            throw applyError.get();
    }

    /**
     * @param consumer Runnable task.
     * @param grpId Group Id.
     * @param partId Partition Id.
     * @param exec Striped executor.
     */
    public void stripedApplyPage(
        Consumer<PageMemoryEx> consumer,
        int grpId,
        int partId,
        StripedExecutor exec,
        Semaphore semaphore
    ) throws IgniteCheckedException {
        assert consumer != null;
        assert exec != null;
        assert semaphore != null;

        PageMemoryEx pageMem = getPageMemoryForCacheGroup(grpId);

        if (pageMem == null)
            return;

        stripedApply(() -> consumer.accept(pageMem), grpId, partId, exec, semaphore);
    }

    /**
     * @param run Runnable task.
     * @param grpId Group Id.
     * @param partId Partition Id.
     * @param exec Striped executor.
     */
    public void stripedApply(
        Runnable run,
        int grpId,
        int partId,
        StripedExecutor exec,
        Semaphore semaphore
    ) {
        assert run != null;
        assert exec != null;
        assert semaphore != null;

        int stripes = exec.stripesCount();

        int stripe = U.stripeIdx(stripes, grpId, partId);

        assert stripe >= 0 && stripe <= stripes : "idx=" + stripe + ", stripes=" + stripes;

        try {
            semaphore.acquire();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }

        exec.execute(stripe, () -> {
            // WA for avoid assert check in PageMemory, that current thread hold chpLock.
            CHECKPOINT_LOCK_HOLD_COUNT.set(1);

            try {
                run.run();
            }
            finally {
                CHECKPOINT_LOCK_HOLD_COUNT.set(0);

                semaphore.release();
            }
        });
    }

    /**
     * @param pageMem Page memory.
     * @param pageSnapshotRecord Page snapshot record.
     * @throws IgniteCheckedException If failed.
     */
    public void applyPageSnapshot(PageMemoryEx pageMem, PageSnapshot pageSnapshotRecord) throws IgniteCheckedException {
        int grpId = pageSnapshotRecord.fullPageId().groupId();
        long pageId = pageSnapshotRecord.fullPageId().pageId();

        long page = pageMem.acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, true);

        try {
            long pageAddr = pageMem.writeLock(grpId, pageId, page, true);

            try {
                PageUtils.putBytes(pageAddr, 0, pageSnapshotRecord.pageData());

                if (PageIO.getCompressionType(pageAddr) != CompressionProcessor.UNCOMPRESSED_PAGE) {
                    int realPageSize = pageMem.realPageSize(pageSnapshotRecord.groupId());

                    assert pageSnapshotRecord.pageDataSize() < realPageSize : pageSnapshotRecord.pageDataSize();

                    cctx.kernalContext().compress().decompressPage(pageMem.pageBuffer(pageAddr), realPageSize);
                }
            }
            finally {
                pageMem.writeUnlock(grpId, pageId, page, null, true, true);
            }
        }
        finally {
            pageMem.releasePage(grpId, pageId, page);
        }
    }

    /**
     * @param pageMem Page memory.
     * @param pageDeltaRecord Page delta record.
     * @param restore Get page for restore.
     * @throws IgniteCheckedException If failed.
     */
    private void applyPageDelta(PageMemoryEx pageMem, PageDeltaRecord pageDeltaRecord, boolean restore) throws IgniteCheckedException {
        int grpId = pageDeltaRecord.groupId();
        long pageId = pageDeltaRecord.pageId();

        // Here we do not require tag check because we may be applying memory changes after
        // several repetitive restarts and the same pages may have changed several times.
        long page = pageMem.acquirePage(grpId, pageId, IoStatisticsHolderNoOp.INSTANCE, restore);

        try {
            long pageAddr = pageMem.writeLock(grpId, pageId, page, restore);

            try {
                pageDeltaRecord.applyDelta(pageMem, pageAddr);
            }
            finally {
                pageMem.writeUnlock(grpId, pageId, page, null, true, restore);
            }
        }
        finally {
            pageMem.releasePage(grpId, pageId, page);
        }
    }

    /**
     * @param grpId Group id.
     * @param partId Partition id.
     */
    private boolean skipRemovedIndexUpdates(int grpId, int partId) {
        return (partId == PageIdAllocator.INDEX_PARTITION) && !storeMgr.hasIndexStore(grpId);
    }

    /**
     * Obtains PageMemory reference from cache descriptor instead of cache context.
     *
     * @param grpId Cache group id.
     * @return PageMemoryEx instance.
     * @throws IgniteCheckedException if no DataRegion is configured for a name obtained from cache descriptor.
     */
    private PageMemoryEx getPageMemoryForCacheGroup(int grpId) throws IgniteCheckedException {
        if (grpId == MetaStorage.METASTORAGE_CACHE_ID)
            return (PageMemoryEx)dataRegion(METASTORE_DATA_REGION_NAME).pageMemory();

        // TODO IGNITE-7792 add generic mapping.
        if (grpId == TxLog.TX_LOG_CACHE_ID)
            return (PageMemoryEx)dataRegion(TxLog.TX_LOG_CACHE_NAME).pageMemory();

        // TODO IGNITE-5075: cache descriptor can be removed.
        GridCacheSharedContext sharedCtx = context();

        CacheGroupDescriptor desc = sharedCtx.cache().cacheGroupDescriptors().get(grpId);

        if (desc == null)
            return null;

        String memPlcName = desc.config().getDataRegionName();

        return (PageMemoryEx)sharedCtx.database().dataRegion(memPlcName).pageMemory();
    }

    /**
     * Apply update from some iterator and with specific filters.
     *
     * @param it WalIterator.
     * @param recPredicate Wal record filter.
     * @param entryPredicate Entry filter.
     */
    public void applyUpdatesOnRecovery(
        @Nullable WALIterator it,
        IgniteBiPredicate<WALPointer, WALRecord> recPredicate,
        IgnitePredicate<DataEntry> entryPredicate
    ) throws IgniteCheckedException {
        if (it == null)
            return;

        cctx.walState().runWithOutWAL(() -> {
            while (it.hasNext()) {
                IgniteBiTuple<WALPointer, WALRecord> next = it.next();

                WALRecord rec = next.get2();

                if (!recPredicate.apply(next.get1(), rec))
                    break;

                switch (rec.type()) {
                    case MVCC_DATA_RECORD:
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
                                            log.warning("Cache is not started. Updates cannot be applied " +
                                                "[cacheId=" + cacheId + ']');
                                    }
                                    finally {
                                        checkpointReadUnlock();
                                    }
                                }
                            }
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                        finally {
                            checkpointReadUnlock();
                        }

                        break;

                    case MVCC_TX_RECORD:
                        checkpointReadLock();

                        try {
                            MvccTxRecord txRecord = (MvccTxRecord)rec;

                            byte txState = convertToTxState(txRecord.state());

                            cctx.coordinators().updateState(txRecord.mvccVersion(), txState, true);
                        }
                        finally {
                            checkpointReadUnlock();
                        }

                        break;

                    default:
                        // Skip other records.
                }
            }
        });
    }

    /**
     * @param status Last registered checkpoint status.
     * @throws IgniteCheckedException If failed to apply updates.
     * @throws StorageException If IO exception occurred while reading write-ahead log.
     */
    private RestoreLogicalState applyLogicalUpdates(
        CheckpointStatus status,
        IgnitePredicate<Integer> cacheGroupsPredicate,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordTypePredicate,
        boolean skipFieldLookup
    ) throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Applying lost cache updates since last checkpoint record [lastMarked="
                + status.startPtr + ", lastCheckpointId=" + status.cpStartId + ']');

        if (skipFieldLookup)
            cctx.kernalContext().query().skipFieldLookup(true);

        long start = U.currentTimeMillis();

        AtomicReference<IgniteCheckedException> applyError = new AtomicReference<>();

        AtomicLong applied = new AtomicLong();

        long lastArchivedSegment = cctx.wal().lastArchivedSegment();

        StripedExecutor exec = cctx.kernalContext().getStripedExecutorService();

        Semaphore semaphore = new Semaphore(semaphorePertmits(exec));

        Map<GroupPartitionId, Integer> partitionRecoveryStates = new HashMap<>();

        WALIterator it = cctx.wal().replay(status.startPtr, recordTypePredicate);

        RestoreLogicalState restoreLogicalState =
            new RestoreLogicalState(status, it, lastArchivedSegment, cacheGroupsPredicate, partitionRecoveryStates);

        try {
            while (it.hasNextX()) {
                WALRecord rec = restoreLogicalState.next();

                if (rec == null)
                    break;

                switch (rec.type()) {
                    case CHECKPOINT_RECORD: // Calculate initial partition states
                        CheckpointRecord cpRec = (CheckpointRecord)rec;

                        for (Map.Entry<Integer, CacheState> entry : cpRec.cacheGroupStates().entrySet()) {
                            CacheState cacheState = entry.getValue();

                            for (int i = 0; i < cacheState.size(); i++) {
                                int partId = cacheState.partitionByIndex(i);
                                byte state = cacheState.stateByIndex(i);

                                // Ignore undefined state.
                                if (state != -1) {
                                    partitionRecoveryStates.put(new GroupPartitionId(entry.getKey(), partId),
                                        (int)state);
                                }
                            }
                        }

                        break;

                    case ROLLBACK_TX_RECORD:
                        RollbackRecord rbRec = (RollbackRecord)rec;

                        CacheGroupContext ctx = cctx.cache().cacheGroup(rbRec.groupId());

                        if (ctx != null && !ctx.isLocal()) {
                            ctx.topology().forceCreatePartition(rbRec.partitionId());

                            ctx.offheap().onPartitionInitialCounterUpdated(rbRec.partitionId(), rbRec.start(),
                                rbRec.range());
                        }

                        break;

                    case MVCC_DATA_RECORD:
                    case DATA_RECORD:
                    case ENCRYPTED_DATA_RECORD:
                        DataRecord dataRec = (DataRecord)rec;

                        for (DataEntry dataEntry : dataRec.writeEntries()) {
                            int cacheId = dataEntry.cacheId();

                            DynamicCacheDescriptor cacheDesc = cctx.cache().cacheDescriptor(cacheId);

                            // Can empty in case recovery node on blt changed.
                            if (cacheDesc == null)
                                continue;

                            stripedApply(() -> {
                                GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                                if (skipRemovedIndexUpdates(cacheCtx.groupId(), PageIdAllocator.INDEX_PARTITION))
                                    cctx.kernalContext().query().markAsRebuildNeeded(cacheCtx);

                                try {
                                    applyUpdate(cacheCtx, dataEntry);
                                }
                                catch (IgniteCheckedException e) {
                                    U.error(log, "Failed to apply data entry, dataEntry=" + dataEntry +
                                        ", ptr=" + dataRec.position());

                                    applyError.compareAndSet(null, e);
                                }

                                applied.incrementAndGet();
                            }, cacheDesc.groupId(), dataEntry.partitionId(), exec, semaphore);
                        }

                        break;

                    case MVCC_TX_RECORD:
                        MvccTxRecord txRecord = (MvccTxRecord)rec;

                        byte txState = convertToTxState(txRecord.state());

                        cctx.coordinators().updateState(txRecord.mvccVersion(), txState, true);

                        break;

                    case PART_META_UPDATE_STATE:
                        PartitionMetaStateRecord metaStateRecord = (PartitionMetaStateRecord)rec;

                        GroupPartitionId groupPartitionId = new GroupPartitionId(
                            metaStateRecord.groupId(), metaStateRecord.partitionId()
                        );

                        restoreLogicalState.partitionRecoveryStates.put(groupPartitionId, (int)metaStateRecord.state());

                        break;

                    case METASTORE_DATA_RECORD:
                        MetastoreDataRecord metastoreDataRecord = (MetastoreDataRecord)rec;

                        metaStorage.applyUpdate(metastoreDataRecord.key(), metastoreDataRecord.value());

                        break;

                    case META_PAGE_UPDATE_NEXT_SNAPSHOT_ID:
                    case META_PAGE_UPDATE_LAST_SUCCESSFUL_SNAPSHOT_ID:
                    case META_PAGE_UPDATE_LAST_SUCCESSFUL_FULL_SNAPSHOT_ID:
                    case META_PAGE_UPDATE_LAST_ALLOCATED_INDEX:
                        PageDeltaRecord pageDelta = (PageDeltaRecord)rec;

                        stripedApplyPage((pageMem) -> {
                            try {
                                applyPageDelta(pageMem, pageDelta, false);
                            }
                            catch (IgniteCheckedException e) {
                                U.error(log, "Failed to apply page delta, " + pageDelta);

                                applyError.compareAndSet(null, e);
                            }

                        }, pageDelta.groupId(), partId(pageDelta.pageId()), exec, semaphore);

                        break;

                    case MASTER_KEY_CHANGE_RECORD:
                        cctx.kernalContext().encryption().applyKeys((MasterKeyChangeRecord)rec);

                        break;

                    default:
                        // Skip other records.
                }
            }
        }
        finally {
            it.close();

            if (skipFieldLookup)
                cctx.kernalContext().query().skipFieldLookup(false);
        }

        awaitApplyComplete(exec, applyError);

        if (log.isInfoEnabled())
            log.info("Finished applying WAL changes [updatesApplied=" + applied +
                ", time=" + (U.currentTimeMillis() - start) + " ms]");

        for (DatabaseLifecycleListener lsnr : getDatabaseListeners(cctx.kernalContext()))
            lsnr.afterLogicalUpdatesApplied(this, restoreLogicalState);

        return restoreLogicalState;
    }

    /**
     * Convert {@link TransactionState} to Mvcc {@link TxState}.
     *
     * @param state TransactionState.
     * @return TxState.
     */
    private byte convertToTxState(TransactionState state) {
        switch (state) {
            case PREPARED:
                return TxState.PREPARED;

            case COMMITTED:
                return TxState.COMMITTED;

            case ROLLED_BACK:
                return TxState.ABORTED;

            default:
                throw new IllegalStateException("Unsupported TxState.");
        }
    }

    /**
     * Wal truncate callBack.
     *
     * @param highBound WALPointer.
     */
    public void onWalTruncated(WALPointer highBound) throws IgniteCheckedException {
        List<CheckpointEntry> removedFromHistory = cpHistory.onWalTruncated(highBound);

        for (CheckpointEntry cp : removedFromHistory)
            removeCheckpointFiles(cp);
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

        GridDhtLocalPartition locPart = cacheCtx.isLocal() ? null : cacheCtx.topology().forceCreatePartition(partId);

        switch (dataEntry.op()) {
            case CREATE:
            case UPDATE:
                if (dataEntry instanceof MvccDataEntry) {
                    cacheCtx.offheap().mvccApplyUpdate(
                        cacheCtx,
                        dataEntry.key(),
                        dataEntry.value(),
                        dataEntry.writeVersion(),
                        dataEntry.expireTime(),
                        locPart,
                        ((MvccDataEntry)dataEntry).mvccVer());
                }
                else {
                    cacheCtx.offheap().update(
                        cacheCtx,
                        dataEntry.key(),
                        dataEntry.value(),
                        dataEntry.writeVersion(),
                        dataEntry.expireTime(),
                        locPart,
                        null);
                }

                if (dataEntry.partitionCounter() != 0)
                    cacheCtx.offheap().onPartitionInitialCounterUpdated(partId, dataEntry.partitionCounter() - 1, 1);

                break;

            case DELETE:
                if (dataEntry instanceof MvccDataEntry) {
                    cacheCtx.offheap().mvccApplyUpdate(
                        cacheCtx,
                        dataEntry.key(),
                        null,
                        dataEntry.writeVersion(),
                        0L,
                        locPart,
                        ((MvccDataEntry)dataEntry).mvccVer());
                }
                else
                    cacheCtx.offheap().remove(cacheCtx, dataEntry.key(), partId, locPart);

                if (dataEntry.partitionCounter() != 0)
                    cacheCtx.offheap().onPartitionInitialCounterUpdated(partId, dataEntry.partitionCounter() - 1, 1);

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
    private void finalizeCheckpointOnRecovery(
        long cpTs,
        UUID cpId,
        WALPointer walPtr,
        StripedExecutor exec
    ) throws IgniteCheckedException {
        assert cpTs != 0;

        long start = System.currentTimeMillis();

        Collection<DataRegion> regions = dataRegions();

        Collection<GridMultiCollectionWrapper<FullPageId>> res = new ArrayList(regions.size());

        int pagesNum = 0;

        GridFinishedFuture finishedFuture = new GridFinishedFuture();

        // Collect collection of dirty pages from all regions.
        for (DataRegion memPlc : regions) {
            if (memPlc.config().isPersistenceEnabled()){
                GridMultiCollectionWrapper<FullPageId> nextCpPagesCol =
                    ((PageMemoryEx)memPlc.pageMemory()).beginCheckpoint(finishedFuture);

                pagesNum += nextCpPagesCol.size();

                res.add(nextCpPagesCol);
            }
        }

        // Sort and split all dirty pages set to several stripes.
        GridMultiCollectionWrapper<FullPageId> pages = splitAndSortCpPagesIfNeeded(
            new IgniteBiTuple<>(res, pagesNum), exec.stripesCount());

        // Identity stores set for future fsync.
        Collection<PageStore> updStores = new GridConcurrentHashSet<>();

        AtomicInteger cpPagesCnt = new AtomicInteger();

        // Shared refernce for tracking exception during write pages.
        AtomicReference<IgniteCheckedException> writePagesError = new AtomicReference<>();

        for (int i = 0; i < pages.collectionsSize(); i++) {
            // Calculate stripe index.
            int stripeIdx = i % exec.stripesCount();

            // Inner collection index.
            int innerIdx = i;

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
                ByteBuffer writePageBuf = ByteBuffer.allocateDirect(pageSize());

                writePageBuf.order(ByteOrder.nativeOrder());

                Collection<FullPageId> pages0 = pages.innerCollection(innerIdx);

                FullPageId fullPageId = null;

                try {
                    for (FullPageId fullId : pages0) {
                        // Fail-fast break if some exception occurred.
                        if (writePagesError.get() != null)
                            break;

                        // Save pageId to local variable for future using if exception occurred.
                        fullPageId = fullId;

                        PageMemoryEx pageMem = getPageMemoryForCacheGroup(fullId.groupId());

                        // Write page content to page store via pageStoreWriter.
                        // Tracker is null, because no need to track checkpoint metrics on recovery.
                        pageMem.checkpointWritePage(fullId, writePageBuf, pageStoreWriter, null);
                    }

                    // Add number of handled pages.
                    cpPagesCnt.addAndGet(pages0.size());
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to write page to pageStore, pageId=" + fullPageId);

                    writePagesError.compareAndSet(null, e);
                }
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

        ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(pageSize());

        tmpWriteBuf.order(ByteOrder.nativeOrder());

        CheckpointEntry cp = prepareCheckpointEntry(
            tmpWriteBuf,
            cpTs,
            cpId,
            walPtr,
            null,
            CheckpointEntryType.END);

        writeCheckpointEntry(tmpWriteBuf, cp, CheckpointEntryType.END);

        cctx.pageStore().finishRecover();

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
     * Prepares checkpoint entry containing WAL pointer to checkpoint record.
     * Writes into given {@code ptrBuf} WAL pointer content.
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
     * Writes checkpoint entry buffer {@code entryBuf} to specified checkpoint file with 2-phase protocol.
     *
     * @param entryBuf Checkpoint entry buffer to write.
     * @param cp Checkpoint entry.
     * @param type Checkpoint entry type.
     * @throws StorageException If failed to write checkpoint entry.
     */
    public void writeCheckpointEntry(ByteBuffer entryBuf, CheckpointEntry cp, CheckpointEntryType type) throws StorageException {
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
     * @param cp Checkpoint entry.
     * @param type Checkpoint type.
     * @return Checkpoint file name.
     */
    public static String checkpointFileName(CheckpointEntry cp, CheckpointEntryType type) {
        return checkpointFileName(cp.timestamp(), cp.checkpointId(), type);
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
     * @return Checkpoint history.
     */
    @Nullable public CheckpointHistory checkpointHistory() {
        return cpHistory;
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
     * Timeout for checkpoint read lock acquisition.
     *
     * @return Timeout for checkpoint read lock acquisition in milliseconds.
     */
    @Override public long checkpointReadLockTimeout() {
        return checkpointReadLockTimeout;
    }

    /**
     * Sets timeout for checkpoint read lock acquisition.
     *
     * @param val New timeout in milliseconds, non-positive value denotes infinite timeout.
     */
    @Override public void checkpointReadLockTimeout(long val) {
        checkpointReadLockTimeout = val;
    }

    /**
     * @return Holder for page list cache limit for given data region.
     */
    public AtomicLong pageListCacheLimitHolder(DataRegion dataRegion) {
        if (dataRegion.config().isPersistenceEnabled()) {
            return pageListCacheLimits.computeIfAbsent(dataRegion.config().getName(), name -> new AtomicLong(
                (long)(((PageMemoryEx)dataRegion.pageMemory()).totalPages() * PAGE_LIST_CACHE_LIMIT_THRESHOLD)));
        }

        return null;
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

        /** Temporary write buffer. */
        private final ByteBuffer tmpWriteBuf;

        /** Next scheduled checkpoint progress. */
        private volatile CheckpointProgressImpl scheduledCp;

        /** Current checkpoint. This field is updated only by checkpoint thread. */
        @Nullable private volatile CheckpointProgressImpl curCpProgress;

        /** Shutdown now. */
        private volatile boolean shutdownNow;

        /** */
        private long lastCpTs;

        /** Pause detector. */
        private final LongJVMPauseDetector pauseDetector;

        /** Long JVM pause threshold. */
        private final int longJvmPauseThreshold =
            getInteger(IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD, DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD);

        /**
         * @param gridName Grid name.
         * @param name Thread name.
         * @param log Logger.
         */
        protected Checkpointer(@Nullable String gridName, String name, IgniteLogger log) {
            super(gridName, name, log, cctx.kernalContext().workersRegistry());

            scheduledCp = new CheckpointProgressImpl(checkpointFreq);

            tmpWriteBuf = ByteBuffer.allocateDirect(pageSize());

            tmpWriteBuf.order(ByteOrder.nativeOrder());

            pauseDetector = cctx.kernalContext().longJvmPauseDetector();
        }

        /**
         * @return Progress of current chekpoint or {@code null}, if isn't checkpoint at this moment.
         */
        public @Nullable CheckpointProgress currentProgress(){
            return curCpProgress;
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

                    GridFutureAdapter<Void> enableChangeApplied = GridCacheDatabaseSharedManager.this.enableChangeApplied;

                    if (enableChangeApplied != null) {
                        enableChangeApplied.onDone();

                        GridCacheDatabaseSharedManager.this.enableChangeApplied = null;
                    }

                    if (checkpointsEnabled)
                        doCheckpoint();
                    else {
                        synchronized (this) {
                            scheduledCp.nextCpNanos = System.nanoTime() + U.millisToNanos(checkpointFreq);
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
                if (err == null && !(stopping && isCancelled))
                    err = new IllegalStateException("Thread is terminated unexpectedly: " + name());

                if (err instanceof OutOfMemoryError)
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));

                scheduledCp.fail(new NodeStoppingException("Node is stopping."));
            }
        }

        /**
         *
         */
        private CheckpointProgress wakeupForCheckpoint(long delayFromNow, String reason) {
            return wakeupForCheckpoint(delayFromNow, reason, null);
        }

        /**
         *
         */
        private <R> CheckpointProgress wakeupForCheckpoint(
            long delayFromNow,
            String reason,
            IgniteInClosure<? super IgniteInternalFuture<R>> lsnr
        ) {
            if (lsnr != null) {
                //To be sure lsnr always will be executed in checkpoint thread.
                synchronized (this) {
                    CheckpointProgressImpl sched = scheduledCp;

                    sched.futureFor(FINISHED).listen(lsnr);
                }
            }

            CheckpointProgressImpl sched = scheduledCp;

            long nextNanos = System.nanoTime() + U.millisToNanos(delayFromNow);

            if (sched.nextCpNanos - nextNanos <= 0)
                return sched;

            synchronized (this) {
                sched = scheduledCp;

                if (sched.nextCpNanos - nextNanos > 0) {
                    sched.reason = reason;

                    sched.nextCpNanos = nextNanos;
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
                scheduledCp.nextCpNanos = System.nanoTime();

                scheduledCp.reason = "snapshot";

                scheduledCp.nextSnapshot = true;

                scheduledCp.snapshotOperation = snapshotOperation;

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
                    cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

                    throw new IgniteException(e); // Re-throw as unchecked exception to force stopping checkpoint thread.
                }

                updateHeartbeat();

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
                                    totalPagesToWriteCnt,
                                    new Runnable() {
                                        @Override public void run() {
                                            updateHeartbeat();
                                        }
                                    },
                                    asyncRunner
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
                                new Runnable() {
                                    @Override public void run() {
                                        updateHeartbeat();
                                    }
                                },
                                null);

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

                                syncedPagesCntr.addAndGet(updStoreEntry.getValue().intValue());
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

                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
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

            CheckpointProgressImpl cur;

            synchronized (this) {
                cur = curCpProgress;

                if (cur != null)
                    req = cur.destroyQueue.cancelDestroy(grpId, partId);
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
                    long remaining = U.nanosToMillis(scheduledCp.nextCpNanos - System.nanoTime());

                    while (remaining > 0 && !isCancelled()) {
                        blockingSectionBegin();

                        try {
                            wait(remaining);

                            remaining = U.nanosToMillis(scheduledCp.nextCpNanos - System.nanoTime());
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
         *
         */
        @SuppressWarnings("TooBroadScope")
        private Checkpoint markCheckpointBegin(CheckpointMetricsTracker tracker) throws IgniteCheckedException {
            long cpTs = updateLastCheckpointTime();

            CheckpointProgressImpl curr = scheduledCp;

            CheckpointRecord cpRec = new CheckpointRecord(memoryRecoveryRecordPtr);

            memoryRecoveryRecordPtr = null;

            CheckpointEntry cp = null;

            IgniteFuture snapFut = null;

            IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> cpPagesTuple;

            boolean hasPages, hasPartitionsToDestroy;

            DbCheckpointContextImpl ctx0 = new DbCheckpointContextImpl(curr, new PartitionAllocationMap());

            internalReadLock();

            try {
                for (DbCheckpointListener lsnr : lsnrs)
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
                for (DbCheckpointListener lsnr : lsnrs)
                    lsnr.onMarkCheckpointBegin(ctx0);

                ctx0.awaitPendingTasksFinished();

                tracker.onListenersExecuteEnd();

                if (curr.nextSnapshot)
                    snapFut = snapshotMgr.onMarkCheckPointBegin(curr.snapshotOperation, ctx0.partitionStatMap());

                fillCacheGroupState(cpRec);

                //There are allowable to replace pages only after checkpoint entry was stored to disk.
                cpPagesTuple = beginAllCheckpoints(curr.futureFor(MARKER_STORED_TO_DISK));

                hasPages = hasPageForWrite(cpPagesTuple.get1());

                hasPartitionsToDestroy = !curr.destroyQueue.pendingReqs.isEmpty();

                WALPointer cpPtr = null;

                if (hasPages || curr.nextSnapshot || hasPartitionsToDestroy) {
                    // No page updates for this checkpoint are allowed from now on.
                    cpPtr = cctx.wal().log(cpRec);

                    if (cpPtr == null)
                        cpPtr = CheckpointStatus.NULL_PTR;
                }

                if (hasPages || hasPartitionsToDestroy) {
                    cp = prepareCheckpointEntry(
                        tmpWriteBuf,
                        cpTs,
                        cpRec.checkpointId(),
                        cpPtr,
                        cpRec,
                        CheckpointEntryType.START);

                    cpHistory.addCheckpoint(cp);
                }
            }
            finally {
                checkpointLock.writeLock().unlock();

                tracker.onLockRelease();
            }

            DbCheckpointListener.Context ctx = createOnCheckpointBeginContext(ctx0, hasPages);

            curr.transitTo(LOCK_RELEASED);

            for (DbCheckpointListener lsnr : lsnrs)
                lsnr.onCheckpointBegin(ctx);

            if (snapFut != null) {
                try {
                    snapFut.get();
                }
                catch (IgniteException e) {
                    U.error(log, "Failed to wait for snapshot operation initialization: " +
                        curr.snapshotOperation, e);
                }
            }

            if (hasPages || hasPartitionsToDestroy) {
                assert cp != null;
                assert cp.checkpointMark() != null;

                tracker.onWalCpRecordFsyncStart();

                // Sync log outside the checkpoint write lock.
                cctx.wal().flush(cp.checkpointMark(), true);

                tracker.onWalCpRecordFsyncEnd();

                writeCheckpointEntry(tmpWriteBuf, cp, CheckpointEntryType.START);

                curr.transitTo(MARKER_STORED_TO_DISK);

                tracker.onSplitAndSortCpPagesStart();

                GridMultiCollectionWrapper<FullPageId> cpPages = splitAndSortCpPagesIfNeeded(
                    cpPagesTuple, persistenceCfg.getCheckpointThreads());

                tracker.onSplitAndSortCpPagesEnd();

                if (printCheckpointStats && log.isInfoEnabled()) {
                    long possibleJvmPauseDur = possibleLongJvmPauseDuration(tracker);

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
                            cpPages.size(),
                            curr.reason
                        )
                    );
                }

                return new Checkpoint(cp, cpPages, curr);
            }
            else {
                if (curr.nextSnapshot)
                    cctx.wal().flush(null, true);

                if (printCheckpointStats) {
                    if (log.isInfoEnabled())
                        LT.info(log, String.format("Skipping checkpoint (no pages were modified) [" +
                                "checkpointBeforeLockTime=%dms, checkpointLockWait=%dms, " +
                                "checkpointListenersExecuteTime=%dms, checkpointLockHoldTime=%dms, reason='%s']",
                            tracker.beforeLockDuration(),
                            tracker.lockWaitDuration(),
                            tracker.listenersExecuteDuration(),
                            tracker.lockHoldDuration(),
                            curr.reason));
                }

                return new Checkpoint(null, new GridMultiCollectionWrapper<>(new Collection[0]), curr);
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

            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal() || !grp.walEnabled())
                    continue;

                Runnable r = () -> {
                    ArrayList<GridDhtLocalPartition> parts = new ArrayList<>(grp.topology().localPartitions().size());

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

                if (curr.reason == null)
                    curr.reason = "timeout";

                // It is important that we assign a new progress object before checkpoint mark in page memory.
                scheduledCp = new CheckpointProgressImpl(checkpointFreq);

                curCpProgress = curr;
            }
            return curr;
        }

        /** */
        private DbCheckpointListener.Context createOnCheckpointBeginContext(
            DbCheckpointListener.Context delegate,
            boolean hasPages
        ) {
            return new DbCheckpointListener.Context() {
                /** {@inheritDoc} */
                @Override public boolean nextSnapshot() {
                    return delegate.nextSnapshot();
                }

                /** {@inheritDoc} */
                @Override public PartitionAllocationMap partitionStatMap() {
                    return delegate.partitionStatMap();
                }

                /** {@inheritDoc} */
                @Override public boolean needToSnapshot(String cacheOrGrpName) {
                    return delegate.needToSnapshot(cacheOrGrpName);
                }

                /** {@inheritDoc} */
                @Override public @Nullable Executor executor() {
                    return delegate.executor();
                }

                /** {@inheritDoc} */
                @Override public boolean hasPages() {
                    return hasPages;
                }
            };
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
         * @return tuple with collections of FullPageIds obtained from each PageMemory, overall number of dirty
         * pages, and flag defines at least one user page became a dirty since last checkpoint.
         * @param allowToReplace The sign which allows to replace pages from a checkpoint by page replacer.
         */
        private IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> beginAllCheckpoints(
            IgniteInternalFuture allowToReplace
        ) {
            Collection<GridMultiCollectionWrapper<FullPageId>> res = new ArrayList(dataRegions().size());

            int pagesNum = 0;

            for (DataRegion memPlc : dataRegions()) {
                if (!memPlc.config().isPersistenceEnabled())
                    continue;

                GridMultiCollectionWrapper<FullPageId> nextCpPagesCol = ((PageMemoryEx)memPlc.pageMemory())
                    .beginCheckpoint(allowToReplace);

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

                currCheckpointPagesCnt = 0;
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

                cctx.wal().notchLastCheckpointPtr(chp.cpEntry.checkpointMark());
            }

            List<CheckpointEntry> removedFromHistory = cpHistory.onCheckpointFinished(chp, truncateWalOnCpFinish);

            for (CheckpointEntry cp : removedFromHistory)
                removeCheckpointFiles(cp);

            if (chp.progress != null)
                chp.progress.transitTo(FINISHED);
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

        /**
         * Context with information about current snapshots.
         */
        private class DbCheckpointContextImpl implements DbCheckpointListener.Context {
            /** Current checkpoint progress. */
            private final CheckpointProgressImpl curr;

            /** Partition map. */
            private final PartitionAllocationMap map;

            /** Pending tasks from executor. */
            private GridCompoundFuture pendingTaskFuture;

            /**
             * @param curr Current checkpoint progress.
             * @param map Partition map.
             */
            private DbCheckpointContextImpl(CheckpointProgressImpl curr, PartitionAllocationMap map) {
                this.curr = curr;
                this.map = map;
                this.pendingTaskFuture = asyncRunner == null ? null : new GridCompoundFuture();
            }

            /** {@inheritDoc} */
            @Override public boolean nextSnapshot() {
                return curr.nextSnapshot;
            }

            /** {@inheritDoc} */
            @Override public PartitionAllocationMap partitionStatMap() {
                return map;
            }

            /** {@inheritDoc} */
            @Override public boolean needToSnapshot(String cacheOrGrpName) {
                return curr.snapshotOperation.cacheGroupIds().contains(CU.cacheId(cacheOrGrpName));
            }

            /** {@inheritDoc} */
            @Override public Executor executor() {
                return asyncRunner == null ? null : cmd -> {
                    try {
                        GridFutureAdapter<?> res = new GridFutureAdapter<>();

                        res.listen(fut -> updateHeartbeat());

                        asyncRunner.execute(U.wrapIgniteFuture(cmd, res));

                        pendingTaskFuture.add(res);
                    }
                    catch (RejectedExecutionException e) {
                        assert false : "A task should never be rejected by async runner";
                    }
                };
            }

            /** {@inheritDoc} */
            @Override public boolean hasPages() {
                throw new IllegalStateException(
                    "Property is unknown at this moment. You should use onCheckpointBegin() method."
                );
            }

            /**
             * Await all async tasks from executor was finished.
             *
             * @throws IgniteCheckedException if fail.
             */
            public void awaitPendingTasksFinished() throws IgniteCheckedException {
                GridCompoundFuture pendingFut = this.pendingTaskFuture;

                this.pendingTaskFuture = new GridCompoundFuture();

                if (pendingFut != null) {
                    pendingFut.markInitialized();

                    pendingFut.get();
                }
            }
        }
    }

    /**
     * Reorders list of checkpoint pages and splits them into needed number of sublists according to
     * {@link DataStorageConfiguration#getCheckpointThreads()} and
     * {@link DataStorageConfiguration#getCheckpointWriteOrder()}.
     *
     * @param cpPagesTuple Checkpoint pages tuple.
     * @param threads Checkpoint runner threads.
     */
    private GridMultiCollectionWrapper<FullPageId> splitAndSortCpPagesIfNeeded(
        IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> cpPagesTuple,
        int threads
    ) throws IgniteCheckedException {
        FullPageId[] pagesArr = new FullPageId[cpPagesTuple.get2()];

        int realPagesArrSize = 0;

        for (GridMultiCollectionWrapper<FullPageId> colWrapper : cpPagesTuple.get1()) {
            for (int i = 0; i < colWrapper.collectionsSize(); i++)
                for (FullPageId page : colWrapper.innerCollection(i)) {
                    if (realPagesArrSize == pagesArr.length)
                        throw new AssertionError("Incorrect estimated dirty pages number: " + pagesArr.length);

                    pagesArr[realPagesArrSize++] = page;
                }
        }

        FullPageId fakeMaxFullPageId = new FullPageId(Long.MAX_VALUE, Integer.MAX_VALUE);

        // Some pages may have been replaced, need to fill end of array with fake ones to prevent NPE during sort.
        for (int i = realPagesArrSize; i < pagesArr.length; i++)
            pagesArr[i] = fakeMaxFullPageId;

        if (persistenceCfg.getCheckpointWriteOrder() == CheckpointWriteOrder.SEQUENTIAL) {
            Comparator<FullPageId> cmp = new Comparator<FullPageId>() {
                @Override public int compare(FullPageId o1, FullPageId o2) {
                    int cmp = Long.compare(o1.groupId(), o2.groupId());
                    if (cmp != 0)
                        return cmp;

                    return Long.compare(o1.effectivePageId(), o2.effectivePageId());
                }
            };

            if (pagesArr.length >= parallelSortThreshold)
                parallelSortInIsolatedPool(pagesArr, cmp);
            else
                Arrays.sort(pagesArr, cmp);
        }

        int pagesSubLists = threads == 1 ? 1 : threads * 4;
        // Splitting pages to (threads * 4) subtasks. If any thread will be faster, it will help slower threads.

        Collection[] pagesSubListArr = new Collection[pagesSubLists];

        for (int i = 0; i < pagesSubLists; i++) {
            int from = (int)((long)realPagesArrSize * i / pagesSubLists);

            int to = (int)((long)realPagesArrSize * (i + 1) / pagesSubLists);

            pagesSubListArr[i] = new GridReadOnlyArrayView(pagesArr, from, to);
        }

        return new GridMultiCollectionWrapper<FullPageId>(pagesSubListArr);
    }

    /**
     * Performs parallel sort in isolated fork join pool.
     *
     * @param pagesArr Pages array.
     * @param cmp Cmp.
     */
    private static void parallelSortInIsolatedPool(
        FullPageId[] pagesArr,
        Comparator<FullPageId> cmp
    ) throws IgniteCheckedException {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = new ForkJoinPool.ForkJoinWorkerThreadFactory() {
            @Override public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);

                worker.setName("checkpoint-pages-sorter-" + worker.getPoolIndex());

                return worker;
            }
        };

        ForkJoinPool forkJoinPool = new ForkJoinPool(PARALLEL_SORT_THREADS + 1, factory, null, false);

        ForkJoinTask sortTask = forkJoinPool.submit(() -> Arrays.parallelSort(pagesArr, cmp));

        try {
            sortTask.get();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }
        catch (ExecutionException e) {
            throw new IgniteCheckedException("Failed to perform pages array parallel sort", e.getCause());
        }

        forkJoinPool.shutdown();
    }

    /** Pages write task */
    private class WriteCheckpointPages implements Runnable {
        /** */
        private final CheckpointMetricsTracker tracker;

        /** Collection of page IDs to write under this task. Overall pages to write may be greater than this collection */
        private final Collection<FullPageId> writePageIds;

        /** */
        private final ConcurrentLinkedHashMap<PageStore, LongAdder> updStores;

        /** */
        private final CountDownFuture doneFut;

        /** Total pages to write, counter may be greater than {@link #writePageIds} size */
        private final int totalPagesToWrite;

        /** */
        private final Runnable beforePageWrite;

        /** If any pages were skipped, new task with remaining pages will be submitted here. */
        private final ExecutorService retryWriteExecutor;

        /**
         * Creates task for write pages
         *
         * @param tracker
         * @param writePageIds Collection of page IDs to write.
         * @param updStores
         * @param doneFut
         * @param totalPagesToWrite total pages to be written under this checkpoint
         * @param beforePageWrite Action to be performed before every page write.
         * @param retryWriteExecutor Retry write executor.
         */
        private WriteCheckpointPages(
            final CheckpointMetricsTracker tracker,
            final Collection<FullPageId> writePageIds,
            final ConcurrentLinkedHashMap<PageStore, LongAdder> updStores,
            final CountDownFuture doneFut,
            final int totalPagesToWrite,
            final Runnable beforePageWrite,
            final ExecutorService retryWriteExecutor
        ) {
            this.tracker = tracker;
            this.writePageIds = writePageIds;
            this.updStores = updStores;
            this.doneFut = doneFut;
            this.totalPagesToWrite = totalPagesToWrite;
            this.beforePageWrite = beforePageWrite;
            this.retryWriteExecutor = retryWriteExecutor;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            snapshotMgr.beforeCheckpointPageWritten();

            Collection<FullPageId> writePageIds = this.writePageIds;

            try {
                List<FullPageId> pagesToRetry = writePages(writePageIds);

                if (pagesToRetry.isEmpty())
                    doneFut.onDone();
                else {
                    LT.warn(log, pagesToRetry.size() + " checkpoint pages were not written yet due to unsuccessful " +
                        "page write lock acquisition and will be retried");

                    while (!pagesToRetry.isEmpty())
                        pagesToRetry = writePages(pagesToRetry);

                    doneFut.onDone();
                }
            }
            catch (Throwable e) {
                doneFut.onDone(e);
            }
        }

        /**
         * @param writePageIds Collections of pages to write.
         * @return pagesToRetry Pages which should be retried.
         */
        private List<FullPageId> writePages(Collection<FullPageId> writePageIds) throws IgniteCheckedException {
            List<FullPageId> pagesToRetry = new ArrayList<>();

            CheckpointMetricsTracker tracker = persStoreMetrics.metricsEnabled() ? this.tracker : null;

            PageStoreWriter pageStoreWriter = createPageStoreWriter(pagesToRetry);

            ByteBuffer tmpWriteBuf = threadBuf.get();

            boolean throttlingEnabled = resolveThrottlingPolicy() != PageMemoryImpl.ThrottlingPolicy.DISABLED;

            for (FullPageId fullId : writePageIds) {
                if (checkpointer.shutdownNow)
                    break;

                beforePageWrite.run();

                int grpId = fullId.groupId();

                PageMemoryEx pageMem;

                // TODO IGNITE-7792 add generic mapping.
                if (grpId == MetaStorage.METASTORAGE_CACHE_ID)
                    pageMem = (PageMemoryEx)metaStorage.pageMemory();
                else if (grpId == TxLog.TX_LOG_CACHE_ID)
                    pageMem = (PageMemoryEx)dataRegion(TxLog.TX_LOG_CACHE_NAME).pageMemory();
                else {
                    CacheGroupContext grp = context().cache().cacheGroup(grpId);

                    DataRegion region = grp != null ? grp.dataRegion() : null;

                    if (region == null || !region.config().isPersistenceEnabled())
                        continue;

                    pageMem = (PageMemoryEx)region.pageMemory();
                }

                snapshotMgr.beforePageWrite(fullId);

                tmpWriteBuf.rewind();

                pageMem.checkpointWritePage(fullId, tmpWriteBuf, pageStoreWriter, tracker);

                if (throttlingEnabled) {
                    while (pageMem.shouldThrottle()) {
                        FullPageId cpPageId = pageMem.pullPageFromCpBuffer();

                        if (cpPageId.equals(FullPageId.NULL_PAGE))
                            break;

                        snapshotMgr.beforePageWrite(cpPageId);

                        tmpWriteBuf.rewind();

                        pageMem.checkpointWritePage(cpPageId, tmpWriteBuf, pageStoreWriter, tracker);
                    }
                }
            }

            return pagesToRetry;
        }

        /**
         * Factory method for create {@link PageStoreWriter}.
         *
         * @param pagesToRetry List pages for retry.
         * @return Checkpoint page write context.
         */
        private PageStoreWriter createPageStoreWriter(List<FullPageId> pagesToRetry) {
            return new PageStoreWriter() {
                /** {@inheritDoc} */
                @Override public void writePage(FullPageId fullPageId, ByteBuffer buf, int tag) throws IgniteCheckedException {
                    if (tag == PageMemoryImpl.TRY_AGAIN_TAG) {
                        pagesToRetry.add(fullPageId);

                        return;
                    }

                    int groupId = fullPageId.groupId();
                    long pageId = fullPageId.pageId();

                    assert getType(buf) != 0 : "Invalid state. Type is 0! pageId = " + hexLong(pageId);
                    assert getVersion(buf) != 0 : "Invalid state. Version is 0! pageId = " + hexLong(pageId);

                    if (persStoreMetrics.metricsEnabled()) {
                        int pageType = getType(buf);

                        if (PageIO.isDataPageType(pageType))
                            tracker.onDataPageWritten();
                    }

                    writtenPagesCntr.incrementAndGet();

                    PageStore store = storeMgr.writeInternal(groupId, pageId, buf, tag, true);

                    updStores.computeIfAbsent(store, k -> new LongAdder()).increment();
                }
            };
        }
    }

    /**
     *
     */
    public static class Checkpoint {
        /** Checkpoint entry. */
        @Nullable private final CheckpointEntry cpEntry;

        /** Checkpoint pages. */
        private final GridMultiCollectionWrapper<FullPageId> cpPages;

        /** */
        private final CheckpointProgressImpl progress;

        /** Number of deleted WAL files. */
        private int walFilesDeleted;

        /** WAL segments fully covered by this checkpoint. */
        private IgniteBiTuple<Long, Long> walSegsCoveredRange;

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
            CheckpointProgressImpl progress
        ) {
            this.cpEntry = cpEntry;
            this.cpPages = cpPages;
            this.progress = progress;

            pagesSize = cpPages.size();
        }

        /**
         * @return {@code true} if this checkpoint contains at least one dirty page.
         */
        public boolean hasDelta() {
            return pagesSize != 0;
        }

        /**
         * @param walFilesDeleted Wal files deleted.
         */
        public void walFilesDeleted(int walFilesDeleted) {
            this.walFilesDeleted = walFilesDeleted;
        }

        /**
         * @param walSegsCoveredRange WAL segments fully covered by this checkpoint.
         */
        public void walSegsCoveredRange(final IgniteBiTuple<Long, Long> walSegsCoveredRange) {
            this.walSegsCoveredRange = walSegsCoveredRange;
        }
    }

    /**
     *
     */
    public static class CheckpointStatus {
        /** Null checkpoint UUID. */
        private static final UUID NULL_UUID = new UUID(0L, 0L);

        /** Null WAL pointer. */
        public static final WALPointer NULL_PTR = new FileWALPointer(0, 0, 0);

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
         * @return {@code True} if need perform binary memory recovery. Only records {@link PageDeltaRecord}
         * and {@link PageSnapshot} needs to be applyed from {@link #cpStartId}.
         */
        public boolean needRestoreMemory() {
            return !F.eq(cpStartId, cpEndId) && !F.eq(NULL_UUID, cpStartId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CheckpointStatus.class, this);
        }
    }

    /**
     * Data class representing the state of running/scheduled checkpoint.
     */
    public static class CheckpointProgressImpl implements CheckpointProgress {
        /** Scheduled time of checkpoint. */
        private volatile long nextCpNanos;

        /** Current checkpoint state. */
        private volatile AtomicReference<CheckpointState> state = new AtomicReference(CheckpointState.SCHEDULED);

        /** Future which would be finished when corresponds state is set. */
        private final Map<CheckpointState, GridFutureAdapter> stateFutures = new ConcurrentHashMap<>();

        /** Cause of fail, which has happened during the checkpoint or null if checkpoint was successful. */
        private volatile Throwable failCause;

        /** Flag indicates that snapshot operation will be performed after checkpoint. */
        private volatile boolean nextSnapshot;

        /** Snapshot operation that should be performed if {@link #nextSnapshot} set to true. */
        private volatile SnapshotOperation snapshotOperation;

        /** Partitions destroy queue. */
        private final PartitionDestroyQueue destroyQueue = new PartitionDestroyQueue();

        /** Wakeup reason. */
        private String reason;

        /**
         * @param cpFreq Timeout until next checkpoint.
         */
        private CheckpointProgressImpl(long cpFreq) {
            this.nextCpNanos = System.nanoTime() + U.millisToNanos(cpFreq);
        }

        /**
         * @return {@code true} If checkpoint already started but have not finished yet.
         */
        @Override public boolean inProgress() {
            return greaterOrEqualTo(LOCK_RELEASED) && !greaterOrEqualTo(FINISHED);
        }

        /**
         * @param expectedState Expected state.
         * @return {@code true} if current state equal to given state.
         */
        public boolean greaterOrEqualTo(CheckpointState expectedState) {
            return state.get().ordinal() >= expectedState.ordinal();
        }

        /**
         * @param state State for which future should be returned.
         * @return Existed or new future which corresponds to the given state.
         */
        @Override public GridFutureAdapter futureFor(CheckpointState state) {
            GridFutureAdapter stateFut = stateFutures.computeIfAbsent(state, (k) -> new GridFutureAdapter());

            if (greaterOrEqualTo(state) && !stateFut.isDone())
                stateFut.onDone(failCause);

            return stateFut;
        }

        /**
         * Mark this checkpoint execution as failed.
         *
         * @param error Causal error of fail.
         */
        public void fail(Throwable error) {
            failCause = error;

            transitTo(FINISHED);
        }

        /**
         * Changing checkpoint state if order of state is correct.
         *
         * @param newState New checkpoint state.
         */
        public void transitTo(@NotNull CheckpointState newState) {
            CheckpointState state = this.state.get();

            if (state.ordinal() < newState.ordinal()) {
                this.state.compareAndSet(state, newState);

                doFinishFuturesWhichLessOrEqualTo(newState);
            }
        }

        /**
         * Finishing futures with correct result in direct state order until lastState(included).
         *
         * @param lastState State until which futures should be done.
         */
        private void doFinishFuturesWhichLessOrEqualTo(@NotNull CheckpointState lastState) {
            for (CheckpointState old : CheckpointState.values()) {
                GridFutureAdapter fut = stateFutures.get(old);

                if (fut != null && !fut.isDone())
                    fut.onDone(failCause);

                if (old == lastState)
                    return;
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
        private volatile FileLock lock;

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

                        log.warning("Failed to acquire file lock. Will try again in 1s " +
                            "[nodeId=" + ctx.localNodeId() + ", holder=" + content +
                            ", path=" + file.getAbsolutePath() + ']');
                    }

                    U.sleep(1000);
                }

                if (content == null)
                    content = readContent();

                failMsg = "Failed to acquire file lock [holder=" + content + ", time=" + (lockWaitTimeMillis / 1000) +
                    " sec, path=" + file.getAbsolutePath() + ']';
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

        /** Locked or not. */
        public boolean isLocked() {
            return lock != null && lock.isValid();
        }

        /** Releases file lock */
        public void release() {
            U.releaseQuiet(lock);
        }

        /** Closes file channel */
        @Override public void close() {
            release();

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

    /**
     * @return Temporary metastorage to migration of index partition.
     */
    public MetaStorage.TmpStorage temporaryMetaStorage() {
        return tmpMetaStorage;
    }

    /**
     * @param tmpMetaStorage Temporary metastorage to migration of index partition.
     */
    public void temporaryMetaStorage(MetaStorage.TmpStorage tmpMetaStorage) {
        this.tmpMetaStorage = tmpMetaStorage;
    }

    /** {@inheritDoc} */
    @Override public void notifyMetaStorageSubscribersOnReadyForRead() throws IgniteCheckedException {
        metastorageLifecycleLsnrs = cctx.kernalContext().internalSubscriptionProcessor().getMetastorageSubscribers();

        readMetastore();
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
            else {
                metaStorage.write(key, true);

                lastCheckpointInapplicableForWalRebalance(grpId);
            }
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
     * Checks that checkpoint with timestamp {@code cpTs} is inapplicable as start point for WAL rebalance for given group {@code grpId}.
     *
     * @param cpTs Checkpoint timestamp.
     * @param grpId Group ID.
     * @return {@code true} if checkpoint {@code cpTs} is inapplicable as start point for WAL rebalance for {@code grpId}.
     * @throws IgniteCheckedException If failed to check.
     */
    public boolean isCheckpointInapplicableForWalRebalance(Long cpTs, int grpId) throws IgniteCheckedException {
        return metaStorage.read(checkpointInapplicableCpAndGroupIdToKey(cpTs, grpId)) != null;
    }

    /**
     * Set last checkpoint as inapplicable for WAL rebalance for given group {@code grpId}.
     *
     * @param grpId Group ID.
     */
    @Override public void lastCheckpointInapplicableForWalRebalance(int grpId) {
        checkpointReadLock();

        try {
            CheckpointEntry lastCp = cpHistory.lastCheckpoint();
            long lastCpTs = lastCp != null ? lastCp.timestamp() : 0;

            if (lastCpTs != 0)
                metaStorage.write(checkpointInapplicableCpAndGroupIdToKey(lastCpTs, grpId), true);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to mark last checkpoint as inapplicable for WAL rebalance for group: " + grpId, e);
        }
        finally {
            checkpointReadUnlock();
        }
    }

    /**
     *
     */
    private void fillWalDisabledGroups() {
        assert metaStorage != null;

        try {
            metaStorage.iterate(WAL_KEY_PREFIX, (key, val) -> {
                T2<Integer, Boolean> t2 = walKeyToGroupIdAndLocalFlag(key);

                if (t2 != null) {
                    if (t2.get2())
                        initiallyLocalWalDisabledGrps.add(t2.get1());
                    else
                        initiallyGlobalWalDisabledGrps.add(t2.get1());
                }
            }, false);
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
     * Convert checkpoint timestamp and cache group ID to key for {@link #CHECKPOINT_INAPPLICABLE_FOR_REBALANCE} metastorage records.
     *
     * @param cpTs Checkpoint timestamp.
     * @param grpId Group ID.
     * @return Key.
     */
    private static String checkpointInapplicableCpAndGroupIdToKey(long cpTs, int grpId) {
        return CHECKPOINT_INAPPLICABLE_FOR_REBALANCE + cpTs + "-" + grpId;
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
        else if (key.startsWith(WAL_GLOBAL_KEY_PREFIX))
            return new T2<>(Integer.parseInt(key.substring(WAL_GLOBAL_KEY_PREFIX.length())), false);
        else
            return null;
    }

    /**
     * Method dumps partitions info see {@link #dumpPartitionsInfo(CacheGroupContext, IgniteLogger)}
     * for all persistent cache groups.
     *
     * @param cctx Shared context.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    private static void dumpPartitionsInfo(GridCacheSharedContext cctx, IgniteLogger log) throws IgniteCheckedException {
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal() || !grp.persistenceEnabled())
                continue;

            dumpPartitionsInfo(grp, log);
        }
    }

    /**
     * Retrieves from page memory meta information about given {@code grp} group partitions
     * and dumps this information to log INFO level.
     *
     * @param grp Cache group.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    private static void dumpPartitionsInfo(CacheGroupContext grp, IgniteLogger log) throws IgniteCheckedException {
        PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

        IgnitePageStoreManager pageStore = grp.shared().pageStore();

        assert pageStore != null : "Persistent cache should have initialize page store manager.";

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            GridDhtLocalPartition part = grp.topology().localPartition(p);

            if (part != null) {
                log.info("Partition [grp=" + grp.cacheOrGroupName()
                    + ", id=" + p
                    + ", state=" + part.state()
                    + ", counter=" + part.dataStore().partUpdateCounter()
                    + ", size=" + part.fullSize() + "]");

                continue;
            }

            if (!pageStore.exists(grp.groupId(), p))
                continue;

            pageStore.ensure(grp.groupId(), p);

            if (pageStore.pages(grp.groupId(), p) <= 1) {
                log.info("Partition [grp=" + grp.cacheOrGroupName() + ", id=" + p + ", state=N/A (only file header) ]");

                continue;
            }

            long partMetaId = pageMem.partitionMetaPageId(grp.groupId(), p);
            long partMetaPage = pageMem.acquirePage(grp.groupId(), partMetaId);

            try {
                long pageAddr = pageMem.readLock(grp.groupId(), partMetaId, partMetaPage);

                try {
                    PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(pageAddr);

                    GridDhtPartitionState partState = fromOrdinal(io.getPartitionState(pageAddr));

                    String state = partState != null ? partState.toString() : "N/A";

                    long updateCntr = io.getUpdateCounter(pageAddr);
                    long size = io.getSize(pageAddr);

                    log.info("Partition [grp=" + grp.cacheOrGroupName()
                            + ", id=" + p
                            + ", state=" + state
                            + ", counter=" + updateCntr
                            + ", size=" + size + "]");
                }
                finally {
                    pageMem.readUnlock(grp.groupId(), partMetaId, partMetaPage);
                }
            }
            finally {
                pageMem.releasePage(grp.groupId(), partMetaId, partMetaPage);
            }
        }
    }

    /**
     * Recovery lifecycle for read-write metastorage.
     */
    private class MetastorageRecoveryLifecycle implements DatabaseLifecycleListener {
        /** {@inheritDoc} */
        @Override public void beforeBinaryMemoryRestore(IgniteCacheDatabaseSharedManager mgr) throws IgniteCheckedException {
            cctx.pageStore().initializeForMetastorage();
        }

        /** {@inheritDoc} */
        @Override public void afterBinaryMemoryRestore(
            IgniteCacheDatabaseSharedManager mgr,
            RestoreBinaryState restoreState
        ) throws IgniteCheckedException {
            assert metaStorage == null;

            metaStorage = createMetastorage(false);
        }
    }

    /**
     * @return Cache group predicate that passes only Metastorage cache group id.
     */
    private IgnitePredicate<Integer> onlyMetastorageGroup() {
        return groupId -> MetaStorage.METASTORAGE_CACHE_ID == groupId;
    }

    /**
     * @return Cache group predicate that passes only cache groups with enabled WAL.
     */
    private IgnitePredicate<Integer> groupsWithEnabledWal() {
        return groupId -> !initiallyGlobalWalDisabledGrps.contains(groupId)
            && !initiallyLocalWalDisabledGrps.contains(groupId);
    }

    /**
     * @return WAL records predicate that passes only Metastorage and encryption data records.
     */
    private IgniteBiPredicate<WALRecord.RecordType, WALPointer> onlyMetastorageAndEncryptionRecords() {
        return (type, ptr) -> type == METASTORE_DATA_RECORD || type == MASTER_KEY_CHANGE_RECORD;
    }

    /**
     * @return WAL records predicate that passes only physical and mixed WAL records.
     */
    private IgniteBiPredicate<WALRecord.RecordType, WALPointer> physicalRecords() {
        return (type, ptr) -> type.purpose() == WALRecord.RecordPurpose.PHYSICAL
            || type.purpose() == WALRecord.RecordPurpose.MIXED;
    }

    /**
     * @return WAL records predicate that passes only logical and mixed WAL records +
     * CP record (used for restoring initial partition states).
     */
    private IgniteBiPredicate<WALRecord.RecordType, WALPointer> logicalRecords() {
        return (type, ptr) -> type.purpose() == WALRecord.RecordPurpose.LOGICAL
            || type.purpose() == WALRecord.RecordPurpose.MIXED || type == CHECKPOINT_RECORD;
    }

    /**
     * Abstract class to create restore context.
     */
    private abstract class RestoreStateContext {
        /** Last archived segment. */
        protected final long lastArchivedSegment;

        /** Checkpoint status. */
        protected final CheckpointStatus status;

        /** WAL iterator. */
        private final WALIterator iterator;

        /** Only {@link WalRecordCacheGroupAware} records satisfied this predicate will be applied. */
        private final IgnitePredicate<Integer> cacheGroupPredicate;

        /**
         * @param status Checkpoint status.
         * @param iterator WAL iterator.
         * @param lastArchivedSegment Last archived segment index.
         * @param cacheGroupPredicate Cache groups predicate.
         */
        protected RestoreStateContext(
            CheckpointStatus status,
            WALIterator iterator,
            long lastArchivedSegment,
            IgnitePredicate<Integer> cacheGroupPredicate
        ) {
            this.status = status;
            this.iterator = iterator;
            this.lastArchivedSegment = lastArchivedSegment;
            this.cacheGroupPredicate = cacheGroupPredicate;
        }

        /**
         * Advance iterator to the next record.
         *
         * @return WALRecord entry.
         * @throws IgniteCheckedException If CRC check fail during binary recovery state or another exception occurring.
         */
        public WALRecord next() throws IgniteCheckedException {
            try {
                for (;;) {
                    if (!iterator.hasNextX())
                        return null;

                    IgniteBiTuple<WALPointer, WALRecord> tup = iterator.nextX();

                    if (tup == null)
                        return null;

                    WALRecord rec = tup.get2();

                    WALPointer ptr = tup.get1();

                    rec.position(ptr);

                    // Filter out records by group id.
                    if (rec instanceof WalRecordCacheGroupAware) {
                        WalRecordCacheGroupAware grpAwareRecord = (WalRecordCacheGroupAware) rec;

                        if (!cacheGroupPredicate.apply(grpAwareRecord.groupId()))
                            continue;
                    }

                    // Filter out data entries by group id.
                    if (rec instanceof DataRecord)
                        rec = filterEntriesByGroupId((DataRecord) rec);

                    return rec;
                }
            }
            catch (IgniteCheckedException e) {
                boolean throwsCRCError = throwsCRCError();

                if (X.hasCause(e, IgniteDataIntegrityViolationException.class)) {
                    if (throwsCRCError)
                        throw e;
                    else
                        return null;
                }

                log.error("There is an error during restore state [throwsCRCError=" + throwsCRCError + ']', e);

                throw e;
            }
        }

        /**
         * Filter outs data entries from given data record that not satisfy {@link #cacheGroupPredicate}.
         *
         * @param record Original data record.
         * @return Data record with filtered data entries.
         */
        private DataRecord filterEntriesByGroupId(DataRecord record) {
            List<DataEntry> filteredEntries = record.writeEntries().stream()
                .filter(entry -> {
                    int cacheId = entry.cacheId();

                    return cctx.cacheContext(cacheId) != null && cacheGroupPredicate.apply(cctx.cacheContext(cacheId).groupId());
                })
                .collect(Collectors.toList());

            return record.setWriteEntries(filteredEntries);
        }

        /**
         *
         * @return Last read WAL record pointer.
         */
        public FileWALPointer lastReadRecordPointer() {
            assert status.startPtr != null && status.startPtr instanceof FileWALPointer;

            return iterator.lastRead()
                .map(ptr -> (FileWALPointer)ptr)
                .orElseGet(() -> (FileWALPointer)status.startPtr);
        }

        /**
         *
         * @return Flag indicates need throws CRC exception or not.
         */
        public boolean throwsCRCError() {
            return lastReadRecordPointer().index() <= lastArchivedSegment;
        }
    }

    /**
     * Restore memory context. Tracks the safety of binary recovery.
     */
    public class RestoreBinaryState extends RestoreStateContext {

        /** The flag indicates need to apply the binary update or no needed. */
        private boolean needApplyBinaryUpdates;

        /**
         * @param status Checkpoint status.
         * @param iterator WAL iterator.
         * @param lastArchivedSegment Last archived segment index.
         * @param cacheGroupsPredicate Cache groups predicate.
         */
        public RestoreBinaryState(
            CheckpointStatus status,
            WALIterator iterator,
            long lastArchivedSegment,
            IgnitePredicate<Integer> cacheGroupsPredicate
        ) {
            super(status, iterator, lastArchivedSegment, cacheGroupsPredicate);

            this.needApplyBinaryUpdates = status.needRestoreMemory();
        }

        /**
         * Advance iterator to the next record.
         *
         * @return WALRecord entry.
         * @throws IgniteCheckedException If CRC check fail during binary recovery state or another exception occurring.
         */
        @Override public WALRecord next() throws IgniteCheckedException {
            WALRecord rec = super.next();

            if (rec == null)
                return null;

            if (rec.type() == CHECKPOINT_RECORD) {
                CheckpointRecord cpRec = (CheckpointRecord)rec;

                // We roll memory up until we find a checkpoint start record registered in the status.
                if (F.eq(cpRec.checkpointId(), status.cpStartId)) {
                    log.info("Found last checkpoint marker [cpId=" + cpRec.checkpointId() +
                        ", pos=" + rec.position() + ']');

                    needApplyBinaryUpdates = false;
                }
                else if (!F.eq(cpRec.checkpointId(), status.cpEndId))
                    U.warn(log, "Found unexpected checkpoint marker, skipping [cpId=" + cpRec.checkpointId() +
                        ", expCpId=" + status.cpStartId + ", pos=" + rec.position() + ']');
            }

            return rec;
        }

        /**
         *
         * @return Flag indicates need apply binary record or not.
         */
        public boolean needApplyBinaryUpdate() {
            return needApplyBinaryUpdates;
        }

        /**
         *
         * @return Flag indicates need throws CRC exception or not.
         */
        @Override public boolean throwsCRCError() {
            log.info("Throws CRC error check [needApplyBinaryUpdates=" + needApplyBinaryUpdates +
                ", lastArchivedSegment=" + lastArchivedSegment + ", lastRead=" + lastReadRecordPointer() + ']');

            if (needApplyBinaryUpdates)
                return true;

            return super.throwsCRCError();
        }
    }

    /**
     * Restore logical state context. Tracks the safety of logical recovery.
     */
    public class RestoreLogicalState extends RestoreStateContext {
        /** States of partitions recovered during applying logical updates. */
        private final Map<GroupPartitionId, Integer> partitionRecoveryStates;

        /**
         * @param lastArchivedSegment Last archived segment index.
         * @param partitionRecoveryStates Initial partition recovery states.
         */
        public RestoreLogicalState(CheckpointStatus status, WALIterator iterator, long lastArchivedSegment,
            IgnitePredicate<Integer> cacheGroupsPredicate, Map<GroupPartitionId, Integer> partitionRecoveryStates) {
            super(status, iterator, lastArchivedSegment, cacheGroupsPredicate);

            this.partitionRecoveryStates = partitionRecoveryStates;
        }

        /**
         * @return Map of restored partition states for cache groups.
         */
        public Map<GroupPartitionId, Integer> partitionRecoveryStates() {
            return Collections.unmodifiableMap(partitionRecoveryStates);
        }
    }

    /** Indicates checkpoint read lock acquisition failure which did not lead to node invalidation. */
    private static class CheckpointReadLockTimeoutException extends IgniteCheckedException {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private CheckpointReadLockTimeoutException(String msg) {
            super(msg);
        }
    }
}
