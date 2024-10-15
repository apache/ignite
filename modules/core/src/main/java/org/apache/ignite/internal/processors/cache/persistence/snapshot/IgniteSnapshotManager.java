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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.SnapshotEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.DistributedConfigurationUtils;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.management.cache.IdleVerifyResultV2;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.communication.TransmissionCancelledException;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;
import org.apache.ignite.internal.managers.encryption.EncryptionCacheKeyProvider;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.managers.encryption.GroupKeyEncrypted;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.systemview.walker.SnapshotViewWalker;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.IncrementalSnapshotFinishRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.GridLocalConfigManager;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.CreateDumpFutureTask;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.BasicRateLimiter;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.lang.IgniteThrowableFunction;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.spi.systemview.view.SnapshotView;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SNAPSHOT_SEQUENTIAL_WRITE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_FAILED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_FINISHED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_STARTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridClosureCallMode.BALANCE;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;
import static org.apache.ignite.internal.IgniteFeatures.PERSISTENCE_CACHE_SNAPSHOT;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.MarshallerContextImpl.mappingFileStoreWorkDir;
import static org.apache.ignite.internal.MarshallerContextImpl.resolveMappingFileStoreWorkDir;
import static org.apache.ignite.internal.MarshallerContextImpl.saveMappings;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.toDetailString;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.baselineNode;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isPersistenceEnabled;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.resolveBinaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirectories;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheGroupName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileName;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_ID;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getTypeByPartId;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.formatTmpDirName;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.T_DATA;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getPageIO;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedLongProperty;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.IgniteUtils.isLocalNodeCoordinator;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT;
import static org.apache.ignite.internal.util.io.GridFileUtils.ensureHardLinkAvailable;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_SNAPSHOT;
import static org.apache.ignite.spi.systemview.view.SnapshotView.SNAPSHOT_SYS_VIEW;
import static org.apache.ignite.spi.systemview.view.SnapshotView.SNAPSHOT_SYS_VIEW_DESC;

/**
 * Internal implementation of snapshot operations over persistence caches.
 * <p>
 * These major actions available:
 * <ul>
 *     <li>Create snapshot of the whole persistent cluster cache groups by triggering PME to achieve consistency.</li>
 *     <li>Create cache dump - snapshot of cluster cache groups including in-memory.</li>
 *     <li>Create incremental snapshot using lightweight, non-blocking Consistent Cut algorithm.</li>
 * </ul>
 */
public class IgniteSnapshotManager extends GridCacheSharedManagerAdapter
    implements IgniteSnapshot, PartitionsExchangeAware, MetastorageLifecycleListener, IgniteChangeGlobalStateSupport {
    /** File with delta pages suffix. */
    public static final String DELTA_SUFFIX = ".delta";

    /** File with delta pages index suffix. */
    public static final String DELTA_IDX_SUFFIX = ".idx";

    /** File name template consists of delta pages. */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** File name template for index delta pages. */
    public static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** Text Reason for checkpoint to start snapshot operation. */
    public static final String CP_SNAPSHOT_REASON = "Checkpoint started to enforce snapshot operation: %s";

    /** Name prefix for each remote snapshot operation. */
    public static final String RMT_SNAPSHOT_PREFIX = "snapshot_";

    /** Default snapshot directory for loading remote snapshots. */
    public static final String DFLT_SNAPSHOT_TMP_DIR = "snp";

    /** Snapshot in progress error message. */
    public static final String SNP_IN_PROGRESS_ERR_MSG = "Operation rejected due to the snapshot operation in progress.";

    /** Error message to finalize snapshot tasks. */
    public static final String SNP_NODE_STOPPING_ERR_MSG = "The operation is cancelled due to the local node is stopping";

    /** Snapshot metrics prefix. */
    public static final String SNAPSHOT_METRICS = "snapshot";

    /** Incremental snapshot metrics prefix. */
    public static final String INCREMENTAL_SNAPSHOT_METRICS = metricName("snapshot", "incremental");

    /** Snapshot metafile extension. */
    public static final String SNAPSHOT_METAFILE_EXT = ".smf";

    /** Snapshot temporary metafile extension. */
    public static final String SNAPSHOT_METAFILE_TMP_EXT = ".tmp";

    /** Prefix for snapshot threads. */
    public static final String SNAPSHOT_RUNNER_THREAD_PREFIX = "snapshot-runner";

    /** Snapshot transfer rate distributed configuration key */
    public static final String SNAPSHOT_TRANSFER_RATE_DMS_KEY = "snapshotTransferRate";

    /** Snapshot transfer rate is unlimited by default. */
    public static final long DFLT_SNAPSHOT_TRANSFER_RATE_BYTES = 0L;

    /** Maximum block size for limited snapshot transfer (64KB by default). */
    public static final int SNAPSHOT_LIMITED_TRANSFER_BLOCK_SIZE_BYTES = 64 * 1024;

    /** Metastorage key to save currently running snapshot directory path. */
    public static final String SNP_RUNNING_DIR_KEY = "snapshot-running-dir";

    /** Prefix for meta store records which means that incremental snapshot creation is disabled for a cache group. */
    private static final String INC_SNP_DISABLED_KEY_PREFIX = "grp-inc-snp-disabled-";

    /** Default value of {@link IgniteSystemProperties#IGNITE_SNAPSHOT_SEQUENTIAL_WRITE}. */
    public static final boolean DFLT_IGNITE_SNAPSHOT_SEQUENTIAL_WRITE = true;

    /** Default value of check flag. */
    public static final boolean DFLT_CHECK_ON_RESTORE = false;

    /** @deprecated Use #SNP_RUNNING_DIR_KEY instead. */
    @Deprecated
    private static final String SNP_RUNNING_KEY = "snapshot-running";

    /** Snapshot operation start log message. */
    private static final String SNAPSHOT_STARTED_MSG = "Cluster-wide snapshot operation started: ";

    /** Snapshot operation finish log message. */
    private static final String SNAPSHOT_FINISHED_MSG = "Cluster-wide snapshot operation finished successfully: ";

    /** Snapshot operation finish with warnings log message. */
    public static final String SNAPSHOT_FINISHED_WRN_MSG = "Cluster-wide snapshot operation finished with warnings: ";

    /** Snapshot operation fail log message. */
    private static final String SNAPSHOT_FAILED_MSG = "Cluster-wide snapshot operation failed: ";

    /** Default snapshot topic to receive snapshots from remote node. */
    private static final Object DFLT_INITIAL_SNAPSHOT_TOPIC = GridTopic.TOPIC_SNAPSHOT.topic("rmt_snp");

    /** File transmission parameter of cache group id. */
    private static final String SNP_GRP_ID_PARAM = "grpId";

    /** File transmission parameter of cache partition id. */
    private static final String SNP_PART_ID_PARAM = "partId";

    /** File transmission parameter of a cache directory with is currently sends its partitions. */
    private static final String SNP_CACHE_DIR_NAME_PARAM = "cacheDirName";

    /** Snapshot parameter name for a file transmission. */
    private static final String RQ_ID_NAME_PARAM = "rqId";

    /** Total snapshot files count which receiver should expect to receive. */
    private static final String SNP_PARTITIONS_CNT = "partsCnt";

    /** Incremental snapshots directory name. */
    public static final String INC_SNP_DIR = "increments";

    /** Pattern for incremental snapshot directory names. */
    public static final Pattern INC_SNP_NAME_PATTERN = U.fixedLengthNumberNamePattern(null);

    /** Lock file for dump directory. */
    public static final String DUMP_LOCK = "dump.lock";

    /**
     * Local buffer to perform copy-on-write operations with pages for {@code SnapshotFutureTask.PageStoreSerialWriter}s.
     * It is important to have only one buffer per thread (instead of creating each buffer per
     * each {@code SnapshotFutureTask.PageStoreSerialWriter}) this is redundant and can lead to OOM errors. Direct buffer
     * deallocate only when ByteBuffer is garbage collected, but it can get out of off-heap memory before it.
     */
    private final ThreadLocal<ByteBuffer> locBuff;

    /** Map of registered cache snapshot processes and their corresponding contexts. */
    private final ConcurrentMap<String, AbstractSnapshotFutureTask<?>> locSnpTasks = new ConcurrentHashMap<>();

    /** Lock to protect the resources is used. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Mutex used to order cluster snapshot operation progress. */
    private final Object snpOpMux = new Object();

    /** Take snapshot operation procedure. */
    private final DistributedProcess<SnapshotOperationRequest, SnapshotOperationResponse> startSnpProc;

    /** Check previously performed snapshot operation and delete uncompleted files if we need. */
    private final DistributedProcess<SnapshotOperationRequest, SnapshotOperationResponse> endSnpProc;

    /** Marshaller. */
    private final Marshaller marsh;

    /** Distributed process to restore cache group from the snapshot. */
    private final SnapshotRestoreProcess restoreCacheGrpProc;

    /** Transfer rate limiter. */
    private final BasicRateLimiter transferRateLimiter = new BasicRateLimiter(DFLT_SNAPSHOT_TRANSFER_RATE_BYTES);

    /** Resolved persistent data storage settings. */
    private volatile PdsFolderSettings<?> pdsSettings;

    /** Fully initialized metastorage. */
    private volatile ReadWriteMetastorage metaStorage;

    /** Local snapshot sender factory. */
    private BiFunction<String, String, SnapshotSender> locSndrFactory = LocalSnapshotSender::new;

    /** Remote snapshot sender factory. */
    private BiFunction<String, UUID, SnapshotSender> rmtSndrFactory = this::remoteSnapshotSenderFactory;

    /** Main snapshot directory to save created snapshots. */
    private volatile File locSnpDir;

    /**
     * Working directory for loaded snapshots from the remote nodes and storing
     * temporary partition delta-files of locally started snapshot process.
     */
    private @Nullable File tmpWorkDir;

    /** Factory to working with delta as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** File store manager to create page store for restore. */
    private volatile @Nullable FilePageStoreManager storeMgr;

    /** File store manager to create page store for restore. */
    private volatile GridLocalConfigManager locCfgMgr;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** Cluster snapshot operation requested by user. */
    private ClusterSnapshotFuture clusterSnpFut;

    /** Current snapshot operation on local node. */
    private volatile SnapshotOperationRequest clusterSnpReq;

    /** {@code true} if recovery process occurred for snapshot. */
    private volatile boolean recovered;

    /** Last seen cluster snapshot operation. */
    private volatile ClusterSnapshotFuture lastSeenSnpFut = new ClusterSnapshotFuture();

    /** Last seen incremental snapshot operation. */
    private volatile ClusterSnapshotFuture lastSeenIncSnpFut;

    /** Snapshot operation handlers. */
    private final SnapshotHandlers handlers = new SnapshotHandlers();

    /** Manager to receive responses of remote snapshot requests. */
    private final SequentialRemoteSnapshotManager snpRmtMgr;

    /** Incremental snapshot ID. */
    private volatile UUID incSnpId;

    /**
     * While incremental snapshot is running every node wraps outgoing transaction messages into {@link IncrementalSnapshotAwareMessage}.
     * Nodes start wrapping messages from the moment snapshot started on the node, and finsihes after all baseline
     * nodes completed {@link #markWalFut}. This future completes after last {@link IncrementalSnapshotAwareMessage} was sent.
     */
    private volatile GridFutureAdapter<?> wrapMsgsFut;

    /** Future that completes after {@link IncrementalSnapshotFinishRecord} was written. */
    private volatile @Nullable IncrementalSnapshotMarkWalFuture markWalFut;

    /** Snapshot transfer rate limit in bytes/sec. */
    private final DistributedLongProperty snapshotTransferRate = detachedLongProperty(SNAPSHOT_TRANSFER_RATE_DMS_KEY,
        "Snapshot transfer rate in bytes per second at which snapshot files are created. " +
            "0 means there is no limit.");

    /** Value of {@link IgniteSystemProperties#IGNITE_SNAPSHOT_SEQUENTIAL_WRITE}. */
    private final boolean sequentialWrite =
        IgniteSystemProperties.getBoolean(IGNITE_SNAPSHOT_SEQUENTIAL_WRITE, DFLT_IGNITE_SNAPSHOT_SEQUENTIAL_WRITE);

    /**
     * @param ctx Kernal context.
     */
    public IgniteSnapshotManager(GridKernalContext ctx) {
        locBuff = ThreadLocal.withInitial(() ->
            ByteBuffer.allocateDirect(ctx.config().getDataStorageConfiguration().getPageSize())
                .order(ByteOrder.nativeOrder()));

        startSnpProc = new DistributedProcess<>(ctx, START_SNAPSHOT, this::initLocalSnapshotStartStage,
            this::processLocalSnapshotStartStageResult, SnapshotStartDiscoveryMessage::new);

        endSnpProc = new DistributedProcess<>(ctx, END_SNAPSHOT, this::initLocalSnapshotEndStage,
            this::processLocalSnapshotEndStageResult, (reqId, req) -> new InitMessage<>(reqId, END_SNAPSHOT, req, true));

        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());

        restoreCacheGrpProc = new SnapshotRestoreProcess(ctx, locBuff);

        // Manage remote snapshots.
        snpRmtMgr = new SequentialRemoteSnapshotManager();
    }

    /**
     * @param snapshotCacheDir Snapshot directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    public static File partDeltaFile(File snapshotCacheDir, int partId) {
        return new File(snapshotCacheDir, partDeltaFileName(partId));
    }

    /**
     * Partition delta index file. Represents a sequence of page indexes that written to a delta.
     *
     * @param delta File with delta pages.
     * @return File with delta pages index.
     */
    public static File partDeltaIndexFile(File delta) {
        return new File(delta.getParent(), delta.getName() + DELTA_IDX_SUFFIX);
    }

    /**
     * @param partId Partition id.
     * @return File name of delta partition pages.
     */
    public static String partDeltaFileName(int partId) {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        return partId == INDEX_PARTITION ? INDEX_DELTA_NAME : String.format(PART_DELTA_TEMPLATE, partId);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        GridKernalContext ctx = cctx.kernalContext();

        if (ctx.clientNode())
            return;

        storeMgr = (FilePageStoreManager)cctx.pageStore();
        locCfgMgr = cctx.cache().configManager();

        pdsSettings = cctx.kernalContext().pdsFolderResolver().resolveFolders();

        boolean persistenceEnabled = isPersistenceEnabled(cctx.gridConfig());

        if (persistenceEnabled) {
            tmpWorkDir = U.resolveWorkDirectory(pdsSettings.persistentStoreNodePath().getAbsolutePath(), DFLT_SNAPSHOT_TMP_DIR, true);

            U.ensureDirectory(tmpWorkDir, "temp directory for snapshot creation", log);
        }

        initLocalSnapshotDirectory(persistenceEnabled);

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    snapshotTransferRate.addListener((name, oldVal, newVal) -> {
                        if (!Objects.equals(oldVal, newVal)) {
                            if (newVal < 0) {
                                log.warning("The snapshot transfer rate cannot be negative, " +
                                    "the value '" + newVal + "' is ignored.");

                                return;
                            }

                            transferRateLimiter.setRate(newVal);

                            if (log.isInfoEnabled()) {
                                log.info("The snapshot transfer rate " + (newVal == 0 ? "is not limited." :
                                    "has been changed from '" + oldVal + "' to '" + newVal + "' bytes/sec."));
                            }
                        }
                    });

                    dispatcher.registerProperty(snapshotTransferRate);
                }

                @Override public void onReadyToWrite() {
                    DistributedConfigurationUtils.setDefaultValue(snapshotTransferRate,
                        DFLT_SNAPSHOT_TRANSFER_RATE_BYTES, log);
                }
            }
        );

        handlers.initialize(ctx, ctx.pools().getSnapshotExecutorService());

        MetricRegistry mreg = cctx.kernalContext().metric().registry(SNAPSHOT_METRICS);

        mreg.register("LastSnapshotStartTime", () -> lastSeenSnpFut.startTime,
            "The system time of the last cluster snapshot request start time on this node.");
        mreg.register("LastSnapshotEndTime", () -> lastSeenSnpFut.endTime,
            "The system time of the last cluster snapshot request end time on this node.");
        mreg.register("LastSnapshotName", () -> lastSeenSnpFut.name, String.class,
            "The name of last started cluster snapshot request on this node.");
        mreg.register("LastSnapshotErrorMessage",
            () -> lastSeenSnpFut.error() == null ? "" : lastSeenSnpFut.error().getMessage(),
            String.class,
            "The error message of last started cluster snapshot request which fail with an error. " +
                "This value will be empty if last snapshot request has been completed successfully.");
        mreg.register("LocalSnapshotNames", () -> localSnapshotNames(null), List.class,
            "The list of names of all snapshots currently saved on the local node with respect to " +
                "the configured via IgniteConfiguration snapshot working path.");
        mreg.register("LastRequestId", () -> Optional.ofNullable(lastSeenSnpFut.rqId).map(UUID::toString).orElse(""),
            String.class, "The ID of the last started snapshot operation.");

        mreg.register("CurrentSnapshotTotalSize", () -> {
            SnapshotFutureTask task = currentSnapshotTask(SnapshotFutureTask.class);

            return task == null ? -1 : task.totalSize();
        }, "Estimated size of current cluster snapshot in bytes on this node. The value may grow during snapshot creation.");

        mreg.register("CurrentSnapshotProcessedSize", () -> {
            SnapshotFutureTask task = currentSnapshotTask(SnapshotFutureTask.class);

            return task == null ? -1 : task.processedSize();
        }, "Processed size of current cluster snapshot in bytes on this node.");

        MetricRegistry incSnpMReg = cctx.kernalContext().metric().registry(INCREMENTAL_SNAPSHOT_METRICS);

        incSnpMReg.register("snapshotName",
            () -> Optional.ofNullable(lastSeenIncSnpFut).map(f -> f.name).orElse(""),
            String.class,
            "The name of full snapshot for which the last incremental snapshot created on this node.");
        incSnpMReg.register("incrementIndex",
            () -> Optional.ofNullable(lastSeenIncSnpFut).map(f -> f.incIdx).orElse(0),
            "Ihe index of the last incremental snapshot created on this node.");
        incSnpMReg.register("startTime",
            () -> Optional.ofNullable(lastSeenIncSnpFut).map(f -> f.startTime).orElse(0L),
            "The system time of the last incremental snapshot creation start time on this node.");
        incSnpMReg.register("endTime",
            () -> Optional.ofNullable(lastSeenIncSnpFut).map(f -> f.endTime).orElse(0L),
            "The system time of the last incremental snapshot creation end time on this node.");
        incSnpMReg.register("error",
            () -> Optional.ofNullable(lastSeenIncSnpFut).map(GridFutureAdapter::error).map(Object::toString).orElse(""),
            String.class,
            "The error message of last started incremental snapshot on this node.");

        restoreCacheGrpProc.registerMetrics();

        cctx.exchange().registerExchangeAwareComponent(this);

        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);

        cctx.gridEvents().addDiscoveryEventListener(discoLsnr = (evt, discoCache) -> {
            if (!busyLock.enterBusy())
                return;

            try {
                UUID leftNodeId = evt.eventNode().id();

                if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED) {
                    SnapshotOperationRequest snpReq = clusterSnpReq;
                    String err = "Snapshot operation interrupted, because baseline node left the cluster: " + leftNodeId;
                    boolean reqNodeLeft = snpReq != null && snpReq.nodes().contains(leftNodeId);

                    // If the coordinator left the cluster and did not start
                    // the final snapshot phase (SNAPSHOT_END), we start it from a new one.
                    if (reqNodeLeft && snpReq.startStageEnded() && U.isLocalNodeCoordinator(ctx.discovery())) {
                        snpReq.error(new ClusterTopologyCheckedException(err));

                        endSnpProc.start(snpReq.requestId(), snpReq);
                    }

                    for (AbstractSnapshotFutureTask<?> sctx : locSnpTasks.values()) {
                        if (sctx.sourceNodeId().equals(leftNodeId) ||
                            (reqNodeLeft && snpReq.snapshotName().equals(sctx.snapshotName())))
                            sctx.acceptException(new ClusterTopologyCheckedException(err));
                    }

                    restoreCacheGrpProc.onNodeLeft(leftNodeId);
                    snpRmtMgr.onNodeLeft(leftNodeId);
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        cctx.gridIO().addMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC, snpRmtMgr);
        cctx.kernalContext().io().addTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC, snpRmtMgr);

        ctx.systemView().registerView(
            SNAPSHOT_SYS_VIEW,
            SNAPSHOT_SYS_VIEW_DESC,
            new SnapshotViewWalker(),
            () -> F.flatCollections(F.transform(localSnapshotNames(null), name -> {
                List<SnapshotView> views = new ArrayList<>();

                for (SnapshotMetadata m: readSnapshotMetadatas(name, null)) {
                    List<File> dirs = snapshotCacheDirectories(m.snapshotName(), null, m.folderName(), grpName -> true);

                    Collection<String> cacheGrps = F.viewReadOnly(dirs, FilePageStoreManager::cacheGroupName);

                    views.add(new SnapshotView(m, cacheGrps));
                }

                for (IncrementalSnapshotMetadata m: readIncrementalSnapshotMetadatas(name))
                    views.add(new SnapshotView(m));

                return views;
            })),
            Function.identity());

        File[] files = locSnpDir.listFiles();

        if (files != null) {
            Arrays.stream(files)
                .filter(File::isDirectory)
                .map(dumpDir ->
                    Paths.get(dumpDir.getAbsolutePath(), DB_DEFAULT_FOLDER, pdsSettings.folderName(), DUMP_LOCK).toFile())
                .filter(File::exists)
                .map(File::getParentFile)
                .forEach(lockedDumpDir -> {
                    log.warning("Found locked dump dir. " +
                        "This means, dump creation not finished prior to node fail. " +
                        "Directory will be deleted: " + lockedDumpDir);

                    U.delete(lockedDumpDir);
                });
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        busyLock.block();

        try {
            snpRmtMgr.stop();

            restoreCacheGrpProc.interrupt(new NodeStoppingException("Node is stopping."));

            // Try stop all snapshot processing if not yet.
            for (AbstractSnapshotFutureTask<?> sctx : locSnpTasks.values())
                sctx.acceptException(new NodeStoppingException(SNP_NODE_STOPPING_ERR_MSG));

            locSnpTasks.clear();

            synchronized (snpOpMux) {
                if (clusterSnpFut != null) {
                    clusterSnpFut.onDone(new NodeStoppingException(SNP_NODE_STOPPING_ERR_MSG));

                    clusterSnpFut = null;
                }
            }

            cctx.kernalContext().io().removeMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC);
            cctx.kernalContext().io().removeTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC);

            if (discoLsnr != null)
                cctx.kernalContext().event().removeDiscoveryEventListener(discoLsnr);

            cctx.exchange().unregisterExchangeAwareComponent(this);
        }
        finally {
            busyLock.unblock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        restoreCacheGrpProc.interrupt(new IgniteCheckedException("The cluster has been deactivated."));
    }

    /**
     * @param snpDir Snapshot dir.
     * @param pdsSettings PDS settings.
     */
    public void deleteSnapshot(File snpDir, PdsFolderSettings<?> pdsSettings) {
        if (!snpDir.exists())
            return;

        if (!snpDir.isDirectory())
            return;

        String folderName = pdsSettings.folderName();

        try {
            File binDir = binaryWorkDir(snpDir.getAbsolutePath(), folderName);
            File nodeDbDir = new File(snpDir.getAbsolutePath(), databaseRelativePath(folderName));
            File smf = new File(snpDir, snapshotMetaFileName(U.maskForFileName(pdsSettings.consistentId().toString())));

            U.delete(binDir);
            U.delete(nodeDbDir);
            U.delete(smf);

            File marshDir = mappingFileStoreWorkDir(snpDir.getAbsolutePath());

            deleteDirectory(marshDir);

            File binMetadataDfltDir = new File(snpDir, DFLT_BINARY_METADATA_PATH);
            File marshallerDfltDir = new File(snpDir, DFLT_MARSHALLER_PATH);

            deleteDirectory(binMetadataDfltDir);
            deleteDirectory(marshallerDfltDir);

            File db = new File(snpDir, DB_DEFAULT_FOLDER);

            db.delete();
            snpDir.delete();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** Concurrently traverse the directory and delete all files. */
    private void deleteDirectory(File dir) throws IOException {
        Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<Path>() {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                U.delete(file);

                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                // Skip files which can be concurrently removed from FileTree.
                return FileVisitResult.CONTINUE;
            }

            @Override public FileVisitResult postVisitDirectory(Path dir, IOException e) {
                dir.toFile().delete();

                if (log.isInfoEnabled() && e != null)
                    log.info("Snapshot directory cleaned with an exception [dir=" + dir + ", e=" + e.getMessage() + ']');

                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * @param snpName Snapshot name.
     * @return Local snapshot directory for snapshot with given name.
     */
    public File snapshotLocalDir(String snpName) {
        return snapshotLocalDir(snpName, null);
    }

    /**
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @return Local snapshot directory where snapshot files are located.
     */
    public File snapshotLocalDir(String snpName, @Nullable String snpPath) {
        assert locSnpDir != null;
        assert U.alphanumericUnderscore(snpName) : snpName;

        return snpPath == null ? new File(locSnpDir, snpName) : new File(snpPath, snpName);
    }

    /**
     * Returns path to specific incremental snapshot.
     * For example, {@code "work/snapshots/mybackup/increments/0000000000000001"}.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param incIdx Increment index.
     * @return Local snapshot directory where snapshot files are located.
     */
    public File incrementalSnapshotLocalDir(String snpName, @Nullable String snpPath, int incIdx) {
        return new File(incrementalSnapshotsLocalRootDir(snpName, snpPath), U.fixedLengthNumberName(incIdx, null));
    }

    /**
     * Returns root folder for incremental snapshot.
     * For example, {@code "work/snapshots/mybackup/increments/"}.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @return Local snapshot directory where snapshot files are located.
     */
    public File incrementalSnapshotsLocalRootDir(String snpName, @Nullable String snpPath) {
        return new File(snapshotLocalDir(snpName, snpPath), INC_SNP_DIR);
    }

    /**
     * @param incSnpDir Incremental snapshot directory.
     * @param consId Consistent ID.
     * @return WALs directory for specified incremental snapshot.
     */
    public static File incrementalSnapshotWalsDir(File incSnpDir, String consId) {
        String folderName = U.maskForFileName(consId);

        return incSnpDir.toPath().resolve(DFLT_WAL_PATH).resolve(folderName).toFile();
    }

    /**
     * @param snpName Snapshot name.
     * @return Local snapshot directory for snapshot with given name.
     * @throws IgniteCheckedException If directory doesn't exist.
     */
    private File resolveSnapshotDir(String snpName, @Nullable String snpPath) throws IgniteCheckedException {
        File snpDir = snapshotLocalDir(snpName, snpPath);

        if (!snpDir.exists())
            throw new IgniteCheckedException("Snapshot directory doesn't exists: " + snpDir.getAbsolutePath());

        return snpDir;
    }

    /**
     * @return Node snapshot working directory.
     */
    public File snapshotTmpDir() {
        assert tmpWorkDir != null;

        return tmpWorkDir;
    }

    /** */
    private void initLocalSnapshotDirectory(boolean create) {
        try {
            locSnpDir = resolveSnapshotWorkDirectory(cctx.kernalContext().config(), create);

            if (create)
                U.ensureDirectory(locSnpDir, "snapshot work directory", log);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when a snapshot has been started.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalSnapshotStartStage(SnapshotOperationRequest req) {
        // Executed inside discovery notifier thread, prior to firing discovery custom event,
        // so it is safe to set new snapshot task inside this method without synchronization.
        if (clusterSnpReq != null) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot operation has been rejected. " +
                "Another snapshot operation in progress [req=" + req + ", curr=" + clusterSnpReq + ']'));
        }

        clusterSnpReq = req;

        if (req.incremental())
            handleIncrementalSnapshotId(req.requestId(), cctx.discovery().topologyVersion());

        if (!CU.baselineNode(cctx.localNode(), cctx.kernalContext().state().clusterState()))
            return new GridFinishedFuture<>();

        Set<UUID> leftNodes = new HashSet<>(req.nodes());
        leftNodes.removeAll(F.viewReadOnly(cctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            F.node2id()));

        if (!leftNodes.isEmpty()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Some of baseline nodes left the cluster " +
                "prior to snapshot operation start: " + leftNodes));
        }

        if (cctx.kernalContext().encryption().isMasterKeyChangeInProgress()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot operation has been rejected. Master " +
                "key changing process is not finished yet."));
        }

        if (cctx.kernalContext().encryption().reencryptionInProgress()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot operation has been rejected. Caches " +
                "re-encryption process is not finished yet."));
        }

        List<Integer> grpIds = new ArrayList<>(F.viewReadOnly(req.groups(), CU::cacheId));
        Collection<Integer> comprGrpIds = F.view(grpIds, i -> {
            CacheGroupDescriptor desc = cctx.cache().cacheGroupDescriptor(i);
            return desc != null && desc.config().getDiskPageCompression() != DiskPageCompression.DISABLED;
        });

        Set<Integer> leftGrps = new HashSet<>(grpIds);
        leftGrps.removeAll(cctx.cache().cacheGroupDescriptors().keySet());
        boolean withMetaStorage = leftGrps.remove(METASTORAGE_CACHE_ID);

        if (!leftGrps.isEmpty()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Some of requested cache groups doesn't exist " +
                "on the local node [missed=" + leftGrps + ", nodeId=" + cctx.localNodeId() + ']'));
        }

        if (req.incremental()) {
            SnapshotMetadata meta;

            try {
                meta = readSnapshotMetadata(new File(
                    snapshotLocalDir(req.snapshotName(), req.snapshotPath()),
                    snapshotMetaFileName(cctx.localNode().consistentId().toString())
                ));

                checkIncrementalCanBeCreated(req.snapshotName(), req.snapshotPath(), meta);
            }
            catch (IgniteCheckedException | IOException e) {
                return new GridFinishedFuture<>(e);
            }

            return initLocalIncrementalSnapshot(req, meta);
        }
        else
            return initLocalFullSnapshot(req, grpIds, comprGrpIds, withMetaStorage);
    }

    /**
     * Handles received incremental snapshot ID from remote node.
     *
     * @param id Incremental snapshot ID.
     * @param topVer Incremental snapshot topology version.
     */
    public void handleIncrementalSnapshotId(UUID id, long topVer) {
        // TODO: IGNITE-18599 handle if `id != incSnpId`.
        if (incSnpId != null)
            return;

        synchronized (snpOpMux) {
            if (incSnpId != null) {
                if (!incSnpId.equals(id))
                    U.warn(log, "Received incremental snapshot ID differs from the current [rcvId=" + id + ", currId=" + incSnpId + ']');

                return;
            }

            wrapMsgsFut = new GridFutureAdapter<>();

            cctx.tm().txMessageTransformer((msg, tx) -> new IncrementalSnapshotAwareMessage(
                msg, id, tx == null ? null : tx.incrementalSnapshotId(), topVer));

            markWalFut = baselineNode(cctx.localNode(), cctx.kernalContext().state().clusterState())
                ? new IncrementalSnapshotMarkWalFuture(cctx, id, topVer) : null;

            incSnpId = id;
        }

        if (markWalFut != null)
            cctx.kernalContext().pools().getSnapshotExecutorService().submit(markWalFut::init);
    }

    /**
     * @param req Request on snapshot creation.
     * @param meta Full snapshot metadata.
     * @return Future which will be completed when a snapshot has been started.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalIncrementalSnapshot(
        SnapshotOperationRequest req,
        SnapshotMetadata meta
    ) {
        File incSnpDir = incrementalSnapshotLocalDir(req.snapshotName(), req.snapshotPath(), req.incrementIndex());
        WALPointer lowPtr;

        if (req.incrementIndex() == 1)
            lowPtr = meta.snapshotRecordPointer();
        else {
            int prevIdx = req.incrementIndex() - 1;

            IncrementalSnapshotMetadata prevIncSnpMeta;

            try {
                prevIncSnpMeta = readIncrementalSnapshotMetadata(req.snapshotName(), req.snapshotPath(), prevIdx);
            }
            catch (IgniteCheckedException | IOException e) {
                return new GridFinishedFuture<>(e);
            }

            lowPtr = prevIncSnpMeta.incrementalSnapshotPointer();
        }

        IgniteInternalFuture<SnapshotOperationResponse> task0 = registerTask(req.snapshotName(), new IncrementalSnapshotFutureTask(
            cctx,
            req.operationalNodeId(),
            req.requestId(),
            meta,
            req.snapshotPath(),
            req.incrementIndex(),
            lowPtr,
            markWalFut
        )).chain(fut -> {
            if (fut.error() != null)
                throw F.wrap(fut.error());

            assert incSnpDir.exists() : "Incremental snapshot directory must exists";

            IncrementalSnapshotMetadata incMeta = new IncrementalSnapshotMetadata(
                req.requestId(),
                req.snapshotName(),
                req.incrementIndex(),
                cctx.localNode().consistentId().toString(),
                pdsSettings.folderName(),
                markWalFut.result()
            );

            storeSnapshotMeta(
                incMeta,
                new File(incSnpDir, snapshotMetaFileName(pdsSettings.folderName()))
            );

            return new SnapshotOperationResponse();
        });

        if (task0.isDone())
            return task0;

        if (log.isDebugEnabled()) {
            log.debug("Incremental snapshot operation submited for execution " +
                "[snpName=" + req.snapshotName() + ", incIdx=" + req.incrementIndex());
        }

        cctx.kernalContext().pools().getSnapshotExecutorService().submit(() -> {
            SnapshotOperationRequest snpReq = clusterSnpReq;

            AbstractSnapshotFutureTask<?> task = locSnpTasks.get(snpReq.snapshotName());

            if (task == null)
                return;

            if (log.isDebugEnabled()) {
                log.debug("Incremental snapshot operation started " +
                    "[snpName=" + req.snapshotName() + ", incIdx=" + req.incrementIndex());
            }

            writeSnapshotDirectoryToMetastorage(incSnpDir);

            task.start();
        });

        return task0;
    }

    /**
     * @param snpName Full snapshot name.
     * @param snpPath Optional path to snapshot, if differs from default.
     * @param incIdx Index of incremental snapshot.
     * @return Read incremental snapshot metadata.
     */
    public IncrementalSnapshotMetadata readIncrementalSnapshotMetadata(
        String snpName,
        @Nullable String snpPath,
        int incIdx
    ) throws IgniteCheckedException, IOException {
        return readFromFile(new File(
            incrementalSnapshotLocalDir(snpName, snpPath, incIdx),
            snapshotMetaFileName(pdsSettings.folderName())
        ));
    }

    /**
     * @param req Request
     * @param grpIds Groups.
     * @param comprGrpIds Compressed Groups.
     * @param withMetaStorage Flag to include metastorage.
     * @return Create snapshot future.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalFullSnapshot(
        SnapshotOperationRequest req,
        List<Integer> grpIds,
        Collection<Integer> comprGrpIds,
        boolean withMetaStorage
    ) {
        if (!isPersistenceEnabled(cctx.gridConfig()) && req.snapshotPath() == null)
            initLocalSnapshotDirectory(true);

        Map<Integer, Set<Integer>> parts = new HashMap<>();

        // Prepare collection of pairs group and appropriate cache partition to be snapshot.
        // Cache group context may be 'null' on some nodes e.g. a node filter is set.
        for (Integer grpId : grpIds) {
            if (cctx.cache().cacheGroup(grpId) == null)
                continue;

            CacheGroupContext grpCtx = cctx.cache().cacheGroup(grpId);

            AffinityTopologyVersion topVer = grpCtx.affinity().lastVersion();

            if (req.onlyPrimary()) {
                Set<Integer> include = new HashSet<>(grpCtx.affinity().primaryPartitions(cctx.localNodeId(), topVer));

                include.remove(INDEX_PARTITION);

                if (log.isInfoEnabled())
                    log.info("Snapshot only primary partitions " +
                        "[grpId=" + grpId + ", grpName=" + grpCtx.cacheOrGroupName() + ", parts=" + include + ']');

                parts.put(grpId, include);
            }
            else
                parts.put(grpId, null);
        }

        IgniteInternalFuture<?> task0 = registerSnapshotTask(req.snapshotName(),
            req.snapshotPath(),
            req.operationalNodeId(),
            req.requestId(),
            parts,
            withMetaStorage,
            req.dump(),
            req.compress(),
            req.encrypt(),
            locSndrFactory.apply(req.snapshotName(), req.snapshotPath())
        );

        if (withMetaStorage) {
            assert task0 instanceof SnapshotFutureTask;

            ((DistributedMetaStorageImpl)cctx.kernalContext().distributedMetastorage())
                .suspend(((SnapshotFutureTask)task0).started());
        }

        return task0.chain(() -> {
            if (task0.error() != null)
                throw F.wrap(task0.error());

            try {
                Set<String> blts = req.nodes().stream()
                    .map(n -> cctx.discovery().node(n).consistentId().toString())
                    .collect(Collectors.toSet());

                File snpDir = snapshotLocalDir(req.snapshotName(), req.snapshotPath());

                snpDir.mkdirs();

                SnapshotFutureTaskResult res = (SnapshotFutureTaskResult)task0.result();

                Serializable encKey = req.encrypt() ? ((CreateDumpFutureTask)task0).encryptionKey() : null;

                EncryptionSpi encSpi = cctx.gridConfig().getEncryptionSpi();

                SnapshotMetadata meta = new SnapshotMetadata(req.requestId(),
                    req.snapshotName(),
                    cctx.localNode().consistentId().toString(),
                    pdsSettings.folderName(),
                    req.compress(),
                    cctx.gridConfig().getDataStorageConfiguration().getPageSize(),
                    grpIds,
                    comprGrpIds,
                    blts,
                    res.parts(),
                    res.snapshotPointer(),
                    encSpi.masterKeyDigest(),
                    req.onlyPrimary(),
                    req.dump(),
                    encKey == null ? null : encSpi.encryptKey(encKey)
                );

                SnapshotHandlerContext ctx = new SnapshotHandlerContext(meta, req.groups(), cctx.localNode(), snpDir,
                    req.streamerWarning(), true);

                req.meta(meta);

                File smf = new File(snpDir, snapshotMetaFileName(cctx.localNode().consistentId().toString()));

                storeSnapshotMeta(req.meta(), smf);

                log.info("Snapshot metafile has been created: " + smf.getAbsolutePath());

                return new SnapshotOperationResponse(handlers.invokeAll(SnapshotHandlerType.CREATE, ctx));
            }
            catch (IgniteCheckedException e) {
                throw F.wrap(e);
            }
        }, snapshotExecutorService());
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void processLocalSnapshotStartStageResult(UUID id, Map<UUID, SnapshotOperationResponse> res, Map<UUID, Throwable> err) {
        SnapshotOperationRequest snpReq = clusterSnpReq;

        if (snpReq != null && F.eq(id, snpReq.requestId()) && snpReq.incremental()) {
            cctx.tm().txMessageTransformer(null);

            GridCompoundIdentityFuture<IgniteInternalTx> activeTxsFut = new GridCompoundIdentityFuture<>();

            for (IgniteInternalTx tx: cctx.tm().activeTransactions())
                activeTxsFut.add(tx.finishFuture());

            activeTxsFut.markInitialized();

            activeTxsFut.listen(() -> wrapMsgsFut.onDone());
        }

        if (cctx.kernalContext().clientNode())
            return;

        boolean cancelled = err.values().stream().anyMatch(e -> e instanceof IgniteFutureCancelledCheckedException);

        if (snpReq == null || !snpReq.requestId().equals(id)) {
            synchronized (snpOpMux) {
                if (clusterSnpFut != null && clusterSnpFut.rqId.equals(id)) {
                    if (cancelled) {
                        clusterSnpFut.onDone(new IgniteFutureCancelledCheckedException("Execution of snapshot tasks " +
                            "has been cancelled by external process [err=" + err + ", snpReq=" + snpReq + ']'));
                    }
                    else {
                        clusterSnpFut.onDone(new IgniteCheckedException("Snapshot operation has not been fully completed " +
                            "[err=" + err + ", snpReq=" + snpReq + ']'));
                    }

                    clusterSnpFut = null;
                }

                return;
            }
        }

        snpReq.startStageEnded(true);

        if (isLocalNodeCoordinator(cctx.discovery())) {
            Set<UUID> missed = new HashSet<>(snpReq.nodes());
            missed.removeAll(res.keySet());
            missed.removeAll(err.keySet());

            if (cancelled) {
                snpReq.error(new IgniteFutureCancelledCheckedException("Execution of snapshot tasks " +
                    "has been cancelled by external process [err=" + err + ", missed=" + missed + ']'));
            }
            else if (!missed.isEmpty()) {
                snpReq.error(new ClusterTopologyCheckedException("Snapshot operation interrupted, because baseline " +
                    "node left the cluster. Uncompleted snapshot will be deleted [missed=" + missed + ']'));
            }
            else if (!F.isEmpty(err)) {
                snpReq.error(new IgniteCheckedException("Execution of local snapshot tasks fails. " +
                    "Uncompleted snapshot will be deleted [err=" + err + ']'));
            }

            completeHandlersAsyncIfNeeded(snpReq, res.values())
                .listen(f -> {
                        if (f.error() != null)
                            snpReq.error(f.error());

                        endSnpProc.start(snpReq.requestId(), snpReq);
                    }
                );
        }
    }

    /**
     * Stores snapshot metadata.
     *
     * @param meta Metadata to store.
     * @param smf File to store.
     */
    public <M extends Serializable> void storeSnapshotMeta(M meta, File smf) {
        if (smf.exists())
            throw new IgniteException("Snapshot metafile must not exist: " + smf.getAbsolutePath());

        try (OutputStream out = Files.newOutputStream(smf.toPath())) {
            byte[] bytes = U.marshal(marsh, meta);
            int blockSize = SNAPSHOT_LIMITED_TRANSFER_BLOCK_SIZE_BYTES;

            for (int off = 0; off < bytes.length; off += blockSize) {
                int len = Math.min(blockSize, bytes.length - off);

                transferRateLimiter.acquire(len);

                out.write(bytes, off, len);
            }
        }
        catch (IOException | IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Execute the {@link SnapshotHandler#complete(String, Collection)} method of the snapshot handlers asynchronously.
     *
     * @param req Request on snapshot creation.
     * @param res Results.
     * @return Future that will be completed when the handlers are finished executing.
     */
    private IgniteInternalFuture<Void> completeHandlersAsyncIfNeeded(SnapshotOperationRequest req,
        Collection<SnapshotOperationResponse> res) {
        if (req.error() != null)
            return new GridFinishedFuture<>();

        Map<String, List<SnapshotHandlerResult<?>>> clusterHndResults = new HashMap<>();

        for (SnapshotOperationResponse snpRes : res) {
            if (snpRes == null || snpRes.handlerResults() == null)
                continue;

            for (Map.Entry<String, SnapshotHandlerResult<Object>> entry : snpRes.handlerResults().entrySet())
                clusterHndResults.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(entry.getValue());
        }

        if (clusterHndResults.isEmpty())
            return new GridFinishedFuture<>();

        try {
            GridFutureAdapter<Void> resultFut = new GridFutureAdapter<>();

            handlers().execSvc.submit(() -> {
                try {
                    handlers.completeAll(SnapshotHandlerType.CREATE, req.snapshotName(), clusterHndResults, req.nodes(),
                        req::warnings);

                    resultFut.onDone();
                }
                catch (Exception e) {
                    log.warning("The snapshot operation will be aborted due to a handler error " +
                        "[snapshot=" + req.snapshotName() + "].", e);

                    resultFut.onDone(e);
                }
            });

            return resultFut;
        }
        catch (RejectedExecutionException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when the snapshot will be finalized.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalSnapshotEndStage(SnapshotOperationRequest req) {
        SnapshotOperationRequest snpReq = clusterSnpReq;

        if (snpReq == null || !F.eq(req.requestId(), snpReq.requestId()))
            return new GridFinishedFuture<>();

        IgniteInternalFuture<?> prepFut = req.incremental() ? wrapMsgsFut : new GridFinishedFuture<>();

        if (cctx.kernalContext().clientNode())
            return (IgniteInternalFuture<SnapshotOperationResponse>)prepFut;

        return prepFut.chain(() -> {
            try {
                if (req.error() != null) {
                    snpReq.error(req.error());

                    if (req.incremental())
                        U.delete(incrementalSnapshotLocalDir(req.snapshotName(), req.snapshotPath(), req.incrementIndex()));
                    else
                        deleteSnapshot(snapshotLocalDir(req.snapshotName(), req.snapshotPath()), pdsSettings);
                }
                else if (!F.isEmpty(req.warnings())) {
                    // Pass the warnings further to the next stage for the case when snapshot started from not coordinator.
                    if (!isLocalNodeCoordinator(cctx.discovery()))
                        snpReq.warnings(req.warnings());

                    snpReq.meta().warnings(Collections.unmodifiableList(req.warnings()));

                    storeWarnings(snpReq);
                }

                if (req.dump())
                    removeDumpLock(req.snapshotName());
                else {
                    removeLastMetaStorageKey();

                    if (req.error() == null) {
                        Collection<Integer> grpIds = req.groups().stream().map(CU::cacheId).collect(Collectors.toList());

                        enableIncrementalSnapshotsCreation(grpIds);
                    }
                }
            }
            catch (Exception e) {
                throw F.wrap(e);
            }

            return new SnapshotOperationResponse();
        }, cctx.kernalContext().pools().getSnapshotExecutorService());
    }

    /**
     * Stores snapshot creation warnings. The warnings are rare. Also, coordinator might not be a baseline node. Thus,
     * storing meta with warnings once is to be done at second stage initialiation on any other node. Which leads to
     * process possible snapshot errors, deleting snapshot at second stage end. Doesn't worth. If an error occurs on
     * warnings writing, it is logged only.
     */
    private void storeWarnings(SnapshotOperationRequest snpReq) {
        assert !F.isEmpty(snpReq.warnings());

        List<ClusterNode> snpNodes = cctx.kernalContext().cluster().get().nodes().stream()
            .filter(n -> snpReq.nodes().contains(n.id())).collect(Collectors.toList());

        boolean oldestBaseline = U.oldest(snpNodes,
            n -> CU.baselineNode(n, cctx.kernalContext().state().clusterState())).equals(cctx.localNode());

        if (!oldestBaseline)
            return;

        File snpDir = snapshotLocalDir(snpReq.snapshotName(), snpReq.snapshotPath());
        File tempSmf = new File(snpDir, snapshotMetaFileName(cctx.localNode().consistentId().toString()) +
            SNAPSHOT_METAFILE_TMP_EXT);
        File smf = new File(snpDir, snapshotMetaFileName(cctx.localNode().consistentId().toString()));

        try {
            storeSnapshotMeta(snpReq.meta(), tempSmf);

            Files.move(tempSmf.toPath(), smf.toPath(), StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);

            if (log.isDebugEnabled())
                log.debug("Snapshot metafile has been rewrited with the warnings: " + smf.getAbsolutePath());
        }
        catch (Exception e) {
            log.error("Failed to store warnings of snapshot '" + snpReq.snapshotName() +
                "' to the snapshot metafile. Snapshot won't contain them. The warnings: [" +
                String.join(",", snpReq.warnings()) + "].", e);
        }
        finally {
            U.delete(tempSmf);
        }
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void processLocalSnapshotEndStageResult(UUID id, Map<UUID, SnapshotOperationResponse> res, Map<UUID, Throwable> err) {
        SnapshotOperationRequest snpReq = clusterSnpReq;

        if (snpReq == null || !F.eq(id, snpReq.requestId()))
            return;

        Set<UUID> endFail = new HashSet<>(snpReq.nodes());
        endFail.removeAll(res.keySet());

        if (snpReq.incremental()) {
            wrapMsgsFut = null;
            markWalFut = null;

            incSnpId = null;

            if (clusterSnpFut != null && endFail.isEmpty() && snpReq.error() == null)
                warnAtomicCachesInIncrementalSnapshot(snpReq.snapshotName(), snpReq.incrementIndex(), snpReq.groups());
        }

        clusterSnpReq = null;

        synchronized (snpOpMux) {
            if (clusterSnpFut != null) {
                if (endFail.isEmpty() && snpReq.error() == null) {
                    if (!F.isEmpty(snpReq.warnings())) {
                        String wrnsLst = U.nl() + "\t- " + String.join(U.nl() + "\t- ", snpReq.warnings());

                        SnapshotWarningException wrn = new SnapshotWarningException("Snapshot task '" +
                            snpReq.snapshotName() + "' completed with the warnings:" + wrnsLst);

                        clusterSnpFut.onDone(wrn);

                        log.warning(SNAPSHOT_FINISHED_WRN_MSG + snpReq + ". Warnings:" + wrnsLst);
                    }
                    else {
                        clusterSnpFut.onDone();

                        if (log.isInfoEnabled())
                            log.info(SNAPSHOT_FINISHED_MSG + snpReq);
                    }
                }
                else if (snpReq.error() == null) {
                    clusterSnpFut.onDone(new IgniteCheckedException("Snapshot creation has been finished with an error. " +
                        "Local snapshot tasks may not finished completely or finalizing results fails " +
                        "[fail=" + endFail + ", err=" + err + ']'));
                }
                else
                    clusterSnpFut.onDone(snpReq.error());

                clusterSnpFut = null;
            }
        }
    }

    /**
     * @return {@code True} if snapshot operation is in progress.
     */
    public boolean isSnapshotCreating() {
        if (clusterSnpReq != null)
            return true;

        synchronized (snpOpMux) {
            return clusterSnpReq != null || clusterSnpFut != null;
        }
    }

    /**
     * Sets the streamer warning flag to current snapshot process if it is active.
     */
    public void streamerWarning() {
        SnapshotOperationRequest snpTask = currentCreateRequest();

        if (snpTask != null && !snpTask.streamerWarning())
            snpTask.streamerWarning(true);
    }

    /** @return Current create snapshot request. {@code Null} if there is no create snapshot operation in progress. */
    @Nullable public SnapshotOperationRequest currentCreateRequest() {
        return clusterSnpReq;
    }

    /**
     * Check if snapshot restore process is currently running.
     *
     * @return {@code True} if the snapshot restore operation is in progress.
     */
    public boolean isRestoring() {
        return restoreCacheGrpProc.restoringSnapshotName() != null;
    }

    /**
     * Check if snapshot restore process is currently running.
     *
     * @param snpName Snapshot name.
     * @return {@code True} if the snapshot restore operation from the specified snapshot is in progress locally.
     */
    public boolean isRestoring(String snpName) {
        return snpName.equals(restoreCacheGrpProc.restoringSnapshotName());
    }

    /**
     * Check if the cache or group with the specified name is currently being restored from the snapshot.
     *
     * @param ccfg Cache configuration.
     * @return {@code True} if the cache or group with the specified name is being restored.
     */
    public boolean isRestoring(CacheConfiguration<?, ?> ccfg) {
        return restoreCacheGrpProc.isRestoring(ccfg);
    }

    /**
     * Status of the restore operation cluster-wide.
     *
     * @param snpName Snapshot name.
     * @return Future that will be completed when the status of the restore operation is received from all the server
     * nodes. The result of this future will be {@code false} if the restore process with the specified snapshot name is
     * not running on all nodes.
     */
    public IgniteFuture<Boolean> restoreStatus(String snpName) {
        return executeRestoreManagementTask(SnapshotRestoreStatusTask.class, snpName);
    }

    /** @return {@code True} if disk writes during snapshot process should be in a sequential manner when possible. */
    public boolean sequentialWrite() {
        return sequentialWrite;
    }

    /**
     * @param restoreId Restore process ID.
     * @return Server nodes on which a successful start of the cache(s) is required, if any of these nodes fails when
     *         starting the cache(s), the whole procedure is rolled back.
     */
    public Set<UUID> cacheStartRequiredAliveNodes(@Nullable IgniteUuid restoreId) {
        if (restoreId == null)
            return Collections.emptySet();

        return restoreCacheGrpProc.cacheStartRequiredAliveNodes(restoreId);
    }

    /**
     * @return List of all known snapshots on the local node.
     */
    public List<String> localSnapshotNames(@Nullable String snpPath) {
        if (cctx.kernalContext().clientNode())
            throw new UnsupportedOperationException("Client nodes can not perform this operation.");

        if (locSnpDir == null)
            return Collections.emptyList();

        synchronized (snpOpMux) {
            File[] dirs = (snpPath == null ? locSnpDir : new File(snpPath)).listFiles(File::isDirectory);

            if (dirs == null)
                return Collections.emptyList();

            return Arrays.stream(dirs)
                .map(File::getName)
                .collect(Collectors.toList());
        }
    }

    /**
     * @param snpName Full snapshot name.
     * @param snpPath Snapshot path.
     * @return Maximum existing incremental snapshot index.
     */
    private int maxLocalIncrementSnapshot(String snpName, @Nullable String snpPath) {
        if (cctx.kernalContext().clientNode())
            throw new UnsupportedOperationException("Client and daemon nodes can not perform this operation.");

        synchronized (snpOpMux) {
            File[] incDirs = incrementalSnapshotsLocalRootDir(snpName, snpPath).listFiles(File::isDirectory);

            if (incDirs == null)
                return 0;

            return Arrays.stream(incDirs)
                .map(File::getName)
                .filter(name -> INC_SNP_NAME_PATTERN.matcher(name).matches())
                .mapToInt(Integer::parseInt)
                .max()
                .orElse(0);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> cancelSnapshot(String name) {
        return new IgniteFutureImpl<>(cancelSnapshot0(name).chain(() -> null));
    }

    /**
     * @param name Snapshot name.
     * @return Future which will be completed when cancel operation finished.
     */
    private IgniteInternalFuture<Boolean> cancelSnapshot0(String name) {
        A.notNullOrEmpty(name, "Snapshot name must be not empty or null");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        return cctx.kernalContext().closure()
            .callAsync(
                BROADCAST,
                new CancelSnapshotCallable(null, name),
                options(cctx.discovery().aliveServerNodes()).withFailoverDisabled()
            );
    }

    /**
     * @param reqId Snapshot operation request ID.
     * @return Future which will be completed when cancel operation finished.
     */
    public IgniteFuture<Boolean> cancelSnapshotOperation(UUID reqId) {
        A.notNull(reqId, "Snapshot operation request ID must be not null");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        IgniteInternalFuture<Boolean> fut0 = cctx.kernalContext().closure()
            .callAsync(
                BROADCAST,
                new CancelSnapshotCallable(reqId, null),
                options(cctx.discovery().aliveServerNodes()).withFailoverDisabled()
            );

        return new IgniteFutureImpl<>(fut0);
    }

    /**
     * Cancel running snapshot operation (create/restore).
     *
     * @param reqId Snapshot operation request ID.
     * @return {@code True} if the operation with the specified ID was canceled.
     */
    private boolean cancelLocalSnapshotOperations(UUID reqId) {
        A.notNull(reqId, "Snapshot operation request ID must be not null");

        if (cancelLocalSnapshotTask0(task -> reqId.equals(task.requestId())))
            return true;

        return restoreCacheGrpProc.cancel(reqId, null).get();
    }

    /**
     * @param name Snapshot name to cancel operation on local node.
     * @return {@code True} if the snapshot operation was canceled.
     */
    public boolean cancelLocalSnapshotTask(String name) {
        A.notNullOrEmpty(name, "Snapshot name must be not null or empty");

        return cancelLocalSnapshotTask0(task -> name.equals(task.snapshotName()));
    }

    /**
     * @param filter Snapshot task filter.
     * @return {@code True} if the snapshot operation was canceled.
     */
    private boolean cancelLocalSnapshotTask0(Function<AbstractSnapshotFutureTask<?>, Boolean> filter) {
        ClusterSnapshotFuture fut0 = null;
        boolean canceled = false;

        busyLock.enterBusy();

        try {
            for (AbstractSnapshotFutureTask<?> sctx : locSnpTasks.values()) {
                if (filter.apply(sctx))
                    canceled |= sctx.cancel();
            }

            synchronized (snpOpMux) {
                if (clusterSnpFut != null)
                    fut0 = clusterSnpFut;
            }
        }
        finally {
            busyLock.leaveBusy();
        }

        // Future may be completed with cancelled exception, which is expected.
        try {
            if (fut0 != null)
                fut0.get();
        }
        catch (IgniteCheckedException e) {
            if (e instanceof IgniteFutureCancelledCheckedException) {
                if (log.isInfoEnabled())
                    log.info("Expected cancelled exception: " + e.getMessage());
            }
            else
                throw new IgniteException(e);
        }

        return canceled;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> cancelSnapshotRestore(String name) {
        return new IgniteFutureImpl<>(cancelSnapshot0(name));
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> createDump(String name, @Nullable Collection<String> cacheGrpNames) {
        return createSnapshot(name, null, cacheGrpNames, false, false, true, false, false);
    }

    /**
     * @param name Snapshot name.
     *
     * @return Future that will be finished when process the process is complete. The result of this future will be
     * {@code false} if the restore process with the specified snapshot name is not running at all.
     *
     * @deprecated Use {@link #cancelLocalSnapshotOperations(UUID)} instead.
     */
    @Deprecated
    public IgniteFuture<Boolean> cancelLocalRestoreTask(String name) {
        return restoreCacheGrpProc.cancel(null, name);
    }

    /**
     * Checks snapshot.
     *
     * @param name Snapshot name.
     * @param snpPath Snapshot directory path.
     * @return Future with the result of execution snapshot partitions verify task, which besides calculating partition
     *         hashes of {@link IdleVerifyResultV2} also contains the snapshot metadata distribution across the cluster.
     */
    public IgniteInternalFuture<SnapshotPartitionsVerifyTaskResult> checkSnapshot(String name, @Nullable String snpPath) {
        return checkSnapshot(name, snpPath, -1);
    }

    /**
     * Checks snapshot and its increments.
     *
     * @param name Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param incIdx Incremental snapshot index.
     * @return Future with the result of execution snapshot partitions verify task, which besides calculating partition
     *         hashes of {@link IdleVerifyResultV2} also contains the snapshot metadata distribution across the cluster.
     */
    public IgniteInternalFuture<SnapshotPartitionsVerifyTaskResult> checkSnapshot(String name, @Nullable String snpPath, int incIdx) {
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        return checkSnapshot(name, snpPath, null, false, incIdx, true).chain(f -> {
            try {
                return f.get();
            }
            catch (Throwable t) {
                throw new GridClosureException(t);
            }
        });
    }

    /**
     * The check snapshot procedure performs compute operation over the whole cluster to verify the snapshot
     * entirety and partitions consistency. The result future will be completed with an exception if this
     * exception is not related to the check procedure, and will be completed normally with the {@code IdleVerifyResult}.
     *
     * @param name Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param grps Collection of cache group names to check.
     * @param includeCustomHandlers {@code True} to invoke all user-defined {@link SnapshotHandlerType#RESTORE}
     *                              handlers, otherwise only system consistency check will be performed.
     * @param incIdx Incremental snapshot index.
     * @param check If {@code true} check snapshot integrity.
     * @return Future with the result of execution snapshot partitions verify task, which besides calculating partition
     *         hashes of {@link IdleVerifyResultV2} also contains the snapshot metadata distribution across the cluster.
     */
    public IgniteInternalFuture<SnapshotPartitionsVerifyTaskResult> checkSnapshot(
        String name,
        @Nullable String snpPath,
        @Nullable Collection<String> grps,
        boolean includeCustomHandlers,
        int incIdx,
        boolean check
    ) {
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");
        A.ensure(grps == null || grps.stream().filter(Objects::isNull).collect(Collectors.toSet()).isEmpty(),
            "Collection of cache groups names cannot contain null elements.");

        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> res = new GridFutureAdapter<>();

        if (log.isInfoEnabled()) {
            log.info("The check snapshot procedure started [snpName=" + name + ", snpPath=" + snpPath +
                ", incIdx=" + incIdx + ", grps=" + grps + ']');
        }

        GridKernalContext kctx0 = cctx.kernalContext();

        Collection<ClusterNode> bltNodes = F.view(cctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            (node) -> CU.baselineNode(node, kctx0.state().clusterState()));

        Collection<Integer> grpIds = grps == null ? Collections.emptySet() : F.viewReadOnly(grps, CU::cacheId);

        SnapshotMetadataVerificationTaskArg taskArg = new SnapshotMetadataVerificationTaskArg(name, snpPath, incIdx, grpIds);

        kctx0.task().execute(
            SnapshotMetadataVerificationTask.class,
            taskArg,
            options(bltNodes)
        ).listen(f0 -> {
            SnapshotMetadataVerificationTaskResult metasRes = f0.result();

            if (f0.error() == null && F.isEmpty(metasRes.exceptions())) {
                Map<ClusterNode, List<SnapshotMetadata>> metas = metasRes.meta();

                Class<? extends AbstractSnapshotVerificationTask> cls;

                if (includeCustomHandlers)
                    cls = SnapshotHandlerRestoreTask.class;
                else
                    cls = incIdx > 0 ? IncrementalSnapshotVerificationTask.class : SnapshotPartitionsVerifyTask.class;

                kctx0.task().execute(
                        cls,
                        new SnapshotPartitionsVerifyTaskArg(grps, metas, snpPath, incIdx, check),
                        options(new ArrayList<>(metas.keySet()))
                    ).listen(f1 -> {
                        if (f1.error() == null)
                            res.onDone(f1.result());
                        else if (f1.error() instanceof IgniteSnapshotVerifyException)
                            res.onDone(new SnapshotPartitionsVerifyTaskResult(metas,
                                new IdleVerifyResultV2(((IgniteSnapshotVerifyException)f1.error()).exceptions())));
                        else
                            res.onDone(f1.error());
                    });
            }
            else {
                if (f0.error() == null)
                    res.onDone(new IgniteSnapshotVerifyException(metasRes.exceptions()));
                else if (f0.error() instanceof IgniteSnapshotVerifyException)
                    res.onDone(new SnapshotPartitionsVerifyTaskResult(null,
                        new IdleVerifyResultV2(((IgniteSnapshotVerifyException)f0.error()).exceptions())));
                else
                    res.onDone(f0.error());
            }
        });

        if (log.isInfoEnabled()) {
            res.listen(() -> log.info("The check snapshot procedure finished [snpName=" + name +
                ", snpPath=" + snpPath + ", incIdx=" + incIdx + ", grps=" + grps + ']'));
        }

        return res;
    }

    /**
     * @param snpName Snapshot name.
     * @param folderName The name of a directory for the cache group.
     * @param names Cache group names to filter.
     * @return The list of cache or cache group names in given snapshot on local node.
     */
    public List<File> snapshotCacheDirectories(String snpName, @Nullable String snpPath, String folderName, Predicate<String> names) {
        File snpDir = snapshotLocalDir(snpName, snpPath);

        if (!snpDir.exists())
            return Collections.emptyList();

        return cacheDirectories(new File(snpDir, databaseRelativePath(folderName)), names);
    }

    /**
     * @param snpDir The full path to the snapshot files.
     * @param consId Node consistent id to read metadata for.
     * @return Snapshot metadata instance.
     */
    public SnapshotMetadata readSnapshotMetadata(File snpDir, String consId) throws IgniteCheckedException, IOException {
        return readSnapshotMetadata(new File(snpDir, snapshotMetaFileName(consId)));
    }

    /**
     * @param smf File denoting to snapshot metafile.
     * @return Snapshot metadata instance.
     */
    private SnapshotMetadata readSnapshotMetadata(File smf) throws IgniteCheckedException, IOException {
        SnapshotMetadata meta = readFromFile(smf);

        String smfName = smf.getName().substring(0, smf.getName().length() - SNAPSHOT_METAFILE_EXT.length());

        if (!U.maskForFileName(meta.consistentId()).equals(smfName)) {
            throw new IgniteException(
                "Error reading snapshot metadata [smfName=" + smfName + ", consId=" + U.maskForFileName(meta.consistentId())
            );
        }

        return meta;
    }

    /**
     * @param smf File to read.
     * @return Read metadata.
     * @param <T> Type of metadata.
     */
    public <T> T readFromFile(File smf) throws IgniteCheckedException, IOException {
        if (!smf.exists())
            throw new IgniteCheckedException("Snapshot metafile cannot be read due to it doesn't exist: " + smf);

        try (InputStream in = new BufferedInputStream(Files.newInputStream(smf.toPath()))) {
            return marsh.unmarshal(in, U.resolveClassLoader(cctx.gridConfig()));
        }
    }

    /**
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @return List of snapshot metadata for the given snapshot name on local node.
     * If snapshot has been taken from local node the snapshot metadata for given
     * local node will be placed on the first place.
     */
    public List<SnapshotMetadata> readSnapshotMetadatas(String snpName, @Nullable String snpPath) {
        A.notNullOrEmpty(snpName, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(snpName), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");

        File snpDir = snapshotLocalDir(snpName, snpPath);

        if (!(snpDir.exists() && snpDir.isDirectory()))
            return Collections.emptyList();

        List<File> smfs = new ArrayList<>();

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(snpDir.toPath())) {
            for (Path d : ds) {
                if (Files.isRegularFile(d) && d.getFileName().toString().toLowerCase().endsWith(SNAPSHOT_METAFILE_EXT))
                    smfs.add(d.toFile());
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        if (smfs.isEmpty())
            return Collections.emptyList();

        Map<String, SnapshotMetadata> metasMap = new HashMap<>();
        SnapshotMetadata prev = null;

        try {
            for (File smf : smfs) {
                SnapshotMetadata curr = readSnapshotMetadata(smf);

                if (prev != null && !prev.sameSnapshot(curr)) {
                    throw new IgniteException("Snapshot metadata files are from different snapshots " +
                        "[prev=" + prev + ", curr=" + curr + ']');
                }

                metasMap.put(curr.consistentId(), curr);

                prev = curr;
            }
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException(e);
        }

        SnapshotMetadata currNodeSmf = metasMap.remove(cctx.localNode().consistentId().toString());

        // Snapshot metadata for the local node must be first in the result map.
        if (currNodeSmf == null)
            return new ArrayList<>(metasMap.values());
        else {
            List<SnapshotMetadata> result = new ArrayList<>();

            result.add(currNodeSmf);
            result.addAll(metasMap.values());

            return result;
        }
    }

    /**
     * @param snpName Snapshot name.
     * @return Collection of incremental snapshots metafiles.
     */
    public Collection<IncrementalSnapshotMetadata> readIncrementalSnapshotMetadatas(String snpName) {
        File[] incDirs = incrementalSnapshotsLocalRootDir(snpName, null)
            .listFiles((dir, name) -> INC_SNP_NAME_PATTERN.matcher(name).matches());

        if (incDirs == null)
            return Collections.emptyList();

        List<IncrementalSnapshotMetadata> metas = new ArrayList<>();

        try {
            for (File incDir: incDirs) {
                for (File metaFile: incDir.listFiles((dir, name) -> name.endsWith(SNAPSHOT_METAFILE_EXT)))
                    metas.add(readFromFile(metaFile));
            }
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException(e);
        }

        return metas;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> createSnapshot(String name) {
        return createSnapshot(name, null, false, false);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> createIncrementalSnapshot(String name) {
        return createSnapshot(name, null, true, false);
    }

    /**
     * Create a consistent copy of all persistence cache groups from the whole cluster.
     *
     * @param name Snapshot unique name which satisfies the following name pattern [a-zA-Z0-9_].
     * @param snpPath Snapshot directory path.
     * @param incremental Incremental snapshot flag.
     * @param onlyPrimary If {@code true} snapshot only primary copies of partitions.
     * @return Future which will be completed when a process ends.
     */
    public IgniteFutureImpl<Void> createSnapshot(
        String name,
        @Nullable String snpPath,
        boolean incremental,
        boolean onlyPrimary
    ) {
        return createSnapshot(name, snpPath, null, incremental, onlyPrimary, false, false, false);
    }

    /**
     * Create a consistent copy of all persistence cache groups from the whole cluster.
     * Note, {@code encrypt} flag can be used only for cache dump.
     * Full snapshots store partition files itself.
     * So if cache is encrypted ({@link CacheConfiguration#isEncryptionEnabled()}{@code = true}) then snapshot files will be encrypted.
     * On the other hand, dumps stores only entry data and can be used fo in-memory caches.
     * So we provide an ability to encrypt dump content to protect data on the disk.
     *
     * @param name Snapshot unique name which satisfies the following name pattern [a-zA-Z0-9_].
     * @param snpPath Snapshot directory path.
     * @param cacheGrpNames Cache groups to include in snapshot or {@code null} to include all.
     * @param incremental Incremental snapshot flag.
     * @param onlyPrimary If {@code true} snapshot only primary copies of partitions.
     * @param dump If {@code true} cache dump must be created.
     * @param compress If {@code true} then compress partition files.
     * @param encrypt If {@code true} then content of dump encrypted.
     * @return Future which will be completed when a process ends.
     */
    public IgniteFutureImpl<Void> createSnapshot(
        String name,
        @Nullable String snpPath,
        @Nullable Collection<String> cacheGrpNames,
        boolean incremental,
        boolean onlyPrimary,
        boolean dump,
        boolean compress,
        boolean encrypt
    ) {
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");
        A.ensure(!(incremental && onlyPrimary), "Only primary not supported for incremental snapshots");
        A.ensure(!(dump && incremental), "Incremental dump not supported");
        A.ensure(!(cacheGrpNames != null && !dump), "Cache group names filter supported only for dump");
        A.ensure(!compress || dump, "Compression is supported only for dumps");

        try {
            cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

            if (!IgniteFeatures.allNodesSupports(cctx.discovery().aliveServerNodes(), PERSISTENCE_CACHE_SNAPSHOT))
                throw new IgniteException("Not all nodes in the cluster support a snapshot operation.");

            if (!cctx.kernalContext().state().clusterState().state().active())
                throw new IgniteException("Snapshot operation has been rejected. The cluster is inactive.");

            DiscoveryDataClusterState clusterState = cctx.kernalContext().state().clusterState();

            if (!clusterState.hasBaselineTopology())
                throw new IgniteException("Snapshot operation has been rejected. The baseline topology is not configured for cluster.");

            if (cctx.kernalContext().clientNode()) {
                ClusterNode crd = U.oldest(cctx.kernalContext().discovery().aliveServerNodes(), null);

                if (crd == null)
                    throw new IgniteException("There is no alive server nodes in the cluster");

                return new IgniteSnapshotFutureImpl(cctx.kernalContext().closure()
                    .callAsync(
                        BALANCE,
                        new CreateSnapshotCallable(name, cacheGrpNames, incremental, onlyPrimary, dump, compress, encrypt),
                        options(Collections.singletonList(crd)).withFailoverDisabled()
                    ));
            }

            A.ensure(!encrypt || dump, "Encryption key is supported only for dumps");
            A.ensure(
                !encrypt || cctx.gridConfig().getEncryptionSpi() != null,
                "Encryption SPI must be set to encrypt dump"
            );

            if (!CU.isPersistenceEnabled(cctx.gridConfig()) && !dump) {
                throw new IgniteException("Create snapshot request has been rejected. " +
                    "Snapshots on an in-memory clusters are not allowed.");
            }

            ClusterSnapshotFuture snpFut0;
            int incIdx = -1;

            synchronized (snpOpMux) {
                if (clusterSnpFut != null && !clusterSnpFut.isDone()) {
                    throw new IgniteException(
                        "Create snapshot request has been rejected. The previous snapshot operation was not completed."
                    );
                }

                if (clusterSnpReq != null)
                    throw new IgniteException("Create snapshot request has been rejected. Parallel snapshot processes are not allowed.");

                boolean snpExists = localSnapshotNames(snpPath).contains(name);

                if (!incremental && snpExists) {
                    throw new IgniteException("Create snapshot request has been rejected. " +
                        "Snapshot with given name already exists on local node.");
                }

                if (incremental) {
                    if (!cctx.gridConfig().getDataStorageConfiguration().isWalCompactionEnabled()) {
                        throw new IgniteException("Create incremental snapshot request has been rejected. " +
                            "WAL compaction must be enabled.");
                    }

                    if (!snpExists) {
                        throw new IgniteException("Create incremental snapshot request has been rejected. " +
                                "Base snapshot with given name doesn't exist on local node.");
                    }

                    incIdx = maxLocalIncrementSnapshot(name, snpPath) + 1;
                }

                if (isRestoring()) {
                    throw new IgniteException(
                        "Snapshot operation has been rejected. Cache group restore operation is currently in progress."
                    );
                }

                snpFut0 = new ClusterSnapshotFuture(UUID.randomUUID(), name, incIdx);

                clusterSnpFut = snpFut0;

                if (incremental)
                    lastSeenIncSnpFut = snpFut0;
                else
                    lastSeenSnpFut = snpFut0;
            }

            Set<String> cacheGrpNames0 = cacheGrpNames == null ? null : new HashSet<>(cacheGrpNames);

            List<String> grps = (dump ? cctx.cache().cacheGroupDescriptors().values() : cctx.cache().persistentGroups()).stream()
                .map(CacheGroupDescriptor::cacheOrGroupName)
                .filter(n -> cacheGrpNames0 == null || cacheGrpNames0.remove(n))
                .filter(cacheName -> cctx.cache().cacheType(cacheName) == CacheType.USER)
                .collect(Collectors.toList());

            if (!F.isEmpty(cacheGrpNames0))
                log.warning("Unknown cache groups will not be included in snapshot [grps=" + cacheGrpNames0 + ']');

            if (!dump)
                grps.add(METASTORAGE_CACHE_NAME);
            else if (grps.isEmpty())
                throw new IgniteException("Dump operation has been rejected. No cache group defined in cluster");

            List<ClusterNode> srvNodes = cctx.discovery().serverNodes(AffinityTopologyVersion.NONE);

            snpFut0.listen(() -> {
                if (snpFut0.error() == null)
                    recordSnapshotEvent(name, SNAPSHOT_FINISHED_MSG + grps, EVT_CLUSTER_SNAPSHOT_FINISHED);
                else {
                    String errMsgPref = snpFut0.error() instanceof SnapshotWarningException ? SNAPSHOT_FINISHED_WRN_MSG
                        : SNAPSHOT_FAILED_MSG;

                    recordSnapshotEvent(name, errMsgPref + snpFut0.error().getMessage(), EVT_CLUSTER_SNAPSHOT_FAILED);
                }
            });

            Set<UUID> bltNodeIds =
                new HashSet<>(F.viewReadOnly(srvNodes, F.node2id(), (node) -> CU.baselineNode(node, clusterState)));

            SnapshotOperationRequest snpOpReq = new SnapshotOperationRequest(
                    snpFut0.rqId,
                    cctx.localNodeId(),
                    name,
                    snpPath,
                    grps,
                    bltNodeIds,
                    incremental,
                    incIdx,
                    onlyPrimary,
                    dump,
                    compress,
                    encrypt
            );

            startSnpProc.start(snpFut0.rqId, snpOpReq);

            String msg = SNAPSHOT_STARTED_MSG + snpOpReq;

            recordSnapshotEvent(name, msg, EVT_CLUSTER_SNAPSHOT_STARTED);

            if (log.isInfoEnabled())
                log.info(msg);

            return new IgniteFutureImpl<>(snpFut0);
        }
        catch (Exception e) {
            recordSnapshotEvent(name, SNAPSHOT_FAILED_MSG + e.getMessage(), EVT_CLUSTER_SNAPSHOT_FAILED);

            U.error(log, SNAPSHOT_FAILED_MSG, e);

            ClusterSnapshotFuture errSnpFut = new ClusterSnapshotFuture(name, e);

            if (incremental)
                lastSeenIncSnpFut = errSnpFut;
            else
                lastSeenSnpFut = errSnpFut;

            return new IgniteFinishedFutureImpl<>(e);
        }
    }

    /** Writes a warning message if an incremental snapshot contains atomic caches. */
    void warnAtomicCachesInIncrementalSnapshot(String snpName, int incIdx, Collection<String> cacheGrps) {
        List<String> warnCaches = new ArrayList<>();

        for (String cacheGrp: cacheGrps) {
            CacheGroupContext cgctx = cctx.cache().cacheGroup(CU.cacheId(cacheGrp));

            if (cgctx != null && cgctx.hasAtomicCaches()) {
                for (GridCacheContext<?, ?> c : cgctx.caches()) {
                    CacheConfiguration<?, ?> ccfg = c.config();

                    if (ccfg.getAtomicityMode() == CacheAtomicityMode.ATOMIC && ccfg.getBackups() > 0)
                        warnCaches.add(ccfg.getName());
                }
            }
        }

        if (warnCaches.isEmpty())
            return;

        U.warn(log, "Incremental snapshot [snpName=" + snpName + ", incIdx=" + incIdx + "] contains ATOMIC caches with backups: "
            + warnCaches + ". Please note, incremental snapshots doesn't guarantee consistency of restored atomic caches. " +
            "It is highly recommended to verify these caches after restoring with the \"idle_verify\" command. " +
            "If it is needed it's possible to repair inconsistent partitions with the \"consistency\" command. " +
            "Please, check the \"Control Script\" section of Ignite docs for more information about these commands.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> restoreSnapshot(String name, @Nullable Collection<String> grpNames) {
        return restoreSnapshot(name, null, grpNames, 0, DFLT_CHECK_ON_RESTORE);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> restoreSnapshot(
        String name,
        @Nullable Collection<String> grpNames,
        int incIdx
    ) {
        A.ensure(incIdx > 0, "Incremental snapshot index must be greater than 0.");

        return restoreSnapshot(name, null, grpNames, incIdx, DFLT_CHECK_ON_RESTORE);
    }

    /**
     * Restore cache group(s) from the snapshot.
     *
     * @param name Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param grpNames Cache groups to be restored or {@code null} to restore all cache groups from the snapshot.
     * @return Future which will be completed when restore operation finished.
     */
    public IgniteFutureImpl<Void> restoreSnapshot(String name, @Nullable String snpPath, @Nullable Collection<String> grpNames) {
        return restoreSnapshot(name, snpPath, grpNames, 0, DFLT_CHECK_ON_RESTORE);
    }

    /**
     * Restore cache group(s) from the snapshot.
     *
     * @param name Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param grpNames Cache groups to be restored or {@code null} to restore all cache groups from the snapshot.
     * @param incIdx Index of incremental snapshot.
     * @param check If {@code true} check snapshot before restore.
     * @return Future which will be completed when restore operation finished.
     */
    public IgniteFutureImpl<Void> restoreSnapshot(
        String name,
        @Nullable String snpPath,
        @Nullable Collection<String> grpNames,
        int incIdx,
        boolean check
    ) {
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");
        A.ensure(grpNames == null || !grpNames.isEmpty(), "List of cache group names cannot be empty.");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        return restoreCacheGrpProc.start(name, snpPath, grpNames, incIdx, check);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metaStorage) throws IgniteCheckedException {
        synchronized (snpOpMux) {
            this.metaStorage = metaStorage;

            if (recovered)
                removeLastMetaStorageKey();

            recovered = false;
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metaStorage) throws IgniteCheckedException {
        restoreCacheGrpProc.cleanup();

        // Snapshot which has not been completed due to the local node crashed must be deleted.
        String snpName = (String)metaStorage.read(SNP_RUNNING_KEY);
        String snpDirName = snpName == null ? (String)metaStorage.read(SNP_RUNNING_DIR_KEY) : null;

        File snpDir = snpName != null
            ? snapshotLocalDir(snpName, null)
            : snpDirName != null
                ? new File(snpDirName)
                : null;

        if (snpDir == null)
            return;

        recovered = true;

        for (File tmp : snapshotTmpDir().listFiles())
            U.delete(tmp);

        if (INC_SNP_NAME_PATTERN.matcher(snpDir.getName()).matches() && snpDir.getAbsolutePath().contains(INC_SNP_DIR))
            U.delete(snpDir);
        else
            deleteSnapshot(snpDir, pdsSettings);

        if (log.isInfoEnabled()) {
            log.info("Previous attempt to create snapshot fail due to the local node crash. All resources " +
                "related to snapshot operation have been deleted: " + snpDir.getName());
        }
    }

    /**
     * @param evt Discovery event to check.
     * @return {@code true} if exchange started by snapshot operation.
     */
    public static boolean isSnapshotOperation(DiscoveryEvent evt) {
        return !evt.eventNode().isClient() &&
            evt.type() == EVT_DISCOVERY_CUSTOM_EVT &&
            ((DiscoveryCustomEvent)evt).customMessage() instanceof SnapshotStartDiscoveryMessage;
    }

    /** {@inheritDoc} */
    @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        if (clusterSnpReq == null || cctx.kernalContext().clientNode() || !isSnapshotOperation(fut.firstEvent()))
            return;

        SnapshotOperationRequest snpReq = clusterSnpReq;

        if (snpReq.incremental())
            return;

        AbstractSnapshotFutureTask<?> task = locSnpTasks.get(snpReq.snapshotName());

        if (task == null)
            return;

        if (task.start()) {
            cctx.database().forceNewCheckpoint(String.format("Start snapshot operation: %s", snpReq.snapshotName()), lsnr -> {});

            // Schedule task on a checkpoint and wait when it starts.
            try {
                long start = U.currentTimeMillis();

                ((SnapshotFutureTask)task).started().get();

                if (log.isInfoEnabled()) {
                    log.info("Finished waiting for a synchronized checkpoint under topology lock " +
                        "[snpName=" + task.snapshotName() + ", time=" + (U.currentTimeMillis() - start) + "ms]");
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Fail to wait while cluster-wide snapshot operation started", e);
            }
        }
    }

    /**
     * @param rmtNodeId The remote node to connect to.
     * @param reqId Snapshot operation request ID.
     * @param snpName Snapshot name to request.
     * @param rmtSnpPath Snapshot directory path on the remote node.
     * @param parts Collection of pairs group and appropriate cache partition to be snapshot.
     * @param stopChecker Node stop or process interrupt checker.
     * @param partHnd Received partition handler.
     */
    public IgniteInternalFuture<Void> requestRemoteSnapshotFiles(
        UUID rmtNodeId,
        UUID reqId,
        String snpName,
        @Nullable String rmtSnpPath,
        Map<Integer, Set<Integer>> parts,
        BooleanSupplier stopChecker,
        BiConsumer<@Nullable File, @Nullable Throwable> partHnd
    ) throws IgniteCheckedException {
        assert U.alphanumericUnderscore(snpName) : snpName;
        assert partHnd != null;

        ClusterNode rmtNode = cctx.discovery().node(rmtNodeId);

        if (rmtNode == null) {
            throw new ClusterTopologyCheckedException("Snapshot remote request cannot be performed. " +
                "Remote node left the grid [rmtNodeId=" + rmtNodeId + ']');
        }

        if (!nodeSupports(rmtNode, PERSISTENCE_CACHE_SNAPSHOT))
            throw new IgniteCheckedException("Snapshot on remote node is not supported: " + rmtNode.id());

        RemoteSnapshotFilesRecevier fut =
            new RemoteSnapshotFilesRecevier(this, rmtNodeId, reqId, snpName, rmtSnpPath, parts, stopChecker, partHnd);

        snpRmtMgr.submit(fut);

        return fut;
    }

    /**
     * @param grps List of cache groups which will be destroyed.
     */
    public void onCacheGroupsStopped(List<Integer> grps) {
        Collection<AbstractSnapshotFutureTask<?>> tasks =
            F.view(locSnpTasks.values(), t -> t instanceof SnapshotFutureTask || t instanceof CreateDumpFutureTask);

        for (AbstractSnapshotFutureTask<?> sctx : tasks) {
            Set<Integer> retain = new HashSet<>(grps);

            retain.retainAll(sctx.affectedCacheGroups());

            if (!retain.isEmpty()) {
                sctx.acceptException(new IgniteCheckedException("Snapshot has been interrupted due to some of the required " +
                    "cache groups stopped: " + retain));
            }
        }
    }

    /**
     * @param consId Consistent node id.
     * @return Snapshot metadata file name.
     */
    public static String snapshotMetaFileName(String consId) {
        return U.maskForFileName(consId) + SNAPSHOT_METAFILE_EXT;
    }

    /**
     * @param snpDir The full path to the snapshot files.
     * @param folderName The node folder name, usually it's the same as the U.maskForFileName(consistentId).
     * @return Standalone kernal context related to the snapshot.
     * @throws IgniteCheckedException If fails.
     */
    public StandaloneGridKernalContext createStandaloneKernalContext(
        CompressionProcessor cmpProc,
        File snpDir,
        String folderName
    ) throws IgniteCheckedException {
        return new StandaloneGridKernalContext(log, cmpProc, resolveBinaryWorkDir(snpDir.getAbsolutePath(), folderName),
            resolveMappingFileStoreWorkDir(snpDir.getAbsolutePath()));
    }

    /**
     * @param grpName Cache group name.
     * @param partId Partition id.
     * @param pageStore File page store to iterate over.
     * @return Iterator over partition.
     * @throws IgniteCheckedException If and error occurs.
     */
    public GridCloseableIterator<CacheDataRow> partitionRowIterator(GridKernalContext ctx,
        String grpName,
        int partId,
        FilePageStore pageStore
    ) throws IgniteCheckedException {
        CacheObjectContext coctx = new CacheObjectContext(ctx, grpName, null, false,
            false, false, false, false);

        GridCacheSharedContext<?, ?> sctx = GridCacheSharedContext.builder().build(ctx, null);

        return new DataPageIterator(sctx, coctx, pageStore, partId);
    }

    /**
     * @param snpName Snapshot name.
     * @param folderName The node folder name, usually it's the same as the U.maskForFileName(consistentId).
     * @param grpName Cache group name.
     * @param partId Partition id.
     * @param encrKeyProvider Encryption keys provider to create encrypted IO. If {@code null}, no encrypted IO is used.
     * @return Iterator over partition.
     * @throws IgniteCheckedException If and error occurs.
     */
    public GridCloseableIterator<CacheDataRow> partitionRowIterator(String snpName,
        String folderName,
        String grpName,
        int partId,
        @Nullable EncryptionCacheKeyProvider encrKeyProvider
    ) throws IgniteCheckedException {
        File snpDir = resolveSnapshotDir(snpName, null);

        File nodePath = new File(snpDir, databaseRelativePath(folderName));

        if (!nodePath.exists())
            throw new IgniteCheckedException("Consistent id directory doesn't exists: " + nodePath.getAbsolutePath());

        List<File> grps = cacheDirectories(nodePath, name -> name.equals(grpName));

        if (F.isEmpty(grps)) {
            throw new IgniteCheckedException(
                "The snapshot cache group not found [dir=" + snpDir.getAbsolutePath() + ", grpName=" + grpName + ']'
            );
        }

        if (grps.size() > 1) {
            throw new IgniteCheckedException(
                "The snapshot cache group directory cannot be uniquely identified [dir=" + snpDir.getAbsolutePath() +
                    ", grpName=" + grpName + ']'
            );
        }

        File snpPart = getPartitionFile(new File(snapshotLocalDir(snpName, null), databaseRelativePath(folderName)),
            grps.get(0).getName(), partId);

        int grpId = CU.cacheId(grpName);

        FilePageStore pageStore = (FilePageStore)storeMgr.getPageStoreFactory(grpId,
            encrKeyProvider == null || encrKeyProvider.getActiveKey(grpId) == null ? null : encrKeyProvider).
            createPageStore(getTypeByPartId(partId),
                snpPart::toPath,
                val -> {});

        GridCloseableIterator<CacheDataRow> partIter = partitionRowIterator(cctx.kernalContext(), grpName, partId, pageStore);

        return new GridCloseableIteratorAdapter<CacheDataRow>() {
            /** {@inheritDoc} */
            @Override protected CacheDataRow onNext() throws IgniteCheckedException {
                return partIter.nextX();
            }

            /** {@inheritDoc} */
            @Override protected boolean onHasNext() throws IgniteCheckedException {
                return partIter.hasNextX();
            }

            /** {@inheritDoc} */
            @Override protected void onClose() {
                U.closeQuiet(pageStore);
            }
        };
    }

    /**
     * @param snpName Unique snapshot name.
     * @param snpPath Snapshot path.
     * @param srcNodeId Node id which cause snapshot operation.
     * @param reqId Snapshot operation request ID.
     * @param parts Collection of pairs group and appropriate cache partition to be snapshot.
     * @param withMetaStorage {@code true} if all metastorage data must be also included into snapshot.
     * @param dump {@code true} if cache group dump must be created.
     * @param compress If {@code true} then compress partition files.
     * @param encrypt If {@code true} then content of dump encrypted.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @return Snapshot operation task which should be registered on checkpoint to run.
     */
    AbstractSnapshotFutureTask<?> registerSnapshotTask(
        String snpName,
        @Nullable String snpPath,
        UUID srcNodeId,
        UUID reqId,
        Map<Integer, Set<Integer>> parts,
        boolean withMetaStorage,
        boolean dump,
        boolean compress,
        boolean encrypt,
        SnapshotSender snpSndr
    ) {
        AbstractSnapshotFutureTask<?> task = registerTask(snpName, dump
            ? new CreateDumpFutureTask(cctx,
                srcNodeId,
                reqId,
                snpName,
                snapshotLocalDir(snpName, snpPath),
                ioFactory,
                transferRateLimiter,
                snpSndr,
                parts,
                compress,
                encrypt
            )
            : new SnapshotFutureTask(cctx, srcNodeId, reqId, snpName, tmpWorkDir, ioFactory, snpSndr, parts, withMetaStorage, locBuff));

        if (!withMetaStorage) {
            for (Integer grpId : parts.keySet()) {
                if (!cctx.cache().isEncrypted(grpId))
                    continue;

                task.onDone(new IgniteCheckedException("Snapshot contains encrypted cache group " + grpId +
                    " but doesn't include metastore. Metastore is required because it holds encryption keys " +
                    "required to start with encrypted caches contained in the snapshot."));

                return task;
            }
        }

        return task;
    }

    /**
     * Registers a local snapshot task.
     *
     * @param task Snapshot operation task to be executed.
     * @return Snapshot operation task which should be registered.
     */
    private AbstractSnapshotFutureTask<?> registerTask(String rqId, AbstractSnapshotFutureTask<?> task) {
        if (!busyLock.enterBusy()) {
            return new SnapshotFinishedFutureTask(new IgniteCheckedException("Snapshot manager is stopping [locNodeId=" +
                cctx.localNodeId() + ']'));
        }

        try {
            AbstractSnapshotFutureTask<?> prev = locSnpTasks.putIfAbsent(rqId, task);

            if (prev != null)
                return new SnapshotFinishedFutureTask(new IgniteCheckedException("Snapshot with requested name is already scheduled: " +
                    rqId));

            if (log.isInfoEnabled()) {
                log.info("Snapshot task has been registered on local node [sctx=" + this +
                    ", task=" + task.getClass().getSimpleName() +
                    ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
            }

            task.listen(() -> locSnpTasks.remove(rqId));

            return task;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** @return Current snapshot task. */
    public <T extends AbstractSnapshotFutureTask<?>> T currentSnapshotTask(Class<T> snpTaskCls) {
        SnapshotOperationRequest req = clusterSnpReq;

        if (req == null)
            return null;

        AbstractSnapshotFutureTask<?> task = locSnpTasks.get(req.snapshotName());

        if (task == null || task.getClass() != snpTaskCls)
            return null;

        return (T)task;
    }

    /**
     * @param factory Factory which produces {@link LocalSnapshotSender} implementation.
     */
    void localSnapshotSenderFactory(BiFunction<String, String, SnapshotSender> factory) {
        locSndrFactory = factory;
    }

    /**
     * @return Factory which produces {@link LocalSnapshotSender} implementation.
     */
    BiFunction<String, String, SnapshotSender> localSnapshotSenderFactory() {
        return locSndrFactory;
    }

    /**
     * @param factory Factory which produces {@link RemoteSnapshotSender} implementation.
     */
    void remoteSnapshotSenderFactory(BiFunction<String, UUID, SnapshotSender> factory) {
        rmtSndrFactory = factory;
    }

    /**
     * @param rqId Request id.
     * @param nodeId Node id.
     * @return Snapshot sender related to given node id.
     */
    RemoteSnapshotSender remoteSnapshotSenderFactory(String rqId, UUID nodeId) {
        return new RemoteSnapshotSender(log,
            cctx.kernalContext().pools().getSnapshotExecutorService(),
            cctx.gridIO().openTransmissionSender(nodeId, DFLT_INITIAL_SNAPSHOT_TOPIC),
            rqId);
    }

    /** @param snpLocDir Snapshot local directory. */
    public void writeSnapshotDirectoryToMetastorage(File snpLocDir) {
        cctx.database().checkpointReadLock();

        try {
            assert metaStorage != null && metaStorage.read(SNP_RUNNING_DIR_KEY) == null :
                "The previous snapshot hasn't been completed correctly";

            metaStorage.write(SNP_RUNNING_DIR_KEY, snpLocDir.getAbsolutePath());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
            cctx.database().checkpointReadUnlock();
        }
    }

    /** Snapshot finished successfully or already restored. Key can be removed. */
    private void removeLastMetaStorageKey() throws IgniteCheckedException {
        cctx.database().checkpointReadLock();

        try {
            metaStorage.remove(SNP_RUNNING_DIR_KEY);
            metaStorage.remove(SNP_RUNNING_KEY);
        }
        finally {
            cctx.database().checkpointReadUnlock();
        }
    }

    /** */
    private void removeDumpLock(String dumpName) throws IgniteCheckedException {
        File lock = new File(nodeDumpDirectory(snapshotLocalDir(dumpName, null), cctx), DUMP_LOCK);

        if (!lock.exists())
            return;

        if (!lock.delete())
            throw new IgniteCheckedException("Lock file can't be deleted: " + lock);
    }

    /** */
    public static File nodeDumpDirectory(File dumpDir, GridCacheSharedContext<?, ?> cctx) throws IgniteCheckedException {
        return new File(dumpDir, databaseRelativePath(cctx.kernalContext().pdsFolderResolver().resolveFolders().folderName()));
    }

    /**
     * Disables creation of incremental snapshots for the given cache group.
     *
     * @param metaStorage External metastorage, useful if the flag is set before cluster activation.
     * @param grpId Group ID.
     */
    public void disableIncrementalSnapshotsCreation(MetaStorage metaStorage, int grpId) {
        cctx.database().checkpointReadLock();

        try {
            metaStorage.write(incrementalSnapshotCreationDisabledKey(grpId), true);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to disable incremental snapshot creation for the cache group: " + grpId, e);
        }
        finally {
            cctx.database().checkpointReadUnlock();
        }
    }

    /**
     * Enables creation of incremental snapshots for the given cache groups.
     *
     * @param grpIds Group IDs.
     */
    private void enableIncrementalSnapshotsCreation(Collection<Integer> grpIds) {
        cctx.database().checkpointReadLock();

        try {
            for (int g: grpIds)
                metaStorage.remove(incrementalSnapshotCreationDisabledKey(g));
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to allow incremental snapshot creation for group: " + grpIds, e);
        }
        finally {
            cctx.database().checkpointReadUnlock();
        }
    }

    /**
     * Convert cache group ID to key for {@link #INC_SNP_DISABLED_KEY_PREFIX} metastorage records.
     *
     * @param grpId Group ID.
     * @return Key.
     */
    public static String incrementalSnapshotCreationDisabledKey(int grpId) {
        return INC_SNP_DISABLED_KEY_PREFIX + grpId;
    }

    /**
     * @param snpName Snapshot name event related to.
     * @param msg Event message.
     * @param type Snapshot event type.
     */
    void recordSnapshotEvent(String snpName, String msg, int type) {
        if (!cctx.gridEvents().isRecordable(type) || !cctx.gridEvents().hasListener(type))
            return;

        cctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
            @Override public void run() {
                cctx.gridEvents().record(new SnapshotEvent(cctx.localNode(),
                    msg,
                    snpName,
                    type));
            }
        });
    }

    /**
     * @return The executor used to run snapshot tasks.
     */
    ExecutorService snapshotExecutorService() {
        return cctx.kernalContext().pools().getSnapshotExecutorService();
    }

    /**
     * @param ioFactory Factory to create IO interface over a page stores.
     */
    public void ioFactory(FileIOFactory ioFactory) {
        this.ioFactory = ioFactory;
    }

    /**
     * @return Factory to create IO interface over a page stores.
     */
    public FileIOFactory ioFactory() {
        return ioFactory;
    }

    /**
     * @param nodeId Remote node id on which requests has been registered.
     * @return Snapshot future related to given node id.
     */
    AbstractSnapshotFutureTask<?> lastScheduledSnapshotResponseRemoteTask(UUID nodeId) {
        return locSnpTasks.values().stream()
            .filter(t -> t instanceof SnapshotResponseRemoteFutureTask)
            .filter(t -> t.sourceNodeId().equals(nodeId))
            .findFirst()
            .orElse(null);
    }

    /**
     * @return Relative configured path of persistence data storage directory for the local node.
     * Example: {@code snapshotWorkDir/db/IgniteNodeName0}
     */
    static String databaseRelativePath(String folderName) {
        return Paths.get(DB_DEFAULT_FOLDER, folderName).toString();
    }

    /**
     * @param cfg Ignite configuration.
     * @return Snapshot directory resolved through given configuration.
     */
    public static File resolveSnapshotWorkDirectory(IgniteConfiguration cfg) {
        return resolveSnapshotWorkDirectory(cfg, true);
    }

    /**
     * @param cfg Ignite configuration.
     * @param create If {@code true} then create resolved directory if not exists.
     * @return Snapshot directory resolved through given configuration.
     */
    public static File resolveSnapshotWorkDirectory(IgniteConfiguration cfg, boolean create) {
        try {
            return U.resolveWorkDirectory(cfg.getWorkDirectory() == null ? U.defaultWorkDirectory() : cfg.getWorkDirectory(),
                cfg.getSnapshotPath(), false, create);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param factory Factory to produce FileIO access.
     * @param from Copy from file.
     * @param to Copy data to file.
     * @param length Number of bytes to copy from beginning.
     */
    static void copy(FileIOFactory factory, File from, File to, long length) {
        copy(factory, from, to, length, null);
    }

    /**
     * @param factory Factory to produce FileIO access.
     * @param from Copy from file.
     * @param to Copy data to file.
     * @param length Number of bytes to copy from beginning.
     * @param rateLimiter Transfer rate limiter.
     */
    static void copy(FileIOFactory factory, File from, File to, long length, @Nullable BasicRateLimiter rateLimiter) {
        try (FileIO src = factory.create(from, READ);
             FileChannel dest = new FileOutputStream(to).getChannel()) {
            if (src.size() < length) {
                throw new IgniteException("The source file to copy is not long enough " +
                    "[expected=" + length + ", actual=" + src.size() + ']');
            }

            boolean unlimited = rateLimiter == null || rateLimiter.isUnlimited();
            long written = 0;

            while (written < length) {
                if (unlimited) {
                    written += src.transferTo(written, length - written, dest);

                    continue;
                }

                long blockLen = Math.min(length - written, SNAPSHOT_LIMITED_TRANSFER_BLOCK_SIZE_BYTES);

                rateLimiter.acquire(blockLen);

                long blockWritten = 0;

                do {
                    blockWritten += src.transferTo(written + blockWritten, blockLen - blockWritten, dest);
                }
                while (blockWritten < blockLen);

                written += blockWritten;
            }
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteInterruptedException((InterruptedException)e.getCause());
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param taskCls Snapshot restore operation management task class.
     * @param snpName Snapshot name.
     */
    private IgniteFuture<Boolean> executeRestoreManagementTask(
        Class<? extends ComputeTask<String, Boolean>> taskCls,
        String snpName
    ) {
        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        Collection<ClusterNode> bltNodes = F.view(cctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            (node) -> CU.baselineNode(node, cctx.kernalContext().state().clusterState()));

        return new IgniteFutureImpl<>(cctx.kernalContext().task().execute(
            taskCls,
            snpName,
            options(bltNodes)
        ));
    }

    /**
     * Checks that incremental snapshot can be created for given full snapshot and current cluster state.
     *
     * @param name Full snapshot name.
     * @param snpPath Snapshot path.
     * @param meta Full snapshot metadata.
     */
    private void checkIncrementalCanBeCreated(
        String name,
        @Nullable String snpPath,
        SnapshotMetadata meta
    ) throws IgniteCheckedException, IOException {
        File snpDir = snapshotLocalDir(name, snpPath);

        IgniteWriteAheadLogManager wal = cctx.wal();

        if (wal == null)
            throw new IgniteCheckedException("Create incremental snapshot request has been rejected. WAL must be enabled.");

        File archiveDir = wal.archiveDir();

        if (archiveDir == null)
            throw new IgniteCheckedException("Create incremental snapshot request has been rejected. WAL archive must be enabled.");

        ensureHardLinkAvailable(archiveDir.toPath(), snpDir.toPath());

        Set<String> aliveNodesConsIds = cctx.discovery().aliveServerNodes()
            .stream()
            .map(node -> node.consistentId().toString())
            .collect(Collectors.toSet());

        for (String consId : meta.baselineNodes()) {
            if (!aliveNodesConsIds.contains(consId)) {
                throw new IgniteCheckedException("Create incremental snapshot request has been rejected. " +
                    "Node from full snapshot offline [consistentId=" + consId + ']');
            }
        }

        File rootSnpCachesDir = new File(snpDir, databaseRelativePath(meta.folderName()));

        for (int grpId : meta.cacheGroupIds()) {
            if (grpId == METASTORAGE_CACHE_ID)
                continue;

            if (metaStorage.read(incrementalSnapshotCreationDisabledKey(grpId)) != null) {
                throw new IgniteCheckedException("Create incremental snapshot request has been rejected. " +
                    "WAL was disabled since previous snapshot for cache group [groupId=" + grpId + ']');
            }

            CacheGroupContext gctx = cctx.kernalContext().cache().cacheGroup(grpId);

            if (gctx == null) {
                throw new IgniteCheckedException("Create incremental snapshot request has been rejected. " +
                    "Cache group destroyed [groupId=" + grpId + ']');
            }

            if (gctx.config().isEncryptionEnabled()) {
                throw new IgniteCheckedException("Create incremental snapshot request has been rejected. " +
                    "Encrypted cache groups not supported [groupId=" + grpId + ']');
            }

            List<File> snpCacheDir =
                cacheDirectories(rootSnpCachesDir, grpName -> gctx.cacheOrGroupName().equals(grpName));

            if (snpCacheDir.isEmpty()) {
                throw new IgniteCheckedException("Create incremental snapshot request has been rejected. " +
                    "Cache group directory not found [groupId=" + grpId + ']');
            }

            assert snpCacheDir.size() == 1 : "Single snapshot cache directory must be found";

            for (File snpDataFile : FilePageStoreManager.cacheDataFiles(snpCacheDir.get(0))) {
                StoredCacheData snpCacheData = GridLocalConfigManager.readCacheData(
                    snpDataFile,
                    MarshallerUtils.jdkMarshaller(cctx.kernalContext().igniteInstanceName()),
                    cctx.kernalContext().config()
                );

                byte[] snpCacheDataBytes = Files.readAllBytes(snpDataFile.toPath());

                File nodeDataFile = new File(snpDataFile.getAbsolutePath().replace(
                    rootSnpCachesDir.getAbsolutePath(),
                    pdsSettings.persistentStoreNodePath().getAbsolutePath()
                ));

                if (!nodeDataFile.exists()) {
                    throw new IgniteCheckedException("Create incremental snapshot request has been rejected. " +
                        "Cache destroyed [cacheId=" + snpCacheData.cacheId() +
                        ", cacheName=" + snpCacheData.config().getName() + ']');
                }

                byte[] nodeCacheDataBytes = Files.readAllBytes(nodeDataFile.toPath());

                if (!Arrays.equals(snpCacheDataBytes, nodeCacheDataBytes)) {
                    throw new IgniteCheckedException(
                        cacheChangedException(snpCacheData.cacheId(), snpCacheData.config().getName())
                    );
                }
            }
        }
    }

    /**
     * Throw cache changed exception.
     *
     * @param cacheId Cache id.
     * @param name Cache name.
     */
    public static String cacheChangedException(int cacheId, String name) {
        return "Create incremental snapshot request has been rejected. " +
            "Cache changed [cacheId=" + cacheId + ", cacheName=" + name + ']';
    }

    /** @return Snapshot handlers. */
    protected SnapshotHandlers handlers() {
        return handlers;
    }

    /** @return Current incremental snapshot ID. */
    public @Nullable UUID incrementalSnapshotId() {
        return incSnpId;
    }

    /** Snapshot operation handlers. */
    protected static class SnapshotHandlers {
        /** Snapshot operation handlers. */
        private final Map<SnapshotHandlerType, List<SnapshotHandler<Object>>> handlers = new EnumMap<>(SnapshotHandlerType.class);

        /** Executor service used to invoke handlers in parallel. */
        private ExecutorService execSvc;

        /**
         * @param ctx Kernal context.
         * @param execSvc Executor service used to invoke handlers in parallel.
         */
        private void initialize(GridKernalContext ctx, ExecutorService execSvc) {
            this.execSvc = execSvc;

            // Register system default snapshot integrity check that is used before the restore operation.
            registerHandler(new SnapshotPartitionsVerifyHandler(ctx.cache().context()));

            // Register system default DataStreamer updates check.
            registerHandler(new DataStreamerUpdatesHandler());

            // Register system default page size and counters check that is used at the creation operation.
            registerHandler(new SnapshotPartitionsQuickVerifyHandler(ctx.cache().context()));

            // Register custom handlers.
            SnapshotHandler<Object>[] extHnds = (SnapshotHandler<Object>[])ctx.plugins().extensions(SnapshotHandler.class);

            if (extHnds == null)
                return;

            for (SnapshotHandler<Object> extHnd : extHnds)
                registerHandler(extHnd);
        }

        /**
         * @param type Type of snapshot operation handler.
         * @param ctx Snapshot operation handler context.
         * @return Results from all handlers with the specified type.
         * @throws IgniteCheckedException if parallel execution was failed.
         */
        protected @Nullable Map<String, SnapshotHandlerResult<Object>> invokeAll(
            SnapshotHandlerType type,
            SnapshotHandlerContext ctx
        ) throws IgniteCheckedException {
            List<SnapshotHandler<Object>> handlers = this.handlers.get(type);

            if (F.isEmpty(handlers))
                return null;

            if (handlers.size() == 1) {
                SnapshotHandler<Object> hnd = handlers.get(0);

                return F.asMap(hnd.getClass().getName(), invoke(hnd, ctx));
            }

            return U.doInParallel(
                execSvc,
                handlers,
                hnd -> new T2<>(hnd.getClass().getName(), invoke(hnd, ctx))
            ).stream().collect(Collectors.toMap(T2::getKey, T2::getValue));
        }

        /***
         * @param type Type of snapshot operation handler.
         * @param snpName Snapshot name.
         * @param res Results from all nodes and handlers with the specified type.
         * @param reqNodes Node IDs on which the handlers were executed.
         * @param wrnsHnd A handler of snapshot operation warnings.
         * @throws Exception If failed.
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        protected void completeAll(
            SnapshotHandlerType type,
            String snpName,
            Map<String, List<SnapshotHandlerResult<?>>> res,
            Collection<UUID> reqNodes,
            Consumer<List<String>> wrnsHnd
        ) throws Exception {
            if (res.isEmpty())
                return;

            List<SnapshotHandler<Object>> hnds = handlers.get(type);

            if (hnds == null || hnds.size() != res.size()) {
                throw new IgniteCheckedException("Snapshot handlers configuration mismatch (number of local snapshot " +
                    "handlers differs from the remote one). The current operation will be aborted " +
                    "[locHnds=" + (hnds == null ? "" : F.viewReadOnly(hnds, h -> h.getClass().getName()).toString()) +
                    ", rmtHnds=" + res.keySet() + "].");
            }

            List<String> wrns = new ArrayList<>();

            for (SnapshotHandler hnd : hnds) {
                List<SnapshotHandlerResult<?>> nodesRes = res.get(hnd.getClass().getName());

                if (nodesRes == null || nodesRes.size() < reqNodes.size()) {
                    Set<UUID> missing = new HashSet<>(reqNodes);

                    if (nodesRes != null)
                        missing.removeAll(F.viewReadOnly(nodesRes, r -> r.node().id()));

                    throw new IgniteCheckedException("Snapshot handlers configuration mismatch, " +
                        "\"" + hnd.getClass().getName() + "\" handler is missing on the remote node(s). " +
                        "The current operation will be aborted [missing=" + missing + "].");
                }

                try {
                    hnd.complete(snpName, nodesRes);
                }
                catch (SnapshotWarningException e) {
                    wrns.add(e.getMessage());
                }
            }

            if (!F.isEmpty(wrns))
                wrnsHnd.accept(wrns);
        }

        /**
         * Creates a result by invocation the handler.
         *
         * @param hnd Snapshot operation handler.
         * @param ctx Snapshot operation handler context.
         */
        private SnapshotHandlerResult<Object> invoke(SnapshotHandler<Object> hnd, SnapshotHandlerContext ctx) {
            try {
                return new SnapshotHandlerResult<>(hnd.invoke(ctx), null, ctx.localNode());
            }
            catch (Exception e) {
                U.error(null, "Error invoking snapshot handler", e);

                return new SnapshotHandlerResult<>(null, e, ctx.localNode());
            }
        }

        /** */
        private void registerHandler(SnapshotHandler hnd) {
            handlers.computeIfAbsent(hnd.type(), v -> new ArrayList<>()).add(hnd);
        }
    }

    /**
     * Ves pokrit assertami absolutely ves,
     * PageScan iterator in the ignite core est.
     */
    private static class DataPageIterator extends GridCloseableIteratorAdapter<CacheDataRow> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Page store to iterate over. */
        @GridToStringExclude
        private final PageStore store;

        /** Page store partition id. */
        private final int partId;

        /** Grid cache shared context. */
        private final GridCacheSharedContext<?, ?> sctx;

        /** Cache object context for key/value deserialization. */
        private final CacheObjectContext coctx;

        /** Buffer to read pages. */
        private final ByteBuffer locBuff;

        /** Buffer to read the rest part of fragmented rows. */
        private final ByteBuffer fragmentBuff;

        /** Total pages in the page store. */
        private final int pages;

        /**
         * Data row greater than page size contains with header and tail parts. Such pages with tails contain only part
         * of a cache key-value pair. These pages will be marked and skipped at the first partition iteration and
         * will be processed on the second partition iteration when all the pages with key-value headers defined.
         */
        private final BitSet tailPages;

        /** Pages which already read and must be skipped. */
        private final BitSet readPages;

        /** Batch of rows read through iteration. */
        private final Deque<CacheDataRow> rows = new LinkedList<>();

        /** */
        private final CompressionProcessor compressProc;

        /** {@code true} if the iteration though partition reached its end. */
        private boolean secondScanComplete;

        /**
         * Current partition page index for read. Due to we read the partition twice it
         * can't be greater than 2 * store.size().
         */
        private int currIdx;

        /**
         * During scanning a cache partition presented as {@code PageStore} we must guarantee the following:
         * all the pages of this storage remains unchanged during the Iterator remains opened, the stored data
         * keeps its consistency. We can't read the {@code PageStore} during an ongoing checkpoint over it.
         *
         * @param coctx Cache object context.
         * @param store Page store to read.
         * @param partId Partition id.
         * @throws IgniteCheckedException If fails.
         */
        public DataPageIterator(
            GridCacheSharedContext<?, ?> sctx,
            CacheObjectContext coctx,
            PageStore store,
            int partId
        ) throws IgniteCheckedException {
            this.store = store;
            this.partId = partId;
            this.coctx = coctx;
            this.sctx = sctx;
            compressProc = sctx.kernalContext().compress();

            store.ensure();
            pages = store.pages();
            tailPages = new BitSet(pages);
            readPages = new BitSet(pages);

            locBuff = ByteBuffer.allocateDirect(store.getPageSize())
                .order(ByteOrder.nativeOrder());
            fragmentBuff = ByteBuffer.allocateDirect(store.getPageSize())
                .order(ByteOrder.nativeOrder());
        }

        /** {@inheritDoc */
        @Override protected CacheDataRow onNext() throws IgniteCheckedException {
            if (secondScanComplete && rows.isEmpty())
                throw new NoSuchElementException("[partId=" + partId + ", store=" + store + ", skipPages=" + readPages + ']');

            return rows.poll();
        }

        /** {@inheritDoc */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            if (secondScanComplete && rows.isEmpty())
                return false;

            try {
                for (; currIdx < 2 * pages && rows.isEmpty(); currIdx++) {
                    boolean first = currIdx < pages;
                    int pageIdx = currIdx % pages;

                    if (readPages.get(pageIdx) || (!first && tailPages.get(pageIdx)))
                        continue;

                    if (!readPageFromStore(pageId(partId, FLAG_DATA, pageIdx), locBuff)) {
                        // Skip not FLAG_DATA pages.
                        setBit(readPages, pageIdx);

                        continue;
                    }

                    long pageAddr = bufferAddress(locBuff);
                    DataPageIO io = getPageIO(T_DATA, getVersion(pageAddr));
                    int freeSpace = io.getFreeSpace(pageAddr);
                    int rowsCnt = io.getDirectCount(pageAddr);

                    if (first) {
                        // Skip empty pages.
                        if (rowsCnt == 0) {
                            setBit(readPages, pageIdx);

                            continue;
                        }

                        // There is no difference between a page containing an incomplete DataRow fragment and
                        // the page where DataRow takes up all the free space. There is no dedicated
                        // flag for this case in page header.
                        // During the storage scan we can skip such pages at the first iteration over the partition file,
                        // since all the fragmented pages will be marked by BitSet array we will safely read the others
                        // on the second iteration.
                        if (freeSpace == 0 && rowsCnt == 1) {
                            DataPagePayload payload = io.readPayload(pageAddr, 0, locBuff.capacity());

                            long link = payload.nextLink();

                            if (link != 0)
                                setBit(tailPages, pageIndex(pageId(link)));

                            continue;
                        }
                    }

                    setBit(readPages, pageIdx);

                    for (int itemId = 0; itemId < rowsCnt; itemId++) {
                        DataRow row = new DataRow();

                        row.partition(partId);

                        row.initFromPageBuffer(
                            sctx,
                            coctx,
                            new IgniteThrowableFunction<Long, ByteBuffer>() {
                                @Override public ByteBuffer apply(Long nextPageId) throws IgniteCheckedException {
                                    boolean success = readPageFromStore(nextPageId, fragmentBuff);

                                    assert success : "Only FLAG_DATA pages allowed: " + toDetailString(nextPageId);

                                    // Fragment of page has been read, might be skipped further.
                                    setBit(readPages, pageIndex(nextPageId));

                                    return fragmentBuff;
                                }
                            },
                            locBuff,
                            itemId,
                            false,
                            CacheDataRowAdapter.RowData.FULL,
                            false);

                        rows.add(row);
                    }
                }

                if (currIdx == 2 * pages) {
                    secondScanComplete = true;

                    boolean set = true;

                    for (int j = 0; j < pages; j++)
                        set &= readPages.get(j);

                    assert set : "readPages=" + readPages + ", pages=" + pages;
                }

                return !rows.isEmpty();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteCheckedException("Error during iteration through page store: " + this, e);
            }
        }

        /**
         * @param bitSet BitSet to change bit index.
         * @param idx Index of bit to change.
         */
        private static void setBit(BitSet bitSet, int idx) {
            boolean bit = bitSet.get(idx);

            assert !bit : "Bit with given index already set: " + idx;

            bitSet.set(idx);
        }

        /**
         * @param pageId Page id to read from store.
         * @param buff Buffer to read page into.
         * @return {@code true} if page read with given type flag.
         * @throws IgniteCheckedException If fails.
         */
        private boolean readPageFromStore(long pageId, ByteBuffer buff) throws IgniteCheckedException {
            buff.clear();

            boolean read = store.read(pageId, buff, true);

            assert read : toDetailString(pageId);

            if (PageIO.getCompressionType(buff) != CompressionProcessor.UNCOMPRESSED_PAGE)
                compressProc.decompressPage(buff, store.getPageSize());

            return getType(buff) == flag(pageId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DataPageIterator.class, this, super.toString());
        }
    }

    /** Remote snapshot future which tracks remote snapshot transmission result. */
    private static class RemoteSnapshotFilesRecevier extends GridFutureAdapter<Void> {
        /** Snapshot name to create. */
        private final String reqId = RMT_SNAPSHOT_PREFIX + U.maskForFileName(UUID.randomUUID().toString());

        /** Ignite snapshot manager. */
        private final IgniteSnapshotManager snpMgr;

        /** Initial message to send request. */
        private final SnapshotFilesRequestMessage initMsg;

        /** Remote node id to request snapshot from. */
        private final UUID rmtNodeId;

        /** Process interrupt checker. */
        private final BooleanSupplier stopChecker;

        /** Partition handler given by request initiator. */
        private final BiConsumer<File, Throwable> partHnd;

        /** Temporary working directory for consuming partitions. */
        private final Path dir;

        /** Counter which show how many partitions left to be received. */
        private final AtomicInteger partsLeft = new AtomicInteger(-1);

        /**
         * @param snpMgr Ignite snapshot manager.
         * @param rmtNodeId Remote node to request snapshot from.
         * @param reqId Snapshot operation request ID.
         * @param snpName Snapshot name to request.
         * @param rmtSnpPath Snapshot directory path on the remote node.
         * @param parts Cache group and partitions to request.
         * @param stopChecker Process interrupt checker.
         * @param partHnd Partition handler.
         */
        public RemoteSnapshotFilesRecevier(
            IgniteSnapshotManager snpMgr,
            UUID rmtNodeId,
            UUID reqId,
            String snpName,
            @Nullable String rmtSnpPath,
            Map<Integer, Set<Integer>> parts,
            BooleanSupplier stopChecker,
            BiConsumer<@Nullable File, @Nullable Throwable> partHnd
        ) {
            dir = Paths.get(snpMgr.tmpWorkDir.getAbsolutePath(), this.reqId);
            initMsg = new SnapshotFilesRequestMessage(this.reqId, reqId, snpName, rmtSnpPath, parts);

            this.snpMgr = snpMgr;
            this.rmtNodeId = rmtNodeId;
            this.stopChecker = stopChecker;
            this.partHnd = partHnd;
        }

        /** Initiate handler by sending request message. */
        public synchronized void init() {
            if (isDone())
                return;

            try {
                ClusterNode rmtNode = snpMgr.cctx.discovery().node(rmtNodeId);

                if (rmtNode == null) {
                    throw new ClusterTopologyCheckedException("Snapshot remote request cannot be performed. " +
                        "Remote node left the grid [rmtNodeId=" + rmtNodeId + ']');
                }

                snpMgr.cctx.gridIO().sendOrderedMessage(rmtNode,
                    DFLT_INITIAL_SNAPSHOT_TOPIC,
                    initMsg,
                    SYSTEM_POOL,
                    Long.MAX_VALUE,
                    true);

                if (snpMgr.log.isInfoEnabled()) {
                    snpMgr.log.info("Snapshot request is sent to the remote node [rmtNodeId=" + rmtNodeId +
                        ", snpName=" + initMsg.snapshotName() + ", rqId=" + reqId + ']');
                }
            }
            catch (Throwable t) {
                onDone(t);
            }
        }

        /**
         * @param ex Exception occurred during receiving files.
         */
        public synchronized void acceptException(Throwable ex) {
            if (isDone())
                return;

            try {
                partHnd.accept(null, ex);
            }
            catch (Throwable t) {
                ex.addSuppressed(t);
            }

            onDone(ex);
        }

        /**
         * @param part Received file which needs to be handled.
         */
        public synchronized void acceptFile(File part) {
            if (isDone())
                return;

            if (stopChecker.getAsBoolean()) {
                TransmissionCancelledException err =
                    new TransmissionCancelledException("Future cancelled prior to the all requested partitions processed.");

                acceptException(err);

                throw err;
            }

            try {
                partHnd.accept(part, null);
            }
            catch (IgniteInterruptedException e) {
                throw new TransmissionCancelledException(e.getMessage());
            }

            partsLeft.decrementAndGet();
        }

        /** {@inheritDoc} */
        @Override protected synchronized boolean onDone(@Nullable Void res, @Nullable Throwable err, boolean cancel) {
            U.delete(dir);

            return super.onDone(res, err, cancel);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            RemoteSnapshotFilesRecevier fut = (RemoteSnapshotFilesRecevier)o;

            return Objects.equals(reqId, fut.reqId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return reqId.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoteSnapshotFilesRecevier.class, this);
        }
    }

    /**
     * This manager is responsible for requesting and handling snapshots from a remote node. Each snapshot request
     * processed asynchronously but strictly one by one.
     */
    private class SequentialRemoteSnapshotManager implements TransmissionHandler, GridMessageListener {
        /** A task currently being executed and must be explicitly finished. */
        private volatile RemoteSnapshotFilesRecevier active;

        /** Queue of asynchronous tasks to execute. */
        private final Queue<RemoteSnapshotFilesRecevier> queue = new ConcurrentLinkedDeque<>();

        /** {@code true} if the node is stopping. */
        private boolean stopping;

        /**  @param next New task for scheduling. */
        public void submit(IgniteSnapshotManager.RemoteSnapshotFilesRecevier next) {
            assert next != null;

            synchronized (this) {
                if (stopping) {
                    next.acceptException(new IgniteException(SNP_NODE_STOPPING_ERR_MSG));

                    return;
                }

                if (active != null && !active.isDone()) {
                    queue.offer(next);

                    return;
                }

                active = next;

                active.listen(this::scheduleNext);
            }

            next.init();
        }

        /** Schedule next async receiver. */
        private void scheduleNext() {
            RemoteSnapshotFilesRecevier next = queue.poll();

            while (next != null && next.isDone())
                next = queue.poll();

            if (next == null) {
                active = null;

                return;
            }

            submit(next);
        }

        /** Stopping handler. */
        public void stop() {
            synchronized (this) {
                stopping = true;
            }

            IgniteException ex = new IgniteException(SNP_NODE_STOPPING_ERR_MSG);

            RemoteSnapshotFilesRecevier r;

            while ((r = queue.poll()) != null)
                r.acceptException(ex);

            if (active != null)
                active.acceptException(ex);
        }

        /** @param nodeId A node left the cluster. */
        public void onNodeLeft(UUID nodeId) {
            ClusterTopologyCheckedException ex = new ClusterTopologyCheckedException("The node from which a snapshot has been " +
                "requested left the grid");

            queue.forEach(r -> {
                if (r.stopChecker.getAsBoolean() || r.rmtNodeId.equals(nodeId))
                    r.acceptException(ex);
            });

            RemoteSnapshotFilesRecevier task = active;

            if (task != null && !task.isDone() && (task.stopChecker.getAsBoolean() || task.rmtNodeId.equals(nodeId)))
                task.acceptException(ex);
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (!busyLock.enterBusy())
                return;

            try {
                if (msg instanceof SnapshotFilesRequestMessage) {
                    SnapshotFilesRequestMessage reqMsg0 = (SnapshotFilesRequestMessage)msg;
                    String rqId = reqMsg0.id();
                    String snpName = reqMsg0.snapshotName();

                    try {
                        synchronized (this) {
                            AbstractSnapshotFutureTask<?> task = lastScheduledSnapshotResponseRemoteTask(nodeId);

                            if (task != null) {
                                // Task will also be removed from local map due to the listener on future done.
                                task.cancel();

                                log.info("Snapshot request has been cancelled due to another request received " +
                                    "[prevSnpResp=" + task + ", msg0=" + reqMsg0 + ']');
                            }
                        }

                        AbstractSnapshotFutureTask<?> task = registerTask(rqId,
                            new SnapshotResponseRemoteFutureTask(cctx,
                                nodeId,
                                reqMsg0.requestId(),
                                snpName,
                                reqMsg0.snapshotPath(),
                                rmtSndrFactory.apply(rqId, nodeId),
                                reqMsg0.parts()));

                        task.listen(() -> {
                            if (task.error() == null)
                                return;

                            U.error(log, "Failed to process request of creating a snapshot " +
                                "[from=" + nodeId + ", msg=" + reqMsg0 + ']', task.error());

                            try {
                                cctx.gridIO().sendToCustomTopic(nodeId,
                                    DFLT_INITIAL_SNAPSHOT_TOPIC,
                                    new SnapshotFilesFailureMessage(reqMsg0.id(), task.error().getMessage()),
                                    SYSTEM_POOL);
                            }
                            catch (IgniteCheckedException ex0) {
                                U.error(log, "Fail to send the response message with processing snapshot request " +
                                    "error [request=" + reqMsg0 + ", nodeId=" + nodeId + ']', ex0);
                            }
                        });

                        task.start();
                    }
                    catch (Throwable t) {
                        U.error(log, "Error processing snapshot file request message " +
                            "error [request=" + reqMsg0 + ", nodeId=" + nodeId + ']', t);

                        cctx.gridIO().sendToCustomTopic(nodeId,
                            DFLT_INITIAL_SNAPSHOT_TOPIC,
                            new SnapshotFilesFailureMessage(reqMsg0.id(), t.getMessage()),
                            SYSTEM_POOL);
                    }
                }
                else if (msg instanceof SnapshotFilesFailureMessage) {
                    SnapshotFilesFailureMessage respMsg0 = (SnapshotFilesFailureMessage)msg;

                    RemoteSnapshotFilesRecevier task = active;

                    if (task == null || !task.reqId.equals(respMsg0.id())) {
                        if (log.isInfoEnabled()) {
                            log.info("A stale snapshot response message has been received. Will be ignored " +
                                "[fromNodeId=" + nodeId + ", response=" + respMsg0 + ']');
                        }

                        return;
                    }

                    if (respMsg0.errorMessage() != null) {
                        task.acceptException(new IgniteCheckedException("Request cancelled. The snapshot operation stopped " +
                            "on the remote node with an error: " + respMsg0.errorMessage()));
                    }
                }
            }
            catch (Throwable e) {
                U.error(log, "Processing snapshot request from remote node fails with an error", e);

                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public void onEnd(UUID nodeId) {
            RemoteSnapshotFilesRecevier task = active;

            if (task == null)
                return;

            assert task.partsLeft.get() == 0 : task;
            assert task.rmtNodeId.equals(nodeId);

            if (log.isInfoEnabled()) {
                log.info("Requested snapshot from remote node has been fully received " +
                    "[rqId=" + task.reqId + ", task=" + task + ']');
            }

            task.onDone((Void)null);
        }

        /** {@inheritDoc} */
        @Override public void onException(UUID nodeId, Throwable ex) {
            RemoteSnapshotFilesRecevier task = active;

            if (task == null)
                return;

            assert task.rmtNodeId.equals(nodeId);

            task.acceptException(ex);
        }

        /** {@inheritDoc} */
        @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
            Integer partId = (Integer)fileMeta.params().get(SNP_PART_ID_PARAM);
            String cacheDirName = (String)fileMeta.params().get(SNP_CACHE_DIR_NAME_PARAM);
            String rqId = (String)fileMeta.params().get(RQ_ID_NAME_PARAM);
            Integer partsCnt = (Integer)fileMeta.params().get(SNP_PARTITIONS_CNT);

            RemoteSnapshotFilesRecevier task = active;

            if (task == null || task.isDone() || !task.reqId.equals(rqId)) {
                throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                    "[rqId=" + rqId + ", meta=" + fileMeta + ", task=" + task + ']');
            }

            assert task.reqId.equals(rqId) && task.rmtNodeId.equals(nodeId) :
                "Another transmission in progress [task=" + task + ", nodeId=" + rqId + ']';

            busyLock.enterBusy();

            try {
                task.partsLeft.compareAndSet(-1, partsCnt);

                File cacheDir = FilePageStoreManager.cacheWorkDir(storeMgr.workDir(), cacheDirName);

                File tmpCacheDir = U.resolveWorkDirectory(storeMgr.workDir().getAbsolutePath(),
                    formatTmpDirName(cacheDir).getName(), false);

                return Paths.get(tmpCacheDir.getAbsolutePath(), getPartitionFileName(partId)).toString();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /** {@inheritDoc} */
        @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
            throw new UnsupportedOperationException("Loading file by chunks is not supported: " + nodeId);
        }

        /** {@inheritDoc} */
        @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
            Integer grpId = (Integer)initMeta.params().get(SNP_GRP_ID_PARAM);
            Integer partId = (Integer)initMeta.params().get(SNP_PART_ID_PARAM);
            String rqId = (String)initMeta.params().get(RQ_ID_NAME_PARAM);

            assert grpId != null;
            assert partId != null;
            assert rqId != null;

            RemoteSnapshotFilesRecevier task = active;

            if (task == null || task.isDone() || !task.reqId.equals(rqId)) {
                throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                    "[rqId=" + rqId + ", meta=" + initMeta + ", task=" + task + ']');
            }

            return new Consumer<File>() {
                @Override public void accept(File file) {
                    RemoteSnapshotFilesRecevier task0 = active;

                    if (task0 == null || !task0.equals(task) || task0.isDone()) {
                        throw new TransmissionCancelledException("Snapshot request is cancelled [rqId=" + rqId +
                            ", grpId=" + grpId + ", partId=" + partId + ']');
                    }

                    if (!busyLock.enterBusy())
                        throw new IgniteException(SNP_NODE_STOPPING_ERR_MSG);

                    try {
                        task0.acceptFile(file);
                    }
                    finally {
                        busyLock.leaveBusy();
                    }
                }
            };
        }
    }

    /**
     *
     */
    private static class RemoteSnapshotSender extends SnapshotSender {
        /** The sender which sends files to remote node. */
        private final GridIoManager.TransmissionSender sndr;

        /** Snapshot name. */
        private final String rqId;

        /** The number of cache partition files expected to be processed. */
        private int partsCnt;

        /**
         * @param log Ignite logger.
         * @param sndr File sender instance.
         * @param rqId Snapshot name.
         */
        public RemoteSnapshotSender(
            IgniteLogger log,
            Executor exec,
            GridIoManager.TransmissionSender sndr,
            String rqId
        ) {
            super(log, exec);

            this.sndr = sndr;
            this.rqId = rqId;
        }

        /** {@inheritDoc} */
        @Override protected void init(int partsCnt) {
            this.partsCnt = partsCnt;
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
            try {
                assert part.exists();
                assert len > 0 : "Requested partitions has incorrect file length " +
                    "[pair=" + pair + ", cacheDirName=" + cacheDirName + ']';

                sndr.send(part, 0, len, transmissionParams(rqId, cacheDirName, pair), TransmissionPolicy.FILE);

                if (log.isInfoEnabled()) {
                    log.info("Partition file has been sent [part=" + part.getName() + ", pair=" + pair +
                        ", grpName=" + cacheGroupName(new File(cacheDirName)) + ", length=" + len + ']');
                }
            }
            catch (TransmissionCancelledException e) {
                if (log.isInfoEnabled()) {
                    log.info("Transmission partition file has been interrupted [part=" + part.getName() +
                        ", pair=" + pair + ']');
                }
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                U.error(log, "Error sending partition file [part=" + part.getName() + ", pair=" + pair +
                    ", length=" + len + ']', e);

                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            throw new UnsupportedOperationException("Sending files by chunks of data is not supported: " + delta.getAbsolutePath());
        }

        /**
         * @param cacheDirName Cache directory name.
         * @param pair Cache group id with corresponding partition id.
         * @return Map of params.
         */
        private Map<String, Serializable> transmissionParams(String rqId, String cacheDirName,
            GroupPartitionId pair) {
            Map<String, Serializable> params = new HashMap<>();

            params.put(SNP_GRP_ID_PARAM, pair.getGroupId());
            params.put(SNP_PART_ID_PARAM, pair.getPartitionId());
            params.put(SNP_CACHE_DIR_NAME_PARAM, cacheDirName);
            params.put(RQ_ID_NAME_PARAM, rqId);
            params.put(SNP_PARTITIONS_CNT, partsCnt);

            return params;
        }

        /** {@inheritDoc} */
        @Override public void close0(@Nullable Throwable th) {
            U.closeQuiet(sndr);

            if (th == null) {
                if (log.isInfoEnabled())
                    log.info("The remote snapshot sender closed normally [snpName=" + rqId + ']');
            }
            else {
                U.warn(log, "The remote snapshot sender closed due to an error occurred while processing " +
                    "snapshot operation [snpName=" + rqId + ']', th);
            }
        }
    }

    /**
     * Snapshot sender which writes all data to local directory.
     */
    private class LocalSnapshotSender extends SnapshotSender {
        /** Local snapshot directory. */
        private final File snpLocDir;

        /** Local node snapshot directory calculated on snapshot directory. */
        private File dbDir;

        /** Size of page. */
        private final int pageSize;

        /** Delta iterator factory. */
        private final Factory<File, FileIOFactory, DeltaIterator> deltaIterFactory =
            sequentialWrite() ? DeltaSortedIterator::new : DeltaIterator::new;

        /**
         * @param snpName Snapshot name.
         * @param snpPath Snapshot directory path.
         */
        public LocalSnapshotSender(String snpName, @Nullable String snpPath) {
            super(IgniteSnapshotManager.this.log, cctx.kernalContext().pools().getSnapshotExecutorService());

            snpLocDir = snapshotLocalDir(snpName, snpPath);
            pageSize = cctx.kernalContext().config().getDataStorageConfiguration().getPageSize();
        }

        /** {@inheritDoc} */
        @Override protected void init(int partsCnt) {
            dbDir = new File(snpLocDir, databaseRelativePath(pdsSettings.folderName()));

            if (dbDir.exists()) {
                throw new IgniteException("Snapshot with given name already exists " +
                    "[snpName=" + snpLocDir.getName() + ", absPath=" + dbDir.getAbsolutePath() + ']');
            }

            writeSnapshotDirectoryToMetastorage(snpLocDir);

            try {
                U.ensureDirectory(dbDir, "snapshot work directory for a local snapshot sender", log);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig0(File ccfg, String cacheDirName) {
            assert dbDir != null;

            try {
                File cacheDir = U.resolveWorkDirectory(dbDir.getAbsolutePath(), cacheDirName, false);

                File targetCacheCfg = new File(cacheDir, ccfg.getName());

                copy(ioFactory, ccfg, targetCacheCfg, ccfg.length(), transferRateLimiter);

                StoredCacheData cacheData = locCfgMgr.readCacheData(targetCacheCfg);

                if (cacheData.config().isEncryptionEnabled()) {
                    EncryptionSpi encSpi = cctx.kernalContext().config().getEncryptionSpi();

                    GroupKey gKey = cctx.kernalContext().encryption().getActiveKey(CU.cacheGroupId(cacheData.config()));

                    cacheData.groupKeyEncrypted(new GroupKeyEncrypted(gKey.id(), encSpi.encryptKey(gKey.key())));

                    locCfgMgr.writeCacheData(cacheData, targetCacheCfg);
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta0(List<Map<Integer, MappedName>> mappings) {
            if (mappings == null)
                return;

            try {
                saveMappings(cctx.kernalContext(), mappings, snpLocDir);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta0(Collection<BinaryType> types) {
            if (types == null)
                return;

            cctx.kernalContext().cacheObjects().saveMetadata(types, snpLocDir);
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
            try {
                if (len == 0)
                    return;

                File cacheDir = U.resolveWorkDirectory(dbDir.getAbsolutePath(), cacheDirName, false);

                File snpPart = new File(cacheDir, part.getName());

                if (!snpPart.exists() || snpPart.delete())
                    snpPart.createNewFile();

                copy(ioFactory, part, snpPart, len, transferRateLimiter);

                if (log.isDebugEnabled()) {
                    log.debug("Partition has been snapshot [snapshotDir=" + dbDir.getAbsolutePath() +
                        ", cacheDirName=" + cacheDirName + ", part=" + part.getName() +
                        ", length=" + part.length() + ", snapshot=" + snpPart.getName() + ']');
                }
            }
            catch (IOException | IgniteCheckedException ex) {
                throw new IgniteException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            File snpPart = getPartitionFile(dbDir, cacheDirName, pair.getPartitionId());

            if (log.isDebugEnabled()) {
                log.debug("Start partition snapshot recovery with the given delta page file [part=" + snpPart +
                    ", delta=" + delta + ']');
            }

            boolean encrypted = cctx.cache().isEncrypted(pair.getGroupId());

            FileIOFactory ioFactory = encrypted ? ((FilePageStoreManager)cctx.pageStore())
                .encryptedFileIoFactory(IgniteSnapshotManager.this.ioFactory, pair.getGroupId()) :
                IgniteSnapshotManager.this.ioFactory;

            try (DeltaIterator deltaIter = deltaIterFactory.create(delta, ioFactory);
                 FilePageStore pageStore = (FilePageStore)storeMgr.getPageStoreFactory(pair.getGroupId(), encrypted)
                     .createPageStore(getTypeByPartId(pair.getPartitionId()), snpPart::toPath, v -> {})
            ) {
                pageStore.beginRecover();

                while (deltaIter.hasNext()) {
                    transferRateLimiter.acquire(pageSize);

                    ByteBuffer page = deltaIter.next();
                    long pageId = PageIO.getPageId(page);

                    pageStore.write(pageId, page, 0, false);
                }

                pageStore.finishRecover();
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override protected void close0(@Nullable Throwable th) {
            if (th == null) {
                if (log.isInfoEnabled())
                    log.info("The Local snapshot sender closed. All resources released [dbNodeSnpDir=" + dbDir + ']');
            }
            else {
                deleteSnapshot(snpLocDir, pdsSettings);

                if (log.isDebugEnabled())
                    log.debug("Local snapshot sender closed due to an error occurred: " + th.getMessage());
            }
        }
    }

    /** Delta file iterator. */
    private class DeltaIterator implements Iterator<ByteBuffer>, Closeable {
        /** Delta file. */
        protected final File delta;

        /** Delta file IO. */
        private final FileIO fileIo;

        /** Delta file length. */
        protected final long totalBytes;

        /** */
        protected final int pageSize;

        /** Pages count written to a delta file. */
        protected final int pagesCnt;

        /** */
        protected final ByteBuffer pageBuf;

        /** */
        private long pos;

        /** */
        DeltaIterator(File delta, FileIOFactory ioFactory) throws IOException {
            pageSize = cctx.kernalContext().config().getDataStorageConfiguration().getPageSize();

            this.delta = delta;

            fileIo = ioFactory.create(delta, READ);

            totalBytes = fileIo.size();

            assert totalBytes % pageSize == 0 : "Given file with delta pages has incorrect size: " + totalBytes;

            pagesCnt = (int)(totalBytes / pageSize);

            pageBuf = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return pos < totalBytes;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer next() {
            if (!hasNext())
                throw new NoSuchElementException();

            readPage(pos);

            pos += pageSize;

            return pageBuf;
        }

        /** Reads a page from the delta file from the given position. */
        protected void readPage(long pos) {
            pageBuf.clear();

            try {
                long read = fileIo.readFully(pageBuf, pos);

                assert read == pageBuf.capacity();
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }

            pageBuf.flip();

            if (log.isDebugEnabled()) {
                log.debug("Read page given delta file [path=" + delta.getName() + ", pageId=" +
                    PageIO.getPageId(pageBuf) + ", index=" + PageIdUtils.pageIndex(PageIO.getPageId(pageBuf)) +
                    ", pos=" + pos + ", pagesCnt=" + pagesCnt + ", crcBuff=" +
                    FastCrc.calcCrc(pageBuf, pageBuf.limit()) + ", crcPage=" + PageIO.getCrc(pageBuf) + ']');

                pageBuf.rewind();
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            fileIo.close();
        }
    }

    /**
     * Delta file iterator sorted by page indexes to almost sequential disk writes on apply to a page store.
     */
    class DeltaSortedIterator extends DeltaIterator {
        /** Snapshot delta sort batch size in pages count. */
        public static final int DELTA_SORT_BATCH_SIZE = 500_000;

        /** Delta index file IO. */
        private final FileIO idxIo;

        /** */
        private int id;

        /** */
        private Iterator<Integer> sortedIter;

        /** */
        DeltaSortedIterator(File delta, FileIOFactory ioFactory) throws IOException {
            super(delta, ioFactory);

            File deltaIdx = partDeltaIndexFile(delta);

            idxIo = pagesCnt > 0 ? IgniteSnapshotManager.this.ioFactory.create(deltaIdx, READ) : null;

            assert deltaIdx.length() % 4 /* pageIdx */ == 0 : "Wrong delta index size: " + deltaIdx.length();
            assert deltaIdx.length() / 4 == pagesCnt : "Wrong delta index pages count: " + deltaIdx.length();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            if (sortedIter == null || !sortedIter.hasNext())
                advance();

            return sortedIter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer next() {
            readPage((long)sortedIter.next() * pageSize);

            return pageBuf;
        }

        /** */
        private void advance() {
            TreeMap<Integer, Integer> sorted = new TreeMap<>();

            while (id < pagesCnt && sorted.size() < DELTA_SORT_BATCH_SIZE) {
                pageBuf.clear();

                try {
                    idxIo.readFully(pageBuf);
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }

                pageBuf.flip();

                while (pageBuf.hasRemaining())
                    sorted.put(pageBuf.getInt(), id++);
            }

            sortedIter = sorted.values().iterator();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            super.close();

            U.closeQuiet(idxIo);
        }
    }

    /** */
    private static class SnapshotOperationResponse implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Results of single-node handlers execution. */
        private final Map<String, SnapshotHandlerResult<Object>> hndResults;

        /** Default constructor. */
        public SnapshotOperationResponse() {
            this(null);
        }

        /** @param hndResults Results of single-node handlers execution.  */
        public SnapshotOperationResponse(Map<String, SnapshotHandlerResult<Object>> hndResults) {
            this.hndResults = hndResults;
        }

        /** @return Results of single-node handlers execution. */
        public @Nullable Map<String, SnapshotHandlerResult<Object>> handlerResults() {
            return hndResults;
        }
    }

    /** Snapshot operation start message. */
    private static class SnapshotStartDiscoveryMessage extends InitMessage<SnapshotOperationRequest>
        implements SnapshotDiscoveryMessage {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** */
        private final boolean needExchange;

        /**
         * @param procId Unique process id.
         * @param req Snapshot initial request.
         */
        public SnapshotStartDiscoveryMessage(UUID procId, SnapshotOperationRequest req) {
            super(procId, START_SNAPSHOT, req, req.incremental());

            needExchange = !req.incremental();
        }

        /** {@inheritDoc} */
        @Override public boolean needExchange() {
            return needExchange;
        }

        /** {@inheritDoc} */
        @Override public boolean needAssignPartitions() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SnapshotStartDiscoveryMessage.class, this, super.toString());
        }
    }

    /** */
    public static class ClusterSnapshotFuture extends GridFutureAdapter<Void> {
        /** Unique snapshot request id. */
        final UUID rqId;

        /** Snapshot name. */
        final String name;

        /** Snapshot start time. */
        final long startTime;

        /** Incremental snapshot index. */
        final @Nullable Integer incIdx;

        /** Snapshot finish time. */
        volatile long endTime;

        /** Operation interruption exception. */
        volatile IgniteCheckedException interruptEx;

        /**
         * Default constructor.
         */
        public ClusterSnapshotFuture() {
            onDone();

            rqId = null;
            name = "";
            startTime = 0;
            endTime = 0;
            incIdx = null;
        }

        /**
         * @param name Snapshot name.
         * @param err Error starting snapshot operation.
         */
        public ClusterSnapshotFuture(String name, Exception err) {
            onDone(err);

            this.name = name;
            startTime = U.currentTimeMillis();
            endTime = 0;
            rqId = null;
            incIdx = null;
        }

        /**
         * @param rqId Unique snapshot request id.
         * @param name Snapshot name.
         */
        public ClusterSnapshotFuture(UUID rqId, String name, @Nullable Integer incIdx) {
            this.rqId = rqId;
            this.name = name;
            this.incIdx = incIdx;
            startTime = U.currentTimeMillis();
        }

        /** {@inheritDoc} */
        @Override protected boolean onDone(@Nullable Void res, @Nullable Throwable err, boolean cancel) {
            endTime = U.currentTimeMillis();

            return super.onDone(res, err, cancel);
        }

        /** @return Request ID. */
        public UUID requestId() {
            return rqId;
        }
    }

    /** Start creation of cluster snapshot closure. */
    @GridInternal
    private static class CreateSnapshotCallable implements IgniteCallable<Void> {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Snapshot name. */
        private final String snpName;

        /** Cache group names to include in snapshot. */
        private final @Nullable Collection<String> cacheGrpNames;

        /** Incremental flag. */
        private final boolean incremental;

        /** If {@code true} snapshot only primary copies of partitions. */
        private final boolean onlyPrimary;

        /** If {@code true} create cache dump. */
        private final boolean dump;

        /** If {@code true} then compress partition files. */
        private final boolean comprParts;

        /** If {@code true} then content of dump encrypted. */
        private final boolean encrypt;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * @param snpName Snapshot name.
         * @param cacheGrpNames Cache group names to include in snapshot.
         * @param incremental If {@code true} then incremental snapshot must be created.
         * @param onlyPrimary If {@code true} then only copy of primary partitions will be created.
         * @param dump If {@code true} then cache dump must be created.
         * @param comprParts If {@code true} then compress partition files.
         * @param encrypt If {@code true} then content of dump encrypted.
         */
        public CreateSnapshotCallable(
            String snpName,
            @Nullable Collection<String> cacheGrpNames,
            boolean incremental,
            boolean onlyPrimary,
            boolean dump,
            boolean comprParts,
            boolean encrypt
        ) {
            this.snpName = snpName;
            this.cacheGrpNames = cacheGrpNames;
            this.incremental = incremental;
            this.onlyPrimary = onlyPrimary;
            this.dump = dump;
            this.comprParts = comprParts;
            this.encrypt = encrypt;
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            if (incremental)
                ignite.snapshot().createIncrementalSnapshot(snpName).get();
            else {
                ignite.context().cache().context().snapshotMgr().createSnapshot(
                    snpName,
                    null,
                    cacheGrpNames,
                    false,
                    onlyPrimary,
                    dump,
                    comprParts,
                    encrypt
                ).get();
            }

            return null;
        }
    }

    /** Cancel snapshot operation closure. */
    @GridInternal
    private static class CancelSnapshotCallable implements IgniteCallable<Boolean> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Snapshot name. */
        private final String snpName;

        /** Snapshot operation request ID. */
        private final UUID reqId;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * @param reqId Snapshot operation request ID.
         * @param snpName Snapshot name.
         */
        public CancelSnapshotCallable(UUID reqId, String snpName) {
            this.reqId = reqId;
            this.snpName = snpName;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            if (reqId != null)
                return ignite.context().cache().context().snapshotMgr().cancelLocalSnapshotOperations(reqId);
            else {
                if (ignite.context().cache().context().snapshotMgr().cancelLocalSnapshotTask(snpName))
                    return true;

                return ignite.context().cache().context().snapshotMgr().cancelLocalRestoreTask(snpName).get();
            }
        }
    }

    /** Wrapper of internal checked exceptions. */
    private static class IgniteSnapshotFutureImpl extends IgniteFutureImpl<Void> {
        /** @param fut Internal future. */
        public IgniteSnapshotFutureImpl(IgniteInternalFuture<Void> fut) {
            super(fut);
        }

        /** {@inheritDoc} */
        @Override protected IgniteException convertException(IgniteCheckedException e) {
            if (e instanceof IgniteClientDisconnectedCheckedException)
                return new IgniteException("Client disconnected. Snapshot result is unknown", U.convertException(e));
            else {
                SnapshotWarningException wrn = X.cause(e, SnapshotWarningException.class);

                if (wrn != null)
                    return new IgniteException(wrn.getMessage());

                return new IgniteException("Snapshot has not been created", U.convertException(e));
            }
        }
    }

    /** Factory. */
    @FunctionalInterface
    private interface Factory<E1, E2, R> {
        /** @return An instance of {@link R}. */
        R create(E1 e1, E2 e2) throws IOException;
    }
}
