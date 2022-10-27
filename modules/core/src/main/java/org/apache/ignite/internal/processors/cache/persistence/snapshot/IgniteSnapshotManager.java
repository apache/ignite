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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
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
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.CacheConfiguration;
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
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.GridLocalConfigManager;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutMarker;
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
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.BasicRateLimiter;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.spi.systemview.view.SnapshotView;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
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
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.resolveBinaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirectories;
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
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SKIP_AUTH;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBGRID;
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
 *     <li>Create snapshot of the whole cluster cache groups by triggering PME to achieve consistency.</li>
 * </ul>
 */
public class IgniteSnapshotManager extends GridCacheSharedManagerAdapter
    implements IgniteSnapshot, PartitionsExchangeAware, MetastorageLifecycleListener, IgniteChangeGlobalStateSupport {
    /** File with delta pages suffix. */
    public static final String DELTA_SUFFIX = ".delta";

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

    /** Snapshot metafile extension. */
    public static final String SNAPSHOT_METAFILE_EXT = ".smf";

    /** Prefix for snapshot threads. */
    public static final String SNAPSHOT_RUNNER_THREAD_PREFIX = "snapshot-runner";

    /** Snapshot transfer rate distributed configuration key */
    public static final String SNAPSHOT_TRANSFER_RATE_DMS_KEY = "snapshotTransferRate";

    /** Snapshot transfer rate is unlimited by default. */
    public static final long DFLT_SNAPSHOT_TRANSFER_RATE_BYTES = 0L;

    /** Maximum block size for limited snapshot transfer (64KB by default). */
    public static final int SNAPSHOT_LIMITED_TRANSFER_BLOCK_SIZE_BYTES = 64 * 1024;

    /** Metastorage key to save currently running snapshot directory path. */
    private static final String SNP_RUNNING_DIR_KEY = "snapshot-running-dir";

    /** @deprecated Use {@link #SNP_RUNNING_DIR_KEY} instead. */
    @Deprecated
    private static final String SNP_RUNNING_KEY = "snapshot-running";

    /** Snapshot operation finish log message. */
    private static final String SNAPSHOT_FINISHED_MSG = "Cluster-wide snapshot operation finished successfully: ";

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
    private volatile PdsFolderSettings pdsSettings;

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
    private File tmpWorkDir;

    /** Factory to working with delta as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** File store manager to create page store for restore. */
    private volatile FilePageStoreManager storeMgr;

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

    /** Snapshot operation handlers. */
    private final SnapshotHandlers handlers = new SnapshotHandlers();

    /** Manager to receive responses of remote snapshot requests. */
    private final SequentialRemoteSnapshotManager snpRmtMgr;

    /** Snapshot transfer rate limit in bytes/sec. */
    private final DistributedLongProperty snapshotTransferRate = detachedLongProperty(SNAPSHOT_TRANSFER_RATE_DMS_KEY);

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
            this::processLocalSnapshotEndStageResult);

        marsh = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());

        restoreCacheGrpProc = new SnapshotRestoreProcess(ctx);

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

        if (!CU.isPersistenceEnabled(ctx.config()))
            return;

        assert cctx.pageStore() instanceof FilePageStoreManager;

        storeMgr = (FilePageStoreManager)cctx.pageStore();
        locCfgMgr = cctx.cache().configManager();

        pdsSettings = cctx.kernalContext().pdsFolderResolver().resolveFolders();

        locSnpDir = resolveSnapshotWorkDirectory(ctx.config());
        tmpWorkDir = U.resolveWorkDirectory(storeMgr.workDir().getAbsolutePath(), DFLT_SNAPSHOT_TMP_DIR, true);

        U.ensureDirectory(locSnpDir, "snapshot work directory", log);
        U.ensureDirectory(tmpWorkDir, "temp directory for snapshot creation", log);

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
            SnapshotFutureTask task = currentSnapshotTask();

            return task == null ? -1 : task.totalSize();
        }, "Estimated size of current cluster snapshot in bytes on this node. The value may grow during snapshot creation.");

        mreg.register("CurrentSnapshotProcessedSize", () -> {
            SnapshotFutureTask task = currentSnapshotTask();

            return task == null ? -1 : task.processedSize();
        }, "Processed size of current cluster snapshot in bytes on this node.");

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
            () -> F.flatCollections(F.transform(localSnapshotNames(null), name -> readSnapshotMetadatas(name, null))),
            this::snapshotViewSupplier);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        busyLock.block();

        try {
            restoreCacheGrpProc.interrupt(new NodeStoppingException("Node is stopping."));

            // Try stop all snapshot processing if not yet.
            for (AbstractSnapshotFutureTask<?> sctx : locSnpTasks.values())
                sctx.acceptException(new NodeStoppingException(SNP_NODE_STOPPING_ERR_MSG));

            locSnpTasks.clear();

            snpRmtMgr.stop();

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
     * @param folderName Local node folder name (see {@link U#maskForFileName} with consistent id).
     */
    public void deleteSnapshot(File snpDir, String folderName) {
        if (!snpDir.exists())
            return;

        if (!snpDir.isDirectory())
            return;

        try {
            File binDir = binaryWorkDir(snpDir.getAbsolutePath(), folderName);
            File nodeDbDir = new File(snpDir.getAbsolutePath(), databaseRelativePath(folderName));

            U.delete(binDir);
            U.delete(nodeDbDir);

            File marshDir = mappingFileStoreWorkDir(snpDir.getAbsolutePath());

            // Concurrently traverse the snapshot marshaller directory and delete all files.
            Files.walkFileTree(marshDir.toPath(), new SimpleFileVisitor<Path>() {
                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    U.delete(file);

                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    // Skip files which can be concurrently removed from FileTree.
                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    dir.toFile().delete();

                    if (log.isInfoEnabled() && exc != null)
                        log.info("Marshaller directory cleaned with an exception: " + exc.getMessage());

                    return FileVisitResult.CONTINUE;
                }
            });

            File binMetadataDfltDir = new File(snpDir, DFLT_BINARY_METADATA_PATH);
            File marshallerDfltDir = new File(snpDir, DFLT_MARSHALLER_PATH);

            U.delete(binMetadataDfltDir);
            U.delete(marshallerDfltDir);

            File db = new File(snpDir, DB_DEFAULT_FOLDER);

            if (!db.exists() || F.isEmpty(db.list())) {
                marshDir.delete();
                db.delete();
                U.delete(snpDir);
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
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
     * For example, {@code "work/snapshots/mybackup/increments/node01/0001"}.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param incIdx Increment index.
     * @return Local snapshot directory where snapshot files are located.
     */
    public File incrementalSnapshotLocalDir(String snpName, @Nullable String snpPath, long incIdx) {
        return Paths.get(
            incrementalSnapshotsLocalRootDir(snpName, snpPath).getAbsolutePath(),
            U.fixedLengthNumberName(incIdx, null)
        ).toFile();
    }

    /**
     * Returns root folder for incremental snapshot.
     * For example, {@code "work/snapshots/mybackup/increments/node01"}.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @return Local snapshot directory where snapshot files are located.
     */
    public File incrementalSnapshotsLocalRootDir(String snpName, @Nullable String snpPath) {
        return Paths.get(
            snapshotLocalDir(snpName, snpPath).getAbsolutePath(),
            INC_SNP_DIR,
            pdsSettings.folderName()
        ).toFile();
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
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalSnapshotStartStage(SnapshotOperationRequest req) {
        if (req.incremental())
            return initLocalIncrementalSnapshotStartStage(req);
        else
            return initLocalFullSnapshotStartStage(req);
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when a snapshot has been started.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalFullSnapshotStartStage(SnapshotOperationRequest req) {
        if (cctx.kernalContext().clientNode() ||
            !CU.baselineNode(cctx.localNode(), cctx.kernalContext().state().clusterState()))
            return new GridFinishedFuture<>();

        // Executed inside discovery notifier thread, prior to firing discovery custom event,
        // so it is safe to set new snapshot task inside this method without synchronization.
        if (clusterSnpReq != null) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot operation has been rejected. " +
                "Another snapshot operation in progress [req=" + req + ", curr=" + clusterSnpReq + ']'));
        }

        Set<UUID> leftNodes = new HashSet<>(req.nodes());
        leftNodes.removeAll(F.viewReadOnly(cctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            F.node2id()));

        if (!leftNodes.isEmpty()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Some of baseline nodes left the cluster " +
                "prior to snapshot operation start: " + leftNodes));
        }

        if (!cctx.localNode().isClient() && cctx.kernalContext().encryption().isMasterKeyChangeInProgress()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot operation has been rejected. Master " +
                "key changing process is not finished yet."));
        }

        if (!cctx.localNode().isClient() && cctx.kernalContext().encryption().reencryptionInProgress()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot operation has been rejected. Caches " +
                "re-encryption process is not finished yet."));
        }

        List<Integer> grpIds = new ArrayList<>(F.viewReadOnly(req.groups(), CU::cacheId));

        Set<Integer> leftGrps = new HashSet<>(grpIds);
        leftGrps.removeAll(cctx.cache().cacheGroupDescriptors().keySet());
        boolean withMetaStorage = leftGrps.remove(METASTORAGE_CACHE_ID);

        if (!leftGrps.isEmpty()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Some of requested cache groups doesn't exist " +
                "on the local node [missed=" + leftGrps + ", nodeId=" + cctx.localNodeId() + ']'));
        }

        return initLocalFullSnapshot(req, grpIds, withMetaStorage);
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when a snapshot has been started.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalIncrementalSnapshotStartStage(SnapshotOperationRequest req) {
        // Start ConsistentCut in background immediately on every node (cluster-wide operation).
        IgniteInternalFuture<WALPointer> cutFut = cctx.consistentCutMgr().startLocalCut(req.marker());

        if (clusterSnpReq != null) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot operation has been rejected. " +
                "Another snapshot operation in progress [req=" + req + ", curr=" + clusterSnpReq + ']'));
        }

        clusterSnpReq = req;

        if (cctx.kernalContext().clientNode() ||
            !CU.baselineNode(cctx.localNode(), cctx.kernalContext().state().clusterState()))
            return new GridFinishedFuture<>();

        // Verify baseline nodes.
        SnapshotMetadata meta;

        try {
            meta = readSnapshotMetadata(req);

            checkIncrementCanBeCreated(req.snapshotName(), req.snapshotPath(), meta);

            return initLocalIncrementalSnapshot(req, meta);
        }
        catch (IgniteCheckedException | IOException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param req Request on snapshot creation.
     * @param meta Full snapshot metadata.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalIncrementalSnapshot(
        SnapshotOperationRequest req,
        SnapshotMetadata meta
    ) {
        File incSnpDir = incrementalSnapshotLocalDir(req.snapshotName(), req.snapshotPath(), req.marker().index());

        AbstractSnapshotFutureTask<?> task0 = registerTask(req.snapshotName(), new IncrementalSnapshotFutureTask(
            cctx,
            req.operationalNodeId(),
            req.requestId(),
            meta,
            tmpWorkDir,
            ioFactory
        ));

        if (log.isDebugEnabled()) {
            log.debug("Incremental snapshot operation submitted for execution" +
                "[snpName=" + req.snapshotName() + ", marker=" + req.marker());
        }

        writeSnapshotDirectoryToMetastorage(incSnpDir);

        task0.start();

        return task0.chain(t -> {
            if (t.error() != null)
                throw F.wrap(t.error());

            return new SnapshotOperationResponse();
        });
    }

    /** */
    public void storeSnapshotMetafile(SnapshotOperationRequest req, SnapshotMetadata meta, File incSnpDir, WALPointer highPtr) {
        IncrementalSnapshotMetadata incMeta = new IncrementalSnapshotMetadata(
            req.requestId(),
            req.snapshotName(),
            req.marker().index(),
            cctx.localNode().consistentId().toString(),
            pdsSettings.folderName(),
            highPtr
        );

        writeSnapshotMetafile(
            new File(incSnpDir, incrementalSnapshotMetaFileName(req.marker().index())),
            incMeta
        );
    }

    /** */
    private void storeWalFiles(
        long incIdx,
        File incSnpDir,
        WALPointer lowPtr,
        WALPointer highPtr
    ) throws IgniteCheckedException, IOException {
        // First increment must include low segment, because full snapshot knows nothing about WAL.
        // All other begins from the next segment because lowPtr already saved inside previous increment.
        long lowIdx = lowPtr.index() + (incIdx == 1 ? 0 : 1);
        long highIdx = highPtr.index();

        assert cctx.gridConfig().getDataStorageConfiguration().isWalCompactionEnabled()
            : "WAL Compaction must be enabled";
        assert lowIdx <= highIdx;

        if (log.isInfoEnabled())
            log.info("Waiting for WAL segments compression [lowIdx=" + lowIdx + ", highIdx=" + highIdx + ']');

        cctx.wal().awaitCompacted(highPtr.index());

        if (log.isInfoEnabled()) {
            log.info("Linking WAL segments into incremental snapshot [lowIdx=" + lowIdx + ", " +
                "highIdx=" + highIdx + ']');
        }

        for (; lowIdx <= highIdx; lowIdx++) {
            File seg = cctx.wal().compactedSegment(lowIdx);

            if (!seg.exists())
                throw new IgniteException("WAL segment not found in archive [idx=" + lowIdx + ']');

            Path segLink = incSnpDir.toPath().resolve(seg.getName());

            if (log.isDebugEnabled())
                log.debug("Creaing segment link [path=" + segLink.toAbsolutePath() + ']');

            Files.createLink(segLink, seg.toPath());
        }
    }

    /** */
    private WALPointer lowIncrementalSnapshotPointer(
        SnapshotOperationRequest req,
        SnapshotMetadata meta
    ) throws IgniteCheckedException, IOException {
        if (req.marker().index() == 1)
            return meta.snapshotRecordPointer();
        else {
            long prevIdx = req.marker().index() - 1;

            IncrementalSnapshotMetadata prevIncSnpMeta = readFromFile(new File(
                incrementalSnapshotLocalDir(req.snapshotName(), req.snapshotPath(), prevIdx),
                incrementalSnapshotMetaFileName(prevIdx)
            ));

            return prevIncSnpMeta.cutPointer();
        }
    }

    /**
     * @param req Request
     * @param grpIds Groups.
     * @param withMetaStorage Flag to include metastorage.
     * @return Create snapshot future.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalFullSnapshot(
        SnapshotOperationRequest req,
        List<Integer> grpIds,
        boolean withMetaStorage
    ) {
        Map<Integer, Set<Integer>> parts = new HashMap<>();

        // Prepare collection of pairs group and appropriate cache partition to be snapshot.
        // Cache group context may be 'null' on some nodes e.g. a node filter is set.
        for (Integer grpId : grpIds) {
            if (cctx.cache().cacheGroup(grpId) == null)
                continue;

            parts.put(grpId, null);
        }

        IgniteInternalFuture<?> task0;

        if (parts.isEmpty() && !withMetaStorage)
            task0 = new GridFinishedFuture<>(Collections.emptySet());
        else {
            task0 = registerSnapshotTask(req.snapshotName(),
                req.operationalNodeId(),
                req.requestId(),
                parts,
                withMetaStorage,
                locSndrFactory.apply(req.snapshotName(), req.snapshotPath()));

            if (withMetaStorage && task0 instanceof SnapshotFutureTask) {
                ((DistributedMetaStorageImpl)cctx.kernalContext().distributedMetastorage())
                    .suspend(((SnapshotFutureTask)task0).started());
            }

            clusterSnpReq = req;
        }

        return task0.chain(fut -> {
            if (fut.error() != null)
                throw F.wrap(fut.error());

            try {
                Set<String> blts = req.nodes().stream()
                    .map(n -> cctx.discovery().node(n).consistentId().toString())
                    .collect(Collectors.toSet());

                File snpDir = snapshotLocalDir(req.snapshotName(), req.snapshotPath());

                snpDir.mkdirs();

                SnapshotFutureTaskResult res = (SnapshotFutureTaskResult)fut.result();

                SnapshotMetadata meta = new SnapshotMetadata(req.requestId(),
                    req.snapshotName(),
                    cctx.localNode().consistentId().toString(),
                    pdsSettings.folderName(),
                    cctx.gridConfig().getDataStorageConfiguration().getPageSize(),
                    grpIds,
                    blts,
                    res.parts(),
                    res.snapshotPointer(),
                    cctx.gridConfig().getEncryptionSpi().masterKeyDigest()
                );

                writeSnapshotMetafile(
                    new File(snpDir, snapshotMetaFileName(cctx.localNode().consistentId().toString())),
                    meta
                );

                SnapshotHandlerContext ctx = new SnapshotHandlerContext(meta, req.groups(), cctx.localNode(), snpDir);

                return new SnapshotOperationResponse(handlers.invokeAll(SnapshotHandlerType.CREATE, ctx));
            }
            catch (IgniteCheckedException e) {
                throw F.wrap(e);
            }
        });
    }

    /**
     * @param smf File to write to.
     * @param meta Snapshot meta information.
     */
    private <M extends Serializable> void writeSnapshotMetafile(File smf, M meta) {
        if (smf.exists())
            throw new GridClosureException(new IgniteException("Snapshot metafile must not exist: " + smf.getAbsolutePath()));

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

        log.info("Snapshot metafile has been created: " + smf.getAbsolutePath());
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void processLocalSnapshotStartStageResult(UUID id, Map<UUID, SnapshotOperationResponse> res, Map<UUID, Exception> err) {
        SnapshotOperationRequest snpReq = clusterSnpReq;

        if (snpReq.incremental()) {
            WALPointer finishIncSnpRec = cctx.consistentCutMgr().finishLocalCut();

            IncrementalSnapshotFutureTask task = (IncrementalSnapshotFutureTask)locSnpTasks.get(snpReq.snapshotName());

            if (task == null)
                return;

            if (F.isEmpty(err))
                task.onDone(new IncrementalSnapshotFutureTaskResult(finishIncSnpRec));
            else
                task.onDone(new IgniteCheckedException("Snapshot operation failed. " + err));
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

        for (SnapshotOperationResponse response : res) {
            if (response == null || response.handlerResults() == null)
                continue;

            for (Map.Entry<String, SnapshotHandlerResult<Object>> entry : response.handlerResults().entrySet())
                clusterHndResults.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(entry.getValue());
        }

        if (clusterHndResults.isEmpty())
            return new GridFinishedFuture<>();

        try {
            GridFutureAdapter<Void> resultFut = new GridFutureAdapter<>();

            handlers().execSvc.submit(() -> {
                try {
                    handlers.completeAll(SnapshotHandlerType.CREATE, req.snapshotName(), clusterHndResults, req.nodes());

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

        try {
            if (req.error() != null) {
                snpReq.error(req.error());

                if (req.incremental())
                    return new GridFinishedFuture<>(req.error());
                else
                    deleteSnapshot(snapshotLocalDir(req.snapshotName(), req.snapshotPath()), pdsSettings.folderName());
            }

            if (!req.incremental()) {
                removeLastMetaStorageKey();

                return new GridFinishedFuture<>(new SnapshotOperationResponse());
            }
        }
        catch (Exception e) {
            return new GridFinishedFuture<>(e);
        }

        assert req.incremental();

        IncrementalSnapshotFutureTask task = (IncrementalSnapshotFutureTask)locSnpTasks.get(snpReq.snapshotName());

        return task.chain(f -> {
            File incSnpDir = incrementalSnapshotLocalDir(req.snapshotName(), req.snapshotPath(), req.marker().index());

            if (!incSnpDir.mkdirs())
                throw F.wrap(new IgniteException("Can't create snapshot directory[dir=" + incSnpDir.getAbsolutePath() + ']'));

            try {
                SnapshotMetadata meta = readSnapshotMetadata(req);

                WALPointer incSnpPtr = (f.result()).snapshotPointer();

                storeSnapshotMetafile(req, meta, incSnpDir, incSnpPtr);

                WALPointer lowPtr = lowIncrementalSnapshotPointer(req, meta);

                storeWalFiles(req.marker().index(), incSnpDir, lowPtr, incSnpPtr);

                return new SnapshotOperationResponse();
            }
            catch (IgniteCheckedException | IOException e) {
                throw F.wrap(e);
            }
            finally {
                locSnpTasks.remove(snpReq.snapshotName());
            }
        });
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void processLocalSnapshotEndStageResult(UUID id, Map<UUID, SnapshotOperationResponse> res, Map<UUID, Exception> err) {
        SnapshotOperationRequest snpReq = clusterSnpReq;

        if (snpReq == null || !F.eq(id, snpReq.requestId()))
            return;

        Set<UUID> endFail = new HashSet<>(snpReq.nodes());
        endFail.removeAll(res.keySet());

        clusterSnpReq = null;

        if (snpReq.incremental()) {
            if (!endFail.isEmpty() && snpReq.error() != null)
                U.delete(incrementalSnapshotLocalDir(snpReq.snapshotName(), snpReq.snapshotPath(), snpReq.marker().index()));

            try {
                removeLastMetaStorageKey();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to clear snapshot meta information from MetaStorage.", e);

                if (clusterSnpFut == null)
                    throw new IgniteException(e);
                else
                    snpReq.error(e);
            }
        }

        synchronized (snpOpMux) {
            if (clusterSnpFut != null) {
                if (endFail.isEmpty() && snpReq.error() == null) {
                    clusterSnpFut.onDone();

                    if (log.isInfoEnabled())
                        log.info(SNAPSHOT_FINISHED_MSG + snpReq);
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
            throw new UnsupportedOperationException("Client and daemon nodes can not perform this operation.");

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
    private long maxLocalIncrementSnapshot(String snpName, @Nullable String snpPath) {
        if (cctx.kernalContext().clientNode())
            throw new UnsupportedOperationException("Client and daemon nodes can not perform this operation.");

        synchronized (snpOpMux) {
            File[] incDirs = incrementalSnapshotsLocalRootDir(snpName, snpPath).listFiles(File::isDirectory);

            if (incDirs == null)
                return 0;

            return Arrays.stream(incDirs)
                .map(File::getName)
                .filter(name -> INC_SNP_NAME_PATTERN.matcher(name).matches())
                .mapToLong(Long::parseLong)
                .max()
                .orElse(0);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> cancelSnapshot(String name) {
        return new IgniteFutureImpl<>(cancelSnapshot0(name).chain(f -> null));
    }

    /**
     * @param name Snapshot name.
     * @return Future which will be completed when cancel operation finished.
     */
    private IgniteInternalFuture<Boolean> cancelSnapshot0(String name) {
        A.notNullOrEmpty(name, "Snapshot name must be not empty or null");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        return cctx.kernalContext().closure()
            .callAsyncNoFailover(BROADCAST,
                new CancelSnapshotCallable(null, name),
                cctx.discovery().aliveServerNodes(),
                false,
                0,
                true);
    }

    /**
     * @param reqId Snapshot operation request ID.
     * @return Future which will be completed when cancel operation finished.
     */
    public IgniteFuture<Boolean> cancelSnapshotOperation(UUID reqId) {
        A.notNull(reqId, "Snapshot operation request ID must be not null");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        IgniteInternalFuture<Boolean> fut0 = cctx.kernalContext().closure()
            .callAsyncNoFailover(BROADCAST,
                new CancelSnapshotCallable(reqId, null),
                cctx.discovery().aliveServerNodes(),
                false,
                0,
                true);

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
     * @param name Snapshot name.
     * @param snpPath Snapshot directory path.
     * @return Future with the result of execution snapshot partitions verify task, which besides calculating partition
     *         hashes of {@link IdleVerifyResultV2} also contains the snapshot metadata distribution across the cluster.
     */
    public IgniteInternalFuture<IdleVerifyResultV2> checkSnapshot(String name, @Nullable String snpPath) {
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        return checkSnapshot(name, snpPath, null, false).chain(f -> {
            try {
                return f.get().idleVerifyResult();
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
     * @return Future with the result of execution snapshot partitions verify task, which besides calculating partition
     *         hashes of {@link IdleVerifyResultV2} also contains the snapshot metadata distribution across the cluster.
     */
    public IgniteInternalFuture<SnapshotPartitionsVerifyTaskResult> checkSnapshot(
        String name,
        @Nullable String snpPath,
        @Nullable Collection<String> grps,
        boolean includeCustomHandlers
    ) {
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");
        A.ensure(grps == null || grps.stream().filter(Objects::isNull).collect(Collectors.toSet()).isEmpty(),
            "Collection of cache groups names cannot contain null elements.");

        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> res = new GridFutureAdapter<>();

        GridKernalContext kctx0 = cctx.kernalContext();

        Collection<ClusterNode> bltNodes = F.view(cctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            (node) -> CU.baselineNode(node, kctx0.state().clusterState()));

        kctx0.task().setThreadContext(TC_SKIP_AUTH, true);
        kctx0.task().setThreadContext(TC_SUBGRID, bltNodes);

        SnapshotMetadataCollectorTaskArg taskArg = new SnapshotMetadataCollectorTaskArg(name, snpPath);

        kctx0.task().execute(SnapshotMetadataCollectorTask.class, taskArg).listen(f0 -> {
            if (f0.error() == null) {
                Map<ClusterNode, List<SnapshotMetadata>> metas = f0.result();

                Map<Integer, String> grpIds = grps == null ? Collections.emptyMap() :
                    grps.stream().collect(Collectors.toMap(CU::cacheId, v -> v));

                byte[] masterKeyDigest = kctx0.config().getEncryptionSpi().masterKeyDigest();

                for (List<SnapshotMetadata> nodeMetas : metas.values()) {
                    for (SnapshotMetadata meta : nodeMetas) {
                        byte[] snpMasterKeyDigest = meta.masterKeyDigest();

                        if (masterKeyDigest == null && snpMasterKeyDigest != null) {
                            res.onDone(new SnapshotPartitionsVerifyTaskResult(metas, new IdleVerifyResultV2(
                                Collections.singletonMap(cctx.localNode(), new IllegalArgumentException("Snapshot '" +
                                    meta.snapshotName() + "' has encrypted caches while encryption is disabled. To " +
                                    "restore this snapshot, start Ignite with configured encryption and the same " +
                                    "master key.")))));

                            return;
                        }

                        if (snpMasterKeyDigest != null && !Arrays.equals(snpMasterKeyDigest, masterKeyDigest)) {
                            res.onDone(new SnapshotPartitionsVerifyTaskResult(metas, new IdleVerifyResultV2(
                                Collections.singletonMap(cctx.localNode(), new IllegalArgumentException("Snapshot '" +
                                    meta.snapshotName() + "' has different master key digest. To restore this " +
                                    "snapshot, start Ignite with the same master key.")))));

                            return;
                        }

                        grpIds.keySet().removeAll(meta.partitions().keySet());
                    }
                }

                if (!grpIds.isEmpty()) {
                    res.onDone(new SnapshotPartitionsVerifyTaskResult(metas,
                        new IdleVerifyResultV2(Collections.singletonMap(cctx.localNode(),
                            new IllegalArgumentException("Cache group(s) was not " +
                                "found in the snapshot [groups=" + grpIds.values() + ", snapshot=" + name + ']')))));

                    return;
                }

                if (metas.isEmpty()) {
                    res.onDone(new SnapshotPartitionsVerifyTaskResult(metas,
                        new IdleVerifyResultV2(Collections.singletonMap(cctx.localNode(),
                            new IllegalArgumentException("Snapshot does not exists [snapshot=" + name +
                                (snpPath != null ? ", baseDir=" + snpPath : "") + ']')))));

                    return;
                }

                kctx0.task().setThreadContext(TC_SKIP_AUTH, true);
                kctx0.task().setThreadContext(TC_SUBGRID, new ArrayList<>(metas.keySet()));

                Class<? extends AbstractSnapshotVerificationTask> cls =
                    includeCustomHandlers ? SnapshotHandlerRestoreTask.class : SnapshotPartitionsVerifyTask.class;

                kctx0.task().execute(cls, new SnapshotPartitionsVerifyTaskArg(grps, metas, snpPath))
                    .listen(f1 -> {
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
                if (f0.error() instanceof IgniteSnapshotVerifyException)
                    res.onDone(new SnapshotPartitionsVerifyTaskResult(null,
                        new IdleVerifyResultV2(((IgniteSnapshotVerifyException)f0.error()).exceptions())));
                else
                    res.onDone(f0.error());
            }
        });

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

    /** */
    private SnapshotMetadata readSnapshotMetadata(SnapshotOperationRequest req) throws IgniteCheckedException, IOException {
        return readSnapshotMetadata(new File(
            snapshotLocalDir(req.snapshotName(), req.snapshotPath()),
            snapshotMetaFileName(cctx.localNode().consistentId().toString())
        ));
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
    private <T> T readFromFile(File smf) throws IgniteCheckedException, IOException {
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

                if (prev != null && !prev.sameSnapshot(curr))
                    throw new IgniteException("Snapshot metadata files are from different snapshots [prev=" + prev + ", curr=" + curr);

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

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> createSnapshot(String name) {
        return createSnapshot(name, null, false);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> createIncrementalSnapshot(String name) {
        return createSnapshot(name, null, true);
    }

    /**
     * Create a consistent copy of all persistence cache groups from the whole cluster.
     *
     * @param name Snapshot unique name which satisfies the following name pattern [a-zA-Z0-9_].
     * @param snpPath Snapshot directory path.
     * @return Future which will be completed when a process ends.
     */
    public IgniteFutureImpl<Void> createSnapshot(String name, @Nullable String snpPath, boolean incremental) {
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");

        try {
            cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

            if (!IgniteFeatures.allNodesSupports(cctx.discovery().aliveServerNodes(), PERSISTENCE_CACHE_SNAPSHOT))
                throw new IgniteException("Not all nodes in the cluster support a snapshot operation.");

            if (!CU.isPersistenceEnabled(cctx.gridConfig())) {
                throw new IgniteException("Create snapshot request has been rejected. Snapshots on an in-memory " +
                    "clusters are not allowed.");
            }

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
                    .callAsyncNoFailover(BALANCE,
                        new CreateSnapshotCallable(name, incremental),
                        Collections.singletonList(crd),
                        false,
                        0,
                        true));
            }

            ClusterSnapshotFuture snpFut0;

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
                }

                if (isRestoring()) {
                    throw new IgniteException(
                        "Snapshot operation has been rejected. Cache group restore operation is currently in progress."
                    );
                }

                snpFut0 = new ClusterSnapshotFuture(UUID.randomUUID(), name);

                clusterSnpFut = snpFut0;
                lastSeenSnpFut = snpFut0;
            }

            List<String> grps = cctx.cache().persistentGroups().stream()
                .filter(g -> cctx.cache().cacheType(g.cacheOrGroupName()) == CacheType.USER)
                .map(CacheGroupDescriptor::cacheOrGroupName)
                .collect(Collectors.toList());

            grps.add(METASTORAGE_CACHE_NAME);

            List<ClusterNode> srvNodes = cctx.discovery().serverNodes(AffinityTopologyVersion.NONE);

            snpFut0.listen(f -> {
                if (f.error() == null)
                    recordSnapshotEvent(name, SNAPSHOT_FINISHED_MSG + grps, EVT_CLUSTER_SNAPSHOT_FINISHED);
                else
                    recordSnapshotEvent(name, SNAPSHOT_FAILED_MSG + f.error().getMessage(), EVT_CLUSTER_SNAPSHOT_FAILED);
            });

            Set<UUID> bltNodeIds =
                new HashSet<>(F.viewReadOnly(srvNodes, F.node2id(), (node) -> CU.baselineNode(node, clusterState)));

            ConsistentCutMarker marker = null;

            if (incremental) {
                long incIdx = maxLocalIncrementSnapshot(name, snpPath) + 1;

                marker = new ConsistentCutMarker(incIdx, snpFut0.rqId);
            }

            startSnpProc.start(snpFut0.rqId, new SnapshotOperationRequest(
                snpFut0.rqId,
                cctx.localNodeId(),
                name,
                snpPath,
                marker,
                grps,
                bltNodeIds
            ));

            String msg =
                "Cluster-wide snapshot operation started [snpName=" + name + ", grps=" + grps +
                    (incremental ? "" : (", incremental=true, marker=" + marker)) +
                ']';

            recordSnapshotEvent(name, msg, EVT_CLUSTER_SNAPSHOT_STARTED);

            if (log.isInfoEnabled())
                log.info(msg);

            return new IgniteFutureImpl<>(snpFut0);
        }
        catch (Exception e) {
            recordSnapshotEvent(name, SNAPSHOT_FAILED_MSG + e.getMessage(), EVT_CLUSTER_SNAPSHOT_FAILED);

            U.error(log, SNAPSHOT_FAILED_MSG, e);

            lastSeenSnpFut = new ClusterSnapshotFuture(name, e);

            return new IgniteFinishedFutureImpl<>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> restoreSnapshot(String name, @Nullable Collection<String> grpNames) {
        return restoreSnapshot(name, null, grpNames);
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
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");
        A.ensure(grpNames == null || !grpNames.isEmpty(), "List of cache group names cannot be empty.");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        return restoreCacheGrpProc.start(name, snpPath, grpNames);
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
            deleteSnapshot(snpDir, pdsSettings.folderName());

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
        if (clusterSnpReq == null || cctx.kernalContext().clientNode())
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
     * @param stopChecker Node stop or prcoess interrupt checker.
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
        // TODO: Check also IncrementalSnapshotFutureTask
        for (AbstractSnapshotFutureTask<?> sctx : F.view(locSnpTasks.values(), t -> t instanceof SnapshotFutureTask)) {
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
    private static String snapshotMetaFileName(String consId) {
        return U.maskForFileName(consId) + SNAPSHOT_METAFILE_EXT;
    }

    /**
     * @param incIdx Increment index.
     * @return Snapshot metadata file name.
     */
    public static String incrementalSnapshotMetaFileName(long incIdx) {
        return U.fixedLengthNumberName(incIdx, SNAPSHOT_METAFILE_EXT);
    }

    /**
     * @param snpDir The full path to the snapshot files.
     * @param folderName The node folder name, usually it's the same as the U.maskForFileName(consistentId).
     * @return Standalone kernal context related to the snapshot.
     * @throws IgniteCheckedException If fails.
     */
    public StandaloneGridKernalContext createStandaloneKernalContext(File snpDir, String folderName) throws IgniteCheckedException {
        return new StandaloneGridKernalContext(log,
            resolveBinaryWorkDir(snpDir.getAbsolutePath(), folderName),
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

        GridCacheSharedContext<?, ?> sctx = new GridCacheSharedContext<>(ctx, null, null, null,
            null, null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null, null, null);

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
     * @param srcNodeId Node id which cause snapshot operation.
     * @param requestId Snapshot operation request ID.
     * @param parts Collection of pairs group and appropriate cache partition to be snapshot.
     * @param withMetaStorage {@code true} if all metastorage data must be also included into snapshot.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @return Snapshot operation task which should be registered on checkpoint to run.
     */
    AbstractSnapshotFutureTask<?> registerSnapshotTask(
        String snpName,
        UUID srcNodeId,
        UUID requestId,
        Map<Integer, Set<Integer>> parts,
        boolean withMetaStorage,
        SnapshotSender snpSndr
    ) {
        AbstractSnapshotFutureTask<?> task = registerTask(snpName, new SnapshotFutureTask(cctx, srcNodeId, requestId,
            snpName, tmpWorkDir, ioFactory, snpSndr, parts, withMetaStorage, locBuff));

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
            if (locSnpTasks.containsKey(rqId)) {
                return new SnapshotFinishedFutureTask(new IgniteCheckedException("Snapshot with requested name is already scheduled: " +
                    rqId));
            }

            AbstractSnapshotFutureTask<?> prev = locSnpTasks.putIfAbsent(rqId, task);

            if (prev != null)
                return new SnapshotFinishedFutureTask(new IgniteCheckedException("Snapshot with requested name is already scheduled: " +
                    rqId));

            if (log.isInfoEnabled()) {
                log.info("Snapshot task has been registered on local node [sctx=" + this +
                    ", task=" + task.getClass().getSimpleName() +
                    ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
            }

            if (!(task instanceof IncrementalSnapshotFutureTask))
                task.listen(f -> locSnpTasks.remove(rqId));

            return task;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** @return Current snapshot task. */
    private SnapshotFutureTask currentSnapshotTask() {
        SnapshotOperationRequest req = clusterSnpReq;

        if (req == null)
            return null;

        AbstractSnapshotFutureTask<?> task = locSnpTasks.get(req.snapshotName());

        // TODO: for incremental?
        if (!(task instanceof SnapshotFutureTask))
            return null;

        return (SnapshotFutureTask)task;
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
        try {
            return U.resolveWorkDirectory(cfg.getWorkDirectory() == null ? U.defaultWorkDirectory() : cfg.getWorkDirectory(),
                cfg.getSnapshotPath(), false);
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

        cctx.kernalContext().task().setThreadContext(TC_SKIP_AUTH, true);
        cctx.kernalContext().task().setThreadContext(TC_SUBGRID, bltNodes);

        return new IgniteFutureImpl<>(cctx.kernalContext().task().execute(taskCls, snpName));
    }

    /**
     * Checks that incremental snapshot can be created for given full snapshot and current cluster state.
     *
     * @param name Full snapshot name.
     * @param snpPath Snapshot path.
     * @param meta Full snapshot metadata.
     */
    private void checkIncrementCanBeCreated(
        String name,
        @Nullable String snpPath,
        SnapshotMetadata meta
    ) throws IgniteCheckedException, IOException {
        File snpDir = snapshotLocalDir(name, snpPath);

        IgniteWriteAheadLogManager wal = cctx.wal();

        if (wal == null) {
            throw new IgniteCheckedException("Create incremental snapshot request has been rejected. " +
                "WAL must be eanbled."
            );
        }

        File archiveDir = wal.archiveDir();

        if (archiveDir == null) {
            throw new IgniteCheckedException("Create incremental snapshot request has been rejected. " +
                "WAL archive must be eanbled."
            );
        }

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

    /**
     * @param meta Snapshot metadata.
     * @return Snapshot view.
     */
    private SnapshotView snapshotViewSupplier(SnapshotMetadata meta) {
        List<File> dirs = snapshotCacheDirectories(meta.snapshotName(), null, meta.folderName(), name -> true);
        Collection<String> cacheGrps = F.viewReadOnly(dirs, FilePageStoreManager::cacheGroupName);

        return new SnapshotView(meta, cacheGrps);
    }

    /** @return Snapshot handlers. */
    protected SnapshotHandlers handlers() {
        return handlers;
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
            SnapshotHandler<?> sysCheck = new SnapshotPartitionsVerifyHandler(ctx.cache().context());
            handlers.put(sysCheck.type(), new ArrayList<>(F.asList((SnapshotHandler<Object>)sysCheck)));

            // Register custom handlers.
            SnapshotHandler<Object>[] extHnds = (SnapshotHandler<Object>[])ctx.plugins().extensions(SnapshotHandler.class);

            if (extHnds == null)
                return;

            for (SnapshotHandler<Object> extHnd : extHnds)
                handlers.computeIfAbsent(extHnd.type(), v -> new ArrayList<>()).add(extHnd);
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
         * @throws Exception If failed.
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        protected void completeAll(
            SnapshotHandlerType type,
            String snpName,
            Map<String, List<SnapshotHandlerResult<?>>> res,
            Collection<UUID> reqNodes
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

                hnd.complete(snpName, nodesRes);
            }
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

            if (stopChecker.getAsBoolean())
                throw new TransmissionCancelledException("Future cancelled prior to the all requested partitions processed.");

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

            RemoteSnapshotFilesRecevier future = (RemoteSnapshotFilesRecevier)o;

            return Objects.equals(reqId, future.reqId);
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

        /**
         * @param next New task for scheduling.
         */
        public synchronized void submit(IgniteSnapshotManager.RemoteSnapshotFilesRecevier next) {
            assert next != null;

            if (stopping) {
                next.acceptException(new IgniteException(SNP_NODE_STOPPING_ERR_MSG));

                return;
            }

            RemoteSnapshotFilesRecevier curr = active;

            if (curr == null || curr.isDone()) {
                next.listen(f -> scheduleNext());

                active = next;

                next.init();
            }
            else
                queue.offer(next);
        }

        /** Schedule next async receiver. */
        private synchronized void scheduleNext() {
            RemoteSnapshotFilesRecevier next = queue.poll();

            if (next == null)
                return;

            submit(next);
        }

        /** Stopping handler. */
        public synchronized void stop() {
            stopping = true;

            if (active != null)
                active.acceptException(new IgniteException(SNP_NODE_STOPPING_ERR_MSG));

            RemoteSnapshotFilesRecevier r;

            while ((r = queue.poll()) != null)
                r.acceptException(new IgniteException(SNP_NODE_STOPPING_ERR_MSG));

            Set<RemoteSnapshotFilesRecevier> futs = activeTasks();
            GridCompoundFuture<Void, Void> stopFut = new GridCompoundFuture<>();

            try {
                for (IgniteInternalFuture<Void> fut : futs)
                    stopFut.add(fut);

                stopFut.markInitialized().get();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @param nodeId A node left the cluster.
         */
        public void onNodeLeft(UUID nodeId) {
            Set<RemoteSnapshotFilesRecevier> futs = activeTasks();
            ClusterTopologyCheckedException ex = new ClusterTopologyCheckedException("The node from which a snapshot has been " +
                "requested left the grid");

            futs.forEach(t -> {
                if (t.rmtNodeId.equals(nodeId))
                    t.acceptException(ex);
            });
        }

        /**
         * @return The set of currently scheduled tasks, some of them may be already completed.
         */
        private Set<RemoteSnapshotFilesRecevier> activeTasks() {
            Set<RemoteSnapshotFilesRecevier> futs = new HashSet<>(queue);

            RemoteSnapshotFilesRecevier active0 = active;

            if (active0 != null)
                futs.add(active0);

            return futs;
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
                                tmpWorkDir,
                                ioFactory,
                                rmtSndrFactory.apply(rqId, nodeId),
                                reqMsg0.parts()));

                        task.listen(f -> {
                            if (f.error() == null)
                                return;

                            U.error(log, "Failed to process request of creating a snapshot " +
                                "[from=" + nodeId + ", msg=" + reqMsg0 + ']', f.error());

                            try {
                                cctx.gridIO().sendToCustomTopic(nodeId,
                                    DFLT_INITIAL_SNAPSHOT_TOPIC,
                                    new SnapshotFilesFailureMessage(reqMsg0.id(), f.error().getMessage()),
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
                    log.info("Partition file has been send [part=" + part.getName() + ", pair=" + pair +
                        ", length=" + len + ']');
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

            try (FileIO fileIo = ioFactory.create(delta, READ);
                 FilePageStore pageStore = (FilePageStore)storeMgr.getPageStoreFactory(pair.getGroupId(), encrypted)
                     .createPageStore(getTypeByPartId(pair.getPartitionId()), snpPart::toPath, v -> {})
            ) {
                ByteBuffer pageBuf = ByteBuffer.allocate(pageSize)
                    .order(ByteOrder.nativeOrder());

                long totalBytes = fileIo.size();

                assert totalBytes % pageSize == 0 : "Given file with delta pages has incorrect size: " + fileIo.size();

                pageStore.beginRecover();

                for (long pos = 0; pos < totalBytes; pos += pageSize) {
                    long read = fileIo.readFully(pageBuf, pos);

                    assert read == pageBuf.capacity();

                    pageBuf.flip();

                    if (log.isDebugEnabled()) {
                        log.debug("Read page given delta file [path=" + delta.getName() +
                            ", pageId=" + PageIO.getPageId(pageBuf) + ", pos=" + pos + ", pages=" + (totalBytes / pageSize) +
                            ", crcBuff=" + FastCrc.calcCrc(pageBuf, pageBuf.limit()) + ", crcPage=" + PageIO.getCrc(pageBuf) + ']');

                        pageBuf.rewind();
                    }

                    transferRateLimiter.acquire(pageSize);

                    pageStore.write(PageIO.getPageId(pageBuf), pageBuf, 0, false);

                    pageBuf.flip();
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
                deleteSnapshot(snpLocDir, pdsSettings.folderName());

                if (log.isDebugEnabled())
                    log.debug("Local snapshot sender closed due to an error occurred: " + th.getMessage());
            }
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

        /**
         * @param processId Unique process id.
         * @param req Snapshot initial request.
         */
        public SnapshotStartDiscoveryMessage(
            UUID processId,
            SnapshotOperationRequest req
        ) {
            super(processId, START_SNAPSHOT, req);
        }

        /** {@inheritDoc} */
        @Override public boolean needExchange() {
            return true;
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
        }

        /**
         * @param rqId Unique snapshot request id.
         * @param name Snapshot name.
         */
        public ClusterSnapshotFuture(UUID rqId, String name) {
            this.rqId = rqId;
            this.name = name;
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

        /** Incremental flag. */
        private final boolean incremental;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * @param snpName Snapshot name.
         */
        public CreateSnapshotCallable(String snpName, boolean incremental) {
            this.snpName = snpName;
            this.incremental = incremental;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            if (incremental)
                ignite.snapshot().createIncrementalSnapshot(snpName).get();
            else
                ignite.snapshot().createSnapshot(snpName).get();

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
            else
                return new IgniteException("Snapshot has not been created", U.convertException(e));
        }
    }
}
