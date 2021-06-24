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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.SnapshotEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.InitMessage;
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
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.thread.OomExceptionHandler;
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
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_ID;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getTypeByPartId;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.T_DATA;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getPageIO;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SKIP_AUTH;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBGRID;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;
import static org.apache.ignite.internal.util.IgniteUtils.isLocalNodeCoordinator;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_SNAPSHOT;

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

    /** Default snapshot directory for loading remote snapshots. */
    public static final String DFLT_SNAPSHOT_TMP_DIR = "snp";

    /** Snapshot in progress error message. */
    public static final String SNP_IN_PROGRESS_ERR_MSG = "Operation rejected due to the snapshot operation in progress.";

    /** Error message to finalize snapshot tasks. */
    public static final String SNP_NODE_STOPPING_ERR_MSG = "Snapshot has been cancelled due to the local node " +
        "is stopping";

    /** Metastorage key to save currently running snapshot. */
    public static final String SNP_RUNNING_KEY = "snapshot-running";

    /** Snapshot metrics prefix. */
    public static final String SNAPSHOT_METRICS = "snapshot";

    /** Snapshot metafile extension. */
    public static final String SNAPSHOT_METAFILE_EXT = ".smf";

    /** Prefix for snapshot threads. */
    private static final String SNAPSHOT_RUNNER_THREAD_PREFIX = "snapshot-runner";

    /** Snapshot operation finish log message. */
    private static final String SNAPSHOT_FINISHED_MSG = "Cluster-wide snapshot operation finished successfully: ";

    /** Snapshot operation fail log message. */
    private static final String SNAPSHOT_FAILED_MSG = "Cluster-wide snapshot operation failed: ";

    /** Total number of thread to perform local snapshot. */
    private static final int SNAPSHOT_THREAD_POOL_SIZE = 4;

    /**
     * Local buffer to perform copy-on-write operations with pages for {@code SnapshotFutureTask.PageStoreSerialWriter}s.
     * It is important to have only only buffer per thread (instead of creating each buffer per
     * each {@code SnapshotFutureTask.PageStoreSerialWriter}) this is redundant and can lead to OOM errors. Direct buffer
     * deallocate only when ByteBuffer is garbage collected, but it can get out of off-heap memory before it.
     */
    private final ThreadLocal<ByteBuffer> locBuff;

    /** Map of registered cache snapshot processes and their corresponding contexts. */
    private final ConcurrentMap<String, SnapshotFutureTask> locSnpTasks = new ConcurrentHashMap<>();

    /** Lock to protect the resources is used. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Mutex used to order cluster snapshot operation progress. */
    private final Object snpOpMux = new Object();

    /** Take snapshot operation procedure. */
    private final DistributedProcess<SnapshotOperationRequest, SnapshotOperationResponse> startSnpProc;

    /** Check previously performed snapshot operation and delete uncompleted files if need. */
    private final DistributedProcess<SnapshotOperationRequest, SnapshotOperationResponse> endSnpProc;

    /** Marshaller. */
    private final Marshaller marsh;

    /** Distributed process to restore cache group from the snapshot. */
    private final SnapshotRestoreProcess restoreCacheGrpProc;

    /** Resolved persistent data storage settings. */
    private volatile PdsFolderSettings pdsSettings;

    /** Fully initialized metastorage. */
    private volatile ReadWriteMetastorage metaStorage;

    /** Local snapshot sender factory. */
    private Function<String, SnapshotSender> locSndrFactory = LocalSnapshotSender::new;

    /** Main snapshot directory to save created snapshots. */
    private volatile File locSnpDir;

    /**
     * Working directory for loaded snapshots from the remote nodes and storing
     * temporary partition delta-files of locally started snapshot process.
     */
    private File tmpWorkDir;

    /** Factory to working with delta as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Factory to create page store for restore. */
    private volatile BiFunction<Integer, Boolean, FileVersionCheckingFactory> storeFactory;

    /** Snapshot thread pool to perform local partition snapshots. */
    private ExecutorService snpRunner;

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

        snpRunner = new IgniteThreadPoolExecutor(SNAPSHOT_RUNNER_THREAD_PREFIX,
            cctx.igniteInstanceName(),
            SNAPSHOT_THREAD_POOL_SIZE,
            SNAPSHOT_THREAD_POOL_SIZE,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            SYSTEM_POOL,
            new OomExceptionHandler(ctx));

        assert cctx.pageStore() instanceof FilePageStoreManager;

        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        pdsSettings = cctx.kernalContext().pdsFolderResolver().resolveFolders();

        locSnpDir = resolveSnapshotWorkDirectory(ctx.config());
        tmpWorkDir = U.resolveWorkDirectory(storeMgr.workDir().getAbsolutePath(), DFLT_SNAPSHOT_TMP_DIR, true);

        U.ensureDirectory(locSnpDir, "snapshot work directory", log);
        U.ensureDirectory(tmpWorkDir, "temp directory for snapshot creation", log);

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
        mreg.register("LocalSnapshotNames", this::localSnapshotNames, List.class,
            "The list of names of all snapshots currently saved on the local node with respect to " +
                "the configured via IgniteConfiguration snapshot working path.");

        storeFactory = storeMgr::getPageStoreFactory;

        cctx.exchange().registerExchangeAwareComponent(this);
        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);

        cctx.gridEvents().addDiscoveryEventListener(discoLsnr = (evt, discoCache) -> {
            if (!busyLock.enterBusy())
                return;

            try {
                UUID leftNodeId = evt.eventNode().id();

                if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED) {
                    SnapshotOperationRequest snpReq = clusterSnpReq;

                    for (SnapshotFutureTask sctx : locSnpTasks.values()) {
                        if (sctx.sourceNodeId().equals(leftNodeId) ||
                            (snpReq != null &&
                                snpReq.snapshotName().equals(sctx.snapshotName()) &&
                                snpReq.nodes().contains(leftNodeId))) {
                            sctx.acceptException(new ClusterTopologyCheckedException("Snapshot operation interrupted. " +
                                "One of baseline nodes left the cluster: " + leftNodeId));
                        }
                    }

                    restoreCacheGrpProc.onNodeLeft(leftNodeId);
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        busyLock.block();

        try {
            restoreCacheGrpProc.interrupt(new NodeStoppingException("Node is stopping."));

            // Try stop all snapshot processing if not yet.
            for (SnapshotFutureTask sctx : locSnpTasks.values())
                sctx.acceptException(new NodeStoppingException(SNP_NODE_STOPPING_ERR_MSG));

            locSnpTasks.clear();

            synchronized (snpOpMux) {
                if (clusterSnpFut != null) {
                    clusterSnpFut.onDone(new NodeStoppingException(SNP_NODE_STOPPING_ERR_MSG));

                    clusterSnpFut = null;
                }
            }

            if (snpRunner != null)
                snpRunner.shutdownNow();

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
        assert locSnpDir != null;
        assert U.alphanumericUnderscore(snpName) : snpName;

        return new File(locSnpDir, snpName);
    }

    /**
     * @return Node snapshot working directory.
     */
    public File snapshotTmpDir() {
        assert tmpWorkDir != null;

        return tmpWorkDir;
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when a snapshot has been started.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalSnapshotStartStage(SnapshotOperationRequest req) {
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

        List<Integer> grpIds = new ArrayList<>(F.viewReadOnly(req.groups(), CU::cacheId));

        Set<Integer> leftGrps = new HashSet<>(grpIds);
        leftGrps.removeAll(cctx.cache().cacheGroupDescriptors().keySet());
        boolean withMetaStorage = leftGrps.remove(METASTORAGE_CACHE_ID);

        if (!leftGrps.isEmpty()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Some of requested cache groups doesn't exist " +
                "on the local node [missed=" + leftGrps + ", nodeId=" + cctx.localNodeId() + ']'));
        }

        Map<Integer, Set<Integer>> parts = new HashMap<>();

        // Prepare collection of pairs group and appropriate cache partition to be snapshot.
        // Cache group context may be 'null' on some nodes e.g. a node filter is set.
        for (Integer grpId : grpIds) {
            if (cctx.cache().cacheGroup(grpId) == null)
                continue;

            parts.put(grpId, null);
        }

        IgniteInternalFuture<Set<GroupPartitionId>> task0;

        if (parts.isEmpty() && !withMetaStorage)
            task0 = new GridFinishedFuture<>(Collections.emptySet());
        else {
            task0 = registerSnapshotTask(req.snapshotName(),
                req.operationalNodeId(),
                parts,
                withMetaStorage,
                locSndrFactory.apply(req.snapshotName()));

            if (withMetaStorage) {
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

                File smf = new File(snapshotLocalDir(req.snapshotName()), snapshotMetaFileName(cctx.localNode().consistentId().toString()));

                if (smf.exists())
                    throw new GridClosureException(new IgniteException("Snapshot metafile must not exist: " + smf.getAbsolutePath()));

                smf.getParentFile().mkdirs();

                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(smf))) {
                    U.marshal(marsh,
                        new SnapshotMetadata(req.requestId(),
                            req.snapshotName(),
                            cctx.localNode().consistentId().toString(),
                            pdsSettings.folderName(),
                            cctx.gridConfig().getDataStorageConfiguration().getPageSize(),
                            grpIds,
                            blts,
                            fut.result()),
                        out);

                    log.info("Snapshot metafile has been created: " + smf.getAbsolutePath());
                }

                return new SnapshotOperationResponse();
            }
            catch (IOException | IgniteCheckedException e) {
                throw F.wrap(e);
            }
        });
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void processLocalSnapshotStartStageResult(UUID id, Map<UUID, SnapshotOperationResponse> res, Map<UUID, Exception> err) {
        if (cctx.kernalContext().clientNode())
            return;

        SnapshotOperationRequest snpReq = clusterSnpReq;

        boolean cancelled = err.values().stream().anyMatch(e -> e instanceof IgniteFutureCancelledCheckedException);

        if (snpReq == null || !snpReq.requestId().equals(id)) {
            synchronized (snpOpMux) {
                if (clusterSnpFut != null && clusterSnpFut.rqId.equals(id)) {
                    if (cancelled) {
                        clusterSnpFut.onDone(new IgniteFutureCancelledCheckedException("Execution of snapshot tasks " +
                            "has been cancelled by external process [err=" + err + ", snpReq=" + snpReq + ']'));
                    } else {
                        clusterSnpFut.onDone(new IgniteCheckedException("Snapshot operation has not been fully completed " +
                            "[err=" + err + ", snpReq=" + snpReq + ']'));
                    }

                    clusterSnpFut = null;
                }

                return;
            }
        }

        if (isLocalNodeCoordinator(cctx.discovery())) {
            Set<UUID> missed = new HashSet<>(snpReq.nodes());
            missed.removeAll(res.keySet());
            missed.removeAll(err.keySet());

            if (cancelled) {
                snpReq.error(new IgniteFutureCancelledCheckedException("Execution of snapshot tasks " +
                    "has been cancelled by external process [err=" + err + ", missed=" + missed + ']'));
            }
            else if (!F.isEmpty(err) || !missed.isEmpty()) {
                snpReq.error(new IgniteCheckedException("Execution of local snapshot tasks fails or them haven't been executed " +
                    "due to some of nodes left the cluster. Uncompleted snapshot will be deleted " +
                    "[err=" + err + ", missed=" + missed + ']'));
            }

            endSnpProc.start(UUID.randomUUID(), snpReq);
        }
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when the snapshot will be finalized.
     */
    private IgniteInternalFuture<SnapshotOperationResponse> initLocalSnapshotEndStage(SnapshotOperationRequest req) {
        if (clusterSnpReq == null)
            return new GridFinishedFuture<>(new SnapshotOperationResponse());

        try {
            if (req.error() != null)
                deleteSnapshot(snapshotLocalDir(req.snapshotName()), pdsSettings.folderName());

            removeLastMetaStorageKey();
        }
        catch (Exception e) {
            return new GridFinishedFuture<>(e);
        }

        return new GridFinishedFuture<>(new SnapshotOperationResponse());
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    private void processLocalSnapshotEndStageResult(UUID id, Map<UUID, SnapshotOperationResponse> res, Map<UUID, Exception> err) {
        SnapshotOperationRequest snpReq = clusterSnpReq;

        if (snpReq == null)
            return;

        Set<UUID> endFail = new HashSet<>(snpReq.nodes());
        endFail.removeAll(res.keySet());

        clusterSnpReq = null;

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
     * @param cacheName Cache name.
     * @param grpName Cache group name.
     * @return {@code True} if the cache or group with the specified name is being restored.
     */
    public boolean isRestoring(String cacheName, @Nullable String grpName) {
        return restoreCacheGrpProc.isRestoring(cacheName, grpName);
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
    public List<String> localSnapshotNames() {
        if (cctx.kernalContext().clientNode())
            throw new UnsupportedOperationException("Client and daemon nodes can not perform this operation.");

        if (locSnpDir == null)
            return Collections.emptyList();

        synchronized (snpOpMux) {
            return Arrays.stream(locSnpDir.listFiles(File::isDirectory))
                .map(File::getName)
                .collect(Collectors.toList());
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> cancelSnapshot(String name) {
        A.notNullOrEmpty(name, "Snapshot name must be not empty or null");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        IgniteInternalFuture<Void> fut0 = cctx.kernalContext().closure()
            .callAsyncNoFailover(BROADCAST,
                new CancelSnapshotCallable(name),
                cctx.discovery().aliveServerNodes(),
                false,
                0,
                true);

        return new IgniteFutureImpl<>(fut0);
    }

    /**
     * @param name Snapshot name to cancel operation on local node.
     */
    public void cancelLocalSnapshotTask(String name) {
        A.notNullOrEmpty(name, "Snapshot name must be not null or empty");

        ClusterSnapshotFuture fut0 = null;

        busyLock.enterBusy();

        try {
            for (SnapshotFutureTask sctx : locSnpTasks.values()) {
                if (sctx.snapshotName().equals(name))
                    sctx.cancel();
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
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> cancelSnapshotRestore(String name) {
        return executeRestoreManagementTask(SnapshotRestoreCancelTask.class, name);
    }

    /**
     * @param name Snapshot name.
     *
     * @return Future that will be finished when process the process is complete. The result of this future will be
     * {@code false} if the restore process with the specified snapshot name is not running at all.
     */
    public IgniteFuture<Boolean> cancelLocalRestoreTask(String name) {
        return restoreCacheGrpProc.cancel(new IgniteCheckedException("Operation has been canceled by the user."), name);
    }

    /**
     * @param name Snapshot name.
     * @return {@code true} if snapshot is OK.
     */
    public IgniteInternalFuture<IdleVerifyResultV2> checkSnapshot(String name) {
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");

        cctx.kernalContext().security().authorize(ADMIN_SNAPSHOT);

        return checkSnapshot(name, null).chain(f -> {
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
     * @param grps Collection of cache group names to check.
     * @return {@code true} if snapshot is OK.
     */
    public IgniteInternalFuture<SnapshotPartitionsVerifyTaskResult> checkSnapshot(String name, @Nullable Collection<String> grps) {
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

        kctx0.task().execute(SnapshotMetadataCollectorTask.class, name).listen(f0 -> {
            if (f0.error() == null) {
                Map<ClusterNode, List<SnapshotMetadata>> metas = f0.result();

                Map<Integer, String> grpIds = grps == null ? Collections.emptyMap() :
                    grps.stream().collect(Collectors.toMap(CU::cacheId, v -> v));

                for (List<SnapshotMetadata> nodeMetas : metas.values()) {
                    for (SnapshotMetadata meta : nodeMetas)
                        grpIds.keySet().removeAll(meta.partitions().keySet());
                }

                if (!grpIds.isEmpty()) {
                    res.onDone(new SnapshotPartitionsVerifyTaskResult(metas,
                        new IdleVerifyResultV2(Collections.singletonMap(cctx.localNode(),
                            new IllegalArgumentException("Cache group(s) was not " +
                                "found in the snapshot [groups=" + grpIds.values() + ", snapshot=" + name + ']')))));

                    return;
                }

                kctx0.task().setThreadContext(TC_SKIP_AUTH, true);
                kctx0.task().setThreadContext(TC_SUBGRID, new ArrayList<>(metas.keySet()));

                kctx0.task().execute(SnapshotPartitionsVerifyTask.class, new SnapshotPartitionsVerifyTaskArg(grps, metas))
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
     * @param folderName Directory name for cache group.
     * @return The list of cache or cache group names in given snapshot on local node.
     */
    public List<File> snapshotCacheDirectories(String snpName, String folderName) {
        return snapshotCacheDirectories(snpName, folderName, name -> true);
    }

    /**
     * @param snpName Snapshot name.
     * @param folderName Directory name for cache group.
     * @param names Cache group names to filter.
     * @return The list of cache or cache group names in given snapshot on local node.
     */
    public List<File> snapshotCacheDirectories(String snpName, String folderName, Predicate<String> names) {
        File snpDir = snapshotLocalDir(snpName);

        if (!snpDir.exists())
            return Collections.emptyList();

        return cacheDirectories(new File(snpDir, databaseRelativePath(folderName)), names);
    }

    /**
     * @param snpName Snapshot name.
     * @param consId Node consistent id to read medata for.
     * @return Snapshot metadata instance.
     */
    public SnapshotMetadata readSnapshotMetadata(String snpName, String consId) {
        return readSnapshotMetadata(new File(snapshotLocalDir(snpName), snapshotMetaFileName(consId)));
    }

    /**
     * @param smf File denoting to snapshot metafile.
     * @return Snapshot metadata instance.
     */
    private SnapshotMetadata readSnapshotMetadata(File smf) {
        if (!smf.exists())
            throw new IgniteException("Snapshot metafile cannot be read due to it doesn't exist: " + smf);

        String smfName = smf.getName().substring(0, smf.getName().length() - SNAPSHOT_METAFILE_EXT.length());

        try (InputStream in = new BufferedInputStream(new FileInputStream(smf))) {
            SnapshotMetadata meta = marsh.unmarshal(in, U.resolveClassLoader(cctx.gridConfig()));

            if (!U.maskForFileName(meta.consistentId()).equals(smfName)) {
                throw new IgniteException(
                    "Error reading snapshot metadata [smfName=" + smfName + ", consId=" + U.maskForFileName(meta.consistentId())
                );
            }

            return meta;
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException("An error occurred during reading snapshot metadata file [file=" +
                smf.getAbsolutePath() + "]", e);
        }
    }

    /**
     * @param snpName Snapshot name.
     * @return List of snapshot metadata for the given snapshot name on local node.
     * If snapshot has been taken from local node the snapshot metadata for given
     * local node will be placed on the first place.
     */
    public List<SnapshotMetadata> readSnapshotMetadatas(String snpName) {
        A.notNullOrEmpty(snpName, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(snpName), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");

        File[] smfs = snapshotLocalDir(snpName).listFiles((dir, name) ->
            name.toLowerCase().endsWith(SNAPSHOT_METAFILE_EXT));

        if (smfs == null)
            throw new IgniteException("Snapshot directory doesn't exists or an I/O error occurred during directory read.");

        Map<String, SnapshotMetadata> metasMap = new HashMap<>();
        SnapshotMetadata prev = null;

        for (File smf : smfs) {
            SnapshotMetadata curr = readSnapshotMetadata(smf);

            if (prev != null && !prev.sameSnapshot(curr))
                throw new IgniteException("Snapshot metadata files are from different snapshots [prev=" + prev + ", curr=" + curr);

            metasMap.put(curr.consistentId(), curr);

            prev = curr;
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
                        new CreateSnapshotCallable(name),
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

                if (localSnapshotNames().contains(name)) {
                    throw new IgniteException(
                        "Create snapshot request has been rejected. Snapshot with given name already exists on local node."
                    );
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
                .filter(g -> !g.config().isEncryptionEnabled())
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

            startSnpProc.start(snpFut0.rqId, new SnapshotOperationRequest(snpFut0.rqId,
                cctx.localNodeId(),
                name,
                grps,
                new HashSet<>(F.viewReadOnly(srvNodes,
                    F.node2id(),
                    (node) -> CU.baselineNode(node, clusterState)))));

            String msg = "Cluster-wide snapshot operation started [snpName=" + name + ", grps=" + grps + ']';

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
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");
        A.ensure(grpNames == null || !grpNames.isEmpty(), "List of cache group names cannot be empty.");

        return restoreCacheGrpProc.start(name, grpNames);
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

        if (snpName == null)
            return;

        recovered = true;

        for (File tmp : snapshotTmpDir().listFiles())
            U.delete(tmp);

        deleteSnapshot(snapshotLocalDir(snpName), pdsSettings.folderName());

        if (log.isInfoEnabled()) {
            log.info("Previous attempt to create snapshot fail due to the local node crash. All resources " +
                "related to snapshot operation have been deleted: " + snpName);
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

        SnapshotFutureTask task = locSnpTasks.get(snpReq.snapshotName());

        if (task == null)
            return;

        if (task.start()) {
            cctx.database().forceCheckpoint(String.format("Start snapshot operation: %s", snpReq.snapshotName()));

            // Schedule task on a checkpoint and wait when it starts.
            try {
                task.started().get();
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Fail to wait while cluster-wide snapshot operation started", e);
            }
        }
    }

    /**
     * @param grps List of cache groups which will be destroyed.
     */
    public void onCacheGroupsStopped(List<Integer> grps) {
        for (SnapshotFutureTask sctx : locSnpTasks.values()) {
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
     * @param snpName Snapshot name.
     * @param folderName The node folder name, usually it's the same as the U.maskForFileName(consistentId).
     * @param grpName Cache group name.
     * @param partId Partition id.
     * @return Iterator over partition.
     * @throws IgniteCheckedException If and error occurs.
     */
    public GridCloseableIterator<CacheDataRow> partitionRowIterator(String snpName,
        String folderName,
        String grpName,
        int partId
    ) throws IgniteCheckedException {
        File snpDir = snapshotLocalDir(snpName);

        if (!snpDir.exists())
            throw new IgniteCheckedException("Snapshot directory doesn't exists: " + snpDir.getAbsolutePath());

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

        File snpPart = getPartitionFile(new File(snapshotLocalDir(snpName), databaseRelativePath(folderName)),
            grps.get(0).getName(), partId);

        FilePageStore pageStore = (FilePageStore)storeFactory
            .apply(CU.cacheId(grpName), false)
            .createPageStore(getTypeByPartId(partId),
                snpPart::toPath,
                val -> {
                });

        GridKernalContext kctx = new StandaloneGridKernalContext(log,
            resolveBinaryWorkDir(snpDir.getAbsolutePath(), folderName),
            resolveMappingFileStoreWorkDir(snpDir.getAbsolutePath()));

        CacheObjectContext coctx = new CacheObjectContext(kctx, grpName, null, false,
            false, false, false, false);

        GridCacheSharedContext<?, ?> sctx = new GridCacheSharedContext<>(kctx, null, null, null,
            null, null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null, null);

        return new DataPageIterator(sctx, coctx, pageStore, partId);
    }

    /**
     * @param snpName Unique snapshot name.
     * @param srcNodeId Node id which cause snapshot operation.
     * @param parts Collection of pairs group and appropriate cache partition to be snapshot.
     * @param withMetaStorage {@code true} if all metastorage data must be also included into snapshot.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @return Snapshot operation task which should be registered on checkpoint to run.
     */
    SnapshotFutureTask registerSnapshotTask(
        String snpName,
        UUID srcNodeId,
        Map<Integer, Set<Integer>> parts,
        boolean withMetaStorage,
        SnapshotSender snpSndr
    ) {
        if (!busyLock.enterBusy()) {
            return new SnapshotFutureTask(
                new IgniteCheckedException("Snapshot manager is stopping [locNodeId=" + cctx.localNodeId() + ']'));
        }

        try {
            if (locSnpTasks.containsKey(snpName))
                return new SnapshotFutureTask(new IgniteCheckedException("Snapshot with requested name is already scheduled: " + snpName));

            SnapshotFutureTask snpFutTask;

            SnapshotFutureTask prev = locSnpTasks.putIfAbsent(snpName,
                snpFutTask = new SnapshotFutureTask(cctx,
                    srcNodeId,
                    snpName,
                    tmpWorkDir,
                    ioFactory,
                    snpSndr,
                    parts,
                    withMetaStorage,
                    locBuff));

            if (prev != null)
                return new SnapshotFutureTask(new IgniteCheckedException("Snapshot with requested name is already scheduled: " + snpName));

            if (log.isInfoEnabled()) {
                log.info("Snapshot task has been registered on local node [sctx=" + this +
                    ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
            }

            snpFutTask.listen(f -> locSnpTasks.remove(snpName));

            return snpFutTask;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param factory Factory which produces {@link LocalSnapshotSender} implementation.
     */
    void localSnapshotSenderFactory(Function<String, SnapshotSender> factory) {
        locSndrFactory = factory;
    }

    /**
     * @return Factory which produces {@link LocalSnapshotSender} implementation.
     */
    Function<String, SnapshotSender> localSnapshotSenderFactory() {
        return locSndrFactory;
    }

    /** Snapshot finished successfully or already restored. Key can be removed. */
    private void removeLastMetaStorageKey() throws IgniteCheckedException {
        cctx.database().checkpointReadLock();

        try {
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
    private void recordSnapshotEvent(String snpName, String msg, int type) {
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
        assert snpRunner != null;

        return snpRunner;
    }

    /**
     * @param ioFactory Factory to create IO interface over a page stores.
     */
    void ioFactory(FileIOFactory ioFactory) {
        this.ioFactory = ioFactory;
    }

    /**
     * @return Factory to create IO interface over a page stores.
     */
    FileIOFactory ioFactory() {
        return ioFactory;
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
        try (FileIO src = factory.create(from, READ);
             FileChannel dest = new FileOutputStream(to).getChannel()) {
            if (src.size() < length) {
                throw new IgniteException("The source file to copy has to enough length " +
                    "[expected=" + length + ", actual=" + src.size() + ']');
            }

            src.position(0);

            long written = 0;

            while (written < length)
                written += src.transferTo(written, length - written, dest);
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
         * can't be greater that 2 * store.size().
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
                        // the page where DataRow takes up all the free space. There is no a dedicated
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

    /**
     * Snapshot sender which writes all data to local directory.
     */
    private class LocalSnapshotSender extends SnapshotSender {
        /** Snapshot name. */
        private final String snpName;

        /** Local snapshot directory. */
        private final File snpLocDir;

        /** Local node snapshot directory calculated on snapshot directory. */
        private File dbDir;

        /** Size of page. */
        private final int pageSize;

        /**
         * @param snpName Snapshot name.
         */
        public LocalSnapshotSender(String snpName) {
            super(IgniteSnapshotManager.this.log, snpRunner);

            this.snpName = snpName;
            snpLocDir = snapshotLocalDir(snpName);
            pageSize = cctx.kernalContext().config().getDataStorageConfiguration().getPageSize();
        }

        /** {@inheritDoc} */
        @Override protected void init(int partsCnt) {
            dbDir = new File(snpLocDir, databaseRelativePath(pdsSettings.folderName()));

            if (dbDir.exists()) {
                throw new IgniteException("Snapshot with given name already exists " +
                    "[snpName=" + snpName + ", absPath=" + dbDir.getAbsolutePath() + ']');
            }

            cctx.database().checkpointReadLock();

            try {
                assert metaStorage != null && metaStorage.read(SNP_RUNNING_KEY) == null :
                    "The previous snapshot hasn't been completed correctly";

                metaStorage.write(SNP_RUNNING_KEY, snpName);

                U.ensureDirectory(dbDir, "snapshot work directory", log);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                cctx.database().checkpointReadUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig0(File ccfg, String cacheDirName) {
            assert dbDir != null;

            try {
                File cacheDir = U.resolveWorkDirectory(dbDir.getAbsolutePath(), cacheDirName, false);

                copy(ioFactory, ccfg, new File(cacheDir, ccfg.getName()), ccfg.length());
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

                copy(ioFactory, part, snpPart, len);

                if (log.isInfoEnabled()) {
                    log.info("Partition has been snapshot [snapshotDir=" + dbDir.getAbsolutePath() +
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

            if (log.isInfoEnabled()) {
                log.info("Start partition snapshot recovery with the given delta page file [part=" + snpPart +
                    ", delta=" + delta + ']');
            }

            try (FileIO fileIo = ioFactory.create(delta, READ);
                 FilePageStore pageStore = (FilePageStore)storeFactory
                     .apply(pair.getGroupId(), false)
                     .createPageStore(getTypeByPartId(pair.getPartitionId()),
                         snpPart::toPath,
                         val -> {})
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
                    log.info("Local snapshot sender closed, resources released [dbNodeSnpDir=" + dbDir + ']');
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
    protected static class ClusterSnapshotFuture extends GridFutureAdapter<Void> {
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
    }

    /** Start creation of cluster snapshot closure. */
    @GridInternal
    private static class CreateSnapshotCallable implements IgniteCallable<Void> {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Snapshot name. */
        private final String snpName;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * @param snpName Snapshot name.
         */
        public CreateSnapshotCallable(String snpName) {
            this.snpName = snpName;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            ignite.snapshot().createSnapshot(snpName).get();

            return null;
        }
    }

    /** Cancel snapshot operation closure. */
    @GridInternal
    private static class CancelSnapshotCallable implements IgniteCallable<Void> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Snapshot name. */
        private final String snpName;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * @param snpName Snapshot name.
         */
        public CancelSnapshotCallable(String snpName) {
            this.snpName = snpName;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            ignite.context().cache().context().snapshotMgr().cancelLocalSnapshotTask(snpName);

            return null;
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
