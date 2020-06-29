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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.apache.ignite.thread.OomExceptionHandler;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteFeatures.PERSISTENCE_CACHE_SNAPSHOT;
import static org.apache.ignite.internal.MarshallerContextImpl.mappingFileStoreWorkDir;
import static org.apache.ignite.internal.MarshallerContextImpl.saveMappings;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getFlagByPartId;
import static org.apache.ignite.internal.util.IgniteUtils.isLocalNodeCoordinator;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.END_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.START_SNAPSHOT;

/**
 * Internal implementation of snapshot operations over persistence caches.
 * <p>
 * These major actions available:
 * <ul>
 *     <li>Create snapshot of the whole cluster cache groups by triggering PME to achieve consistency.</li>
 * </ul>
 */
public class IgniteSnapshotManager extends GridCacheSharedManagerAdapter
    implements IgniteSnapshot, PartitionsExchangeAware, MetastorageLifecycleListener {
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

    /** Prefix for snapshot threads. */
    private static final String SNAPSHOT_RUNNER_THREAD_PREFIX = "snapshot-runner";

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
    private volatile BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory;

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
                                snpReq.snpName.equals(sctx.snapshotName()) &&
                                snpReq.bltNodes.contains(leftNodeId))) {
                            sctx.acceptException(new ClusterTopologyCheckedException("Snapshot operation interrupted. " +
                                "One of baseline nodes left the cluster: " + leftNodeId));
                        }
                    }
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

    /**
     * @param snpDir Snapshot dir.
     * @param folderName Local node folder name (see {@link U#maskForFileName} with consistent id).
     */
    public void deleteSnapshot(File snpDir, String folderName) {
        if (!snpDir.exists())
            return;

        assert snpDir.isDirectory() : snpDir;

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

        Set<UUID> leftNodes = new HashSet<>(req.bltNodes);
        leftNodes.removeAll(F.viewReadOnly(cctx.discovery().serverNodes(AffinityTopologyVersion.NONE),
            F.node2id()));

        if (!leftNodes.isEmpty()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Some of baseline nodes left the cluster " +
                "prior to snapshot operation start: " + leftNodes));
        }

        Set<Integer> leftGrps = new HashSet<>(req.grpIds);
        leftGrps.removeAll(cctx.cache().cacheGroupDescriptors().keySet());

        if (!leftGrps.isEmpty()) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Some of requested cache groups doesn't exist " +
                "on the local node [missed=" + leftGrps + ", nodeId=" + cctx.localNodeId() + ']'));
        }

        Map<Integer, Set<Integer>> parts = new HashMap<>();

        // Prepare collection of pairs group and appropriate cache partition to be snapshot.
        // Cache group context may be 'null' on some nodes e.g. a node filter is set.
        for (Integer grpId : req.grpIds) {
            if (cctx.cache().cacheGroup(grpId) == null)
                continue;

            parts.put(grpId, null);
        }

        if (parts.isEmpty())
            return new GridFinishedFuture<>();

        SnapshotFutureTask task0 = registerSnapshotTask(req.snpName,
            req.srcNodeId,
            parts,
            locSndrFactory.apply(req.snpName));

        clusterSnpReq = req;

        return task0.chain(fut -> {
            if (fut.error() == null)
                return new SnapshotOperationResponse();
            else
                throw new GridClosureException(fut.error());
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

        if (snpReq == null || !snpReq.rqId.equals(id)) {
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
            Set<UUID> missed = new HashSet<>(snpReq.bltNodes);
            missed.removeAll(res.keySet());
            missed.removeAll(err.keySet());

            if (cancelled) {
                snpReq.err = new IgniteFutureCancelledCheckedException("Execution of snapshot tasks " +
                    "has been cancelled by external process [err=" + err + ", missed=" + missed + ']');
            }
            else if (!F.isEmpty(err) || !missed.isEmpty()) {
                snpReq.err = new IgniteCheckedException("Execution of local snapshot tasks fails or them haven't been executed " +
                    "due to some of nodes left the cluster. Uncompleted snapshot will be deleted " +
                    "[err=" + err + ", missed=" + missed + ']');
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
            if (req.err != null)
                deleteSnapshot(snapshotLocalDir(req.snpName), pdsSettings.folderName());

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

        Set<UUID> endFail = new HashSet<>(snpReq.bltNodes);
        endFail.removeAll(res.keySet());

        clusterSnpReq = null;

        synchronized (snpOpMux) {
            if (clusterSnpFut != null) {
                if (endFail.isEmpty() && snpReq.err == null) {
                    clusterSnpFut.onDone();

                    if (log.isInfoEnabled())
                        log.info("Cluster-wide snapshot operation finished successfully [req=" + snpReq + ']');
                }
                else if (snpReq.err == null) {
                    clusterSnpFut.onDone(new IgniteCheckedException("Snapshot creation has been finished with an error. " +
                        "Local snapshot tasks may not finished completely or finalizing results fails " +
                        "[fail=" + endFail + ", err=" + err + ']'));
                }
                else
                    clusterSnpFut.onDone(snpReq.err);

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

        IgniteInternalFuture<Void> fut0 = cctx.kernalContext().closure()
            .broadcast(new CancelSnapshotClosure(),
                name,
                cctx.discovery().aliveServerNodes(),
                null)
            .chain(new CX1<IgniteInternalFuture<Collection<Void>>, Void>() {
                @Override public Void applyx(IgniteInternalFuture<Collection<Void>> f) throws IgniteCheckedException {
                    f.get();

                    return null;
                }
            });

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
    @Override public IgniteFuture<Void> createSnapshot(String name) {
        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");

        try {
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
                    .callAsync(new CreateSnapshotClosure(),
                        name,
                        Collections.singletonList(crd),
                        null));
            }

            ClusterSnapshotFuture snpFut0;

            synchronized (snpOpMux) {
                if (clusterSnpFut != null && !clusterSnpFut.isDone())
                    throw new IgniteException("Create snapshot request has been rejected. The previous snapshot operation was not completed.");

                if (clusterSnpReq != null)
                    throw new IgniteException("Create snapshot request has been rejected. Parallel snapshot processes are not allowed.");

                if (localSnapshotNames().contains(name))
                    throw new IgniteException("Create snapshot request has been rejected. Snapshot with given name already exists on local node.");

                snpFut0 = new ClusterSnapshotFuture(UUID.randomUUID(), name);

                clusterSnpFut = snpFut0;
                lastSeenSnpFut = snpFut0;
            }

            List<Integer> grps = cctx.cache().persistentGroups().stream()
                .filter(g -> cctx.cache().cacheType(g.cacheOrGroupName()) == CacheType.USER)
                .filter(g -> !g.config().isEncryptionEnabled())
                .map(CacheGroupDescriptor::groupId)
                .collect(Collectors.toList());

            List<ClusterNode> srvNodes = cctx.discovery().serverNodes(AffinityTopologyVersion.NONE);

            startSnpProc.start(snpFut0.rqId, new SnapshotOperationRequest(snpFut0.rqId,
                cctx.localNodeId(),
                name,
                grps,
                new HashSet<>(F.viewReadOnly(srvNodes,
                    F.node2id(),
                    (node) -> CU.baselineNode(node, clusterState)))));

            if (log.isInfoEnabled())
                log.info("Cluster-wide snapshot operation started [snpName=" + name + ", grps=" + grps + ']');

            return new IgniteFutureImpl<>(snpFut0);
        }
        catch (Exception e) {
            U.error(log, "Start snapshot operation failed", e);

            lastSeenSnpFut = new ClusterSnapshotFuture(name, e);

            return new IgniteFinishedFutureImpl<>(e);
        }
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

        SnapshotFutureTask task = locSnpTasks.get(snpReq.snpName);

        if (task == null)
            return;

        if (task.start()) {
            cctx.database().forceCheckpoint(String.format("Start snapshot operation: %s", snpReq.snpName));

            // Schedule task on a checkpoint and wait when it starts.
            try {
                task.awaitStarted();
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
     * @param snpName Unique snapshot name.
     * @param srcNodeId Node id which cause snapshot operation.
     * @param parts Collection of pairs group and appropriate cache partition to be snapshot.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @return Snapshot operation task which should be registered on checkpoint to run.
     */
    SnapshotFutureTask registerSnapshotTask(
        String snpName,
        UUID srcNodeId,
        Map<Integer, Set<Integer>> parts,
        SnapshotSender snpSndr
    ) {
        if (!busyLock.enterBusy())
            return new SnapshotFutureTask(new IgniteCheckedException("Snapshot manager is stopping [locNodeId=" + cctx.localNodeId() + ']'));

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
     * @return The executor used to run snapshot tasks.
     */
    Executor snapshotExecutorService() {
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
                     .createPageStore(getFlagByPartId(pair.getPartitionId()),
                         snpPart::toPath,
                         new LongAdderMetric("NO_OP", null))
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

    /** Snapshot start request for {@link DistributedProcess} initiate message. */
    private static class SnapshotOperationRequest implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Unique snapshot request id. */
        private final UUID rqId;

        /** Source node id which trigger request. */
        private final UUID srcNodeId;

        /** Snapshot name. */
        private final String snpName;

        /** The list of cache groups to include into snapshot. */
        @GridToStringInclude
        private final List<Integer> grpIds;

        /** The list of affected by snapshot operation baseline nodes. */
        @GridToStringInclude
        private final Set<UUID> bltNodes;

        /** Exception occurred during snapshot operation processing. */
        private volatile IgniteCheckedException err;

        /**
         * @param snpName Snapshot name.
         * @param grpIds Cache groups to include into snapshot.
         */
        public SnapshotOperationRequest(UUID rqId, UUID srcNodeId, String snpName, List<Integer> grpIds, Set<UUID> bltNodes) {
            this.rqId = rqId;
            this.srcNodeId = srcNodeId;
            this.snpName = snpName;
            this.grpIds = grpIds;
            this.bltNodes = bltNodes;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SnapshotOperationRequest.class, this);
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
    private static class ClusterSnapshotFuture extends GridFutureAdapter<Void> {
        /** Unique snapshot request id. */
        private final UUID rqId;

        /** Snapshot name. */
        private final String name;

        /** Snapshot start time. */
        private final long startTime;

        /** Snapshot finish time. */
        private volatile long endTime;

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
    private static class CreateSnapshotClosure implements IgniteClosure<String, Void> {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void apply(String name) {
            ignite.snapshot().createSnapshot(name).get();

            return null;
        }
    }

    /** Cancel snapshot operation closure. */
    @GridInternal
    private static class CancelSnapshotClosure implements IgniteClosure<String, Void> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Void apply(String snpName) {
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
