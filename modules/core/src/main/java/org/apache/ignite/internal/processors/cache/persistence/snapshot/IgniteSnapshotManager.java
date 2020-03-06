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
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.communication.TransmissionCancelledException;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.cluster.BaselineTopologyHistoryItem;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.InitMessage;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.lang.IgniteThrowableSupplier;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.cluster.ClusterState.active;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteFeatures.PERSISTENCE_CACHE_SNAPSHOT;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.MarshallerContextImpl.saveMappings;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileName;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getFlagByPartId;
import static org.apache.ignite.internal.util.IgniteUtils.getBaselineTopology;
import static org.apache.ignite.internal.util.IgniteUtils.isLocalNodeCoordinator;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.PREPARE_RESTORE_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_SNAPSHOT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TAKE_SNAPSHOT;

/** */
public class IgniteSnapshotManager extends GridCacheSharedManagerAdapter implements IgniteSnapshot, PartitionsExchangeAware {
    /** File with delta pages suffix. */
    public static final String DELTA_SUFFIX = ".delta";

    /** File extension of snapshot meta infomation. */
    public static final String SNAPSHOT_META_EXTENSION = ".snapshotmeta";

    /** File name template consists of delta pages. */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** File name template for index delta pages. */
    public static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** The reason of checkpoint start for needs of snapshot. */
    public static final String SNAPSHOT_CP_REASON = "Wakeup for checkpoint to take snapshot [name=%s]";

    /** Default snapshot directory for loading remote snapshots. */
    public static final String DFLT_SNAPSHOT_WORK_DIRECTORY = "snp";

    /** Timeout in milliseconsd for snapshot operations. */
    public static final long DFLT_SNAPSHOT_TIMEOUT = 15_000L;

    /** Prefix for snapshot threads. */
    private static final String SNAPSHOT_RUNNER_THREAD_PREFIX = "snapshot-runner";

    /** Total number of thread to perform local snapshot. */
    private static final int SNAPSHOT_THREAD_POOL_SIZE = 4;

    /** Default snapshot topic to receive snapshots from remote node. */
    private static final Object DFLT_INITIAL_SNAPSHOT_TOPIC = GridTopic.TOPIC_SNAPSHOT.topic("rmt_snp");

    /** File transmission parameter of cache group id. */
    private static final String SNP_GRP_ID_PARAM = "grpId";

    /** File transmission parameter of cache partition id. */
    private static final String SNP_PART_ID_PARAM = "partId";

    /** File transmission parameter of node-sender directory path with its consistentId (e.g. db/IgniteNode0). */
    private static final String SNP_DB_NODE_PATH_PARAM = "dbNodePath";

    /** File transmission parameter of a cache directory with is currently sends its partitions. */
    private static final String SNP_CACHE_DIR_NAME_PARAM = "cacheDirName";

    /** Snapshot parameter name for a file transmission. */
    private static final String SNP_NAME_PARAM = "snpName";

    /** Map of registered cache snapshot processes and their corresponding contexts. */
    private final ConcurrentMap<String, SnapshotFutureTask> locSnpTasks = new ConcurrentHashMap<>();

    /** Lock to protect the resources is used. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Requested snapshot from remote node. */
    private final AtomicReference<RemoteSnapshotFuture> rmtSnpReq = new AtomicReference<>();

    /** Mutex used to order cluster snapshot opertaion progress. */
    private final Object snpOpMux = new Object();

    /** Main snapshot directory to save created snapshots. */
    private File locSnpDir;

    /**
     * Working directory for loaded snapshots from the remote nodes and storing
     * temporary partition delta-files of locally started snapshot process.
     */
    private File tmpWorkDir;

    /** Marshaller used to store snapshot meta information. */
    private Marshaller marshaller;

    /** Factory to working with delta as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Factory to create page store for restore. */
    private volatile BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory;

    /** Snapshot thread pool to perform local partition snapshots. */
    private ExecutorService snpRunner;

    /** System discovery message listener. */
    private DiscoveryEventListener discoLsnr;

    /** Database manager for enabled persistence. */
    private GridCacheDatabaseSharedManager dbMgr;

    /** Configured data storage page size. */
    private int pageSize;

    /** Take snapshot operation procedure. */
    private final DistributedProcess<SnapshotOperationRequest, SnapshotOperationResponse> takeSnpProc;

    /** Prepare snapshot restore operation procedure. */
    private final DistributedProcess<String, Boolean> prepareRestoreSnpProc;

    /** Snapshot restore operation procedure. */
    private final DistributedProcess<String, Boolean> restoreSnpProc;

    /** Cluster snapshot operation requested by user. */
    private ClusterSnapshotFuture clusterSnpFut;

    /** Cluster snapshot restore operation requested by user. */
    private ClusterSnapshotFuture restoreSnpFut;

    /** Current snapshot opertaion on local node. */
    private volatile SnapshotFutureTask clusterSnpTask;

    /**
     * Current snapshot restore operation on local node. Accessed only from discovery thread,
     * so the order is guaranteed.
     */
    private volatile String restoringSnpName;

    /**
     * @param ctx Kernal context.
     */
    public IgniteSnapshotManager(GridKernalContext ctx) {
        takeSnpProc = new DistributedProcess<>(ctx, TAKE_SNAPSHOT, this::takeSnapshot, this::takeSnapshotResult);

        prepareRestoreSnpProc = new DistributedProcess<>(ctx, PREPARE_RESTORE_SNAPSHOT, this::prepareSnapshotRestore,
            this::prepareSnapshotRestoreResult);

        restoreSnpProc = new DistributedProcess<>(ctx, RESTORE_SNAPSHOT, this::snapshotRestoreTask,
            this::snapshotRestoreTaskResult);
    }

    /**
     * @param snapshotCacheDir Snapshot directory to store files.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    public static File getPartionDeltaFile(File snapshotCacheDir, int partId) {
        return new File(snapshotCacheDir, getPartitionDeltaFileName(partId));
    }

    /**
     * @param partId Partitoin id.
     * @return File name of delta partition pages.
     */
    public static String getPartitionDeltaFileName(int partId) {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        return partId == INDEX_PARTITION ? INDEX_DELTA_NAME : String.format(PART_DELTA_TEMPLATE, partId);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        GridKernalContext kctx = cctx.kernalContext();

        // todo Will not start for client nodes. Should we?
        if (kctx.clientNode() || kctx.isDaemon())
            return;

        if (!CU.isPersistenceEnabled(cctx.kernalContext().config()))
            return;

        DataStorageConfiguration dcfg = kctx.config().getDataStorageConfiguration();

        pageSize = dcfg.getPageSize();

        assert pageSize > 0;

        snpRunner = new IgniteThreadPoolExecutor(
            SNAPSHOT_RUNNER_THREAD_PREFIX,
            cctx.igniteInstanceName(),
            SNAPSHOT_THREAD_POOL_SIZE,
            SNAPSHOT_THREAD_POOL_SIZE,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            SYSTEM_POOL,
            // todo do we need critical handler for any unhandled errors?
            (t, e) -> kctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e)));

        assert cctx.pageStore() instanceof FilePageStoreManager;

        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        marshaller = MarshallerUtils.jdkMarshaller(kctx.igniteInstanceName());

        locSnpDir = U.resolveWorkDirectory(kctx.config().getWorkDirectory(), dcfg.getLocalSnapshotPath(), false);
        tmpWorkDir = Paths.get(storeMgr.workDir().getAbsolutePath(), DFLT_SNAPSHOT_WORK_DIRECTORY).toFile();

        U.ensureDirectory(locSnpDir, "local snapshots directory", log);
        U.ensureDirectory(tmpWorkDir, "work directory for snapshots creation", log);

        storeFactory = storeMgr::getPageStoreFactory;
        dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        cctx.exchange().registerExchangeAwareComponent(this);

        // Receive remote snapshots requests.
        cctx.gridIO().addMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (!busyLock.enterBusy())
                    return;

                try {
                    if (msg instanceof SnapshotRequestMessage) {
                        SnapshotRequestMessage reqMsg0 = (SnapshotRequestMessage)msg;
                        String snpName = reqMsg0.snapshotName();

                        synchronized (this) {
                            SnapshotFutureTask task = lastScheduledRemoteSnapshotTask(nodeId);

                            if (task != null) {
                                // Task will also be removed from local map due to the listener on future done.
                                task.cancel();

                                log.info("Snapshot request has been cancelled due to another request recevied " +
                                    "[prevSnpResp=" + task + ", msg0=" + reqMsg0 + ']');
                            }
                        }

                        startSnapshotTask(snpName, nodeId, reqMsg0.parts(), remoteSnapshotSender(snpName, nodeId))
                            .listen(f -> {
                                if (f.error() == null)
                                    return;

                                U.error(log, "Failed to proccess request of creating a snapshot " +
                                    "[from=" + nodeId + ", msg=" + reqMsg0 + ']', f.error());

                                try {
                                    cctx.gridIO().sendToCustomTopic(nodeId,
                                        DFLT_INITIAL_SNAPSHOT_TOPIC,
                                        new SnapshotResponseMessage(reqMsg0.snapshotName(), f.error().getMessage()),
                                        SYSTEM_POOL);
                                }
                                catch (IgniteCheckedException ex0) {
                                    U.error(log, "Fail to send the response message with processing snapshot request " +
                                        "error [request=" + reqMsg0 + ", nodeId=" + nodeId + ']', ex0);
                                }
                            });
                    }
                    else if (msg instanceof SnapshotResponseMessage) {
                        SnapshotResponseMessage respMsg0 = (SnapshotResponseMessage)msg;

                        RemoteSnapshotFuture fut0 = rmtSnpReq.get();

                        if (fut0 == null || !fut0.snpName.equals(respMsg0.snapshotName())) {
                            if (log.isInfoEnabled()) {
                                log.info("A stale snapshot response message has been received. Will be ignored " +
                                    "[fromNodeId=" + nodeId + ", response=" + respMsg0 + ']');
                            }

                            return;
                        }

                        if (respMsg0.errorMessage() != null) {
                            fut0.onDone(new IgniteCheckedException("Request cancelled. The snapshot operation stopped " +
                                "on the remote node with an error: " + respMsg0.errorMessage()));
                        }
                    }
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        });

        cctx.gridEvents().addDiscoveryEventListener(discoLsnr = (evt, discoCache) -> {
            if (!busyLock.enterBusy())
                return;

            try {
                if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                    DiscoveryCustomEvent evt0 = (DiscoveryCustomEvent)evt;

                    if (evt0.customMessage() instanceof InitMessage) {
                        InitMessage<?> msg = (InitMessage<?>)evt0.customMessage();

                        if (msg.type() == TAKE_SNAPSHOT.ordinal()) {
                            assert clusterSnpTask != null : evt;

                            DiscoveryCustomEvent customEvt = new DiscoveryCustomEvent();

                            customEvt.node(evt0.node());
                            customEvt.eventNode(evt0.eventNode());
                            customEvt.affinityTopologyVersion(evt0.affinityTopologyVersion());
                            customEvt.customMessage(new SnapshotStartDiscoveryMessage(discoCache, msg.processId()));

                            // Handle new event inside discovery thread, so no guarantees will be violated.
                            cctx.exchange().onDiscoveryEvent(customEvt, discoCache);
                        }
                    }
                }
                else if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED) {
                    for (SnapshotFutureTask sctx : locSnpTasks.values()) {
                        if (sctx.sourceNodeId().equals(evt.eventNode().id())) {
                            sctx.acceptException(new ClusterTopologyCheckedException("The node which requested snapshot " +
                                "creation has left the grid"));
                        }
                    }

                    RemoteSnapshotFuture snpTrFut = rmtSnpReq.get();

                    if (snpTrFut != null && snpTrFut.rmtNodeId.equals(evt.eventNode().id())) {
                        snpTrFut.onDone(new ClusterTopologyCheckedException("The node from which a snapshot has been " +
                            "requested left the grid"));
                    }
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_DISCOVERY_CUSTOM_EVT);

        // Remote snapshot handler.
        cctx.kernalContext().io().addTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC, new TransmissionHandler() {
            /** {@inheritDoc} */
            @Override public void onException(UUID nodeId, Throwable err) {
                RemoteSnapshotFuture fut = rmtSnpReq.get();

                if (fut == null)
                    return;

                if (fut.rmtNodeId.equals(nodeId))
                    fut.onDone(err);
            }

            /** {@inheritDoc} */
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                Integer partId = (Integer)fileMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)fileMeta.params().get(SNP_NAME_PARAM);
                String rmtDbNodePath = (String)fileMeta.params().get(SNP_DB_NODE_PATH_PARAM);
                String cacheDirName = (String)fileMeta.params().get(SNP_CACHE_DIR_NAME_PARAM);

                RemoteSnapshotFuture transFut = rmtSnpReq.get();

                if (transFut == null || !transFut.snpName.equals(snpName)) {
                    throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                        "[snpName=" + snpName + ", transFut=" + transFut + ']');
                }

                assert transFut.snpName.equals(snpName) && transFut.rmtNodeId.equals(nodeId) :
                    "Another transmission in progress [fut=" + transFut + ", nodeId=" + snpName + ", nodeId=" + nodeId + ']';

                try {
                    File cacheDir = U.resolveWorkDirectory(tmpWorkDir.getAbsolutePath(),
                        cacheSnapshotPath(snpName, rmtDbNodePath, cacheDirName),
                        false);

                    return new File(cacheDir, getPartitionFileName(partId)).getAbsolutePath();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            /**
             * @param snpTrans Current snapshot transmission.
             * @param grpPartId Pair of group id and its partition id.
             */
            private void finishRecover(RemoteSnapshotFuture snpTrans, GroupPartitionId grpPartId) {
                FilePageStore pageStore = null;

                try {
                    pageStore = snpTrans.stores.remove(grpPartId);

                    pageStore.finishRecover();

                    snpTrans.partConsumer.accept(new File(pageStore.getFileAbsolutePath()), grpPartId);

                    if (snpTrans.partsLeft.decrementAndGet() == 0) {
                        assert snpTrans.stores.isEmpty() : snpTrans.stores.entrySet();

                        snpTrans.onDone(true);

                        log.info("Requested snapshot from remote node has been fully received " +
                            "[snpName=" + snpTrans.snpName + ", snpTrans=" + snpTrans + ']');
                    }
                }
                catch (StorageException e) {
                    throw new IgniteException(e);
                }
                finally {
                    U.closeQuiet(pageStore);
                }
            }

            /** {@inheritDoc} */
            @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
                Integer grpId = (Integer)initMeta.params().get(SNP_GRP_ID_PARAM);
                Integer partId = (Integer)initMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)initMeta.params().get(SNP_NAME_PARAM);

                GroupPartitionId grpPartId = new GroupPartitionId(grpId, partId);
                RemoteSnapshotFuture snpTrFut = rmtSnpReq.get();

                if (snpTrFut == null || !snpTrFut.snpName.equals(snpName)) {
                    throw new TransmissionCancelledException("Stale snapshot transmission will be ignored " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ", snpTrFut=" + snpTrFut + ']');
                }

                assert snpTrFut.snpName.equals(snpName) && snpTrFut.rmtNodeId.equals(nodeId) :
                    "Another transmission in progress [snpTrFut=" + snpTrFut + ", nodeId=" + snpName + ", nodeId=" + nodeId + ']';

                FilePageStore pageStore = snpTrFut.stores.get(grpPartId);

                if (pageStore == null) {
                    throw new IgniteException("Partition must be loaded before applying snapshot delta pages " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                pageStore.beginRecover();

                // No snapshot delta pages received. Finalize recovery.
                if (initMeta.count() == 0)
                    finishRecover(snpTrFut, grpPartId);

                return new Consumer<ByteBuffer>() {
                    final LongAdder transferred = new LongAdder();

                    @Override public void accept(ByteBuffer buff) {
                        try {
                            assert initMeta.count() != 0 : initMeta;

                            RemoteSnapshotFuture fut0 = rmtSnpReq.get();

                            if (fut0 == null || !fut0.equals(snpTrFut) || fut0.isCancelled()) {
                                throw new TransmissionCancelledException("Snapshot request is cancelled " +
                                    "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                            }

                            pageStore.write(PageIO.getPageId(buff), buff, 0, false);

                            transferred.add(buff.capacity());

                            if (transferred.longValue() == initMeta.count())
                                finishRecover(snpTrFut, grpPartId);
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                };
            }

            /** {@inheritDoc} */
            @Override public Consumer<File> fileHandler(UUID nodeId, TransmissionMeta initMeta) {
                Integer grpId = (Integer)initMeta.params().get(SNP_GRP_ID_PARAM);
                Integer partId = (Integer)initMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)initMeta.params().get(SNP_NAME_PARAM);

                assert grpId != null;
                assert partId != null;
                assert snpName != null;
                assert storeFactory != null;

                RemoteSnapshotFuture transFut = rmtSnpReq.get();

                if (transFut == null) {
                    throw new IgniteException("Snapshot transmission with given name doesn't exists " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                return new Consumer<File>() {
                    @Override public void accept(File file) {
                        RemoteSnapshotFuture fut0 = rmtSnpReq.get();

                        if (fut0 == null || !fut0.equals(transFut) || fut0.isCancelled()) {
                            throw new TransmissionCancelledException("Snapshot request is cancelled [snpName=" + snpName +
                                ", grpId=" + grpId + ", partId=" + partId + ']');
                        }

                        busyLock.enterBusy();

                        try {
                            FilePageStore pageStore = (FilePageStore)storeFactory
                                .apply(grpId, false)
                                .createPageStore(getFlagByPartId(partId),
                                    file::toPath,
                                    new LongAdderMetric("NO_OP", null));

                            transFut.stores.put(new GroupPartitionId(grpId, partId), pageStore);

                            pageStore.init();
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                        finally {
                            busyLock.leaveBusy();
                        }
                    }
                };
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        busyLock.block();

        try {
            for (SnapshotFutureTask sctx : locSnpTasks.values()) {
                // Try stop all snapshot processing if not yet.
                sctx.acceptException(new NodeStoppingException("Snapshot has been cancelled due to the local node " +
                    "is stopping"));
            }

            locSnpTasks.clear();

            RemoteSnapshotFuture snpTrFut = rmtSnpReq.get();

            if (snpTrFut != null)
                snpTrFut.cancel();

            // Do not shutdown immediately since all task must be closed correctly.
            snpRunner.shutdown();

            cctx.kernalContext().io().removeMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC);
            cctx.kernalContext().event().removeDiscoveryEventListener(discoLsnr);
            cctx.kernalContext().io().removeTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC);

            cctx.exchange().unregisterExchangeAwareComponent(this);
        }
        finally {
            busyLock.unblock();
        }
    }

    /**
     * @return Relative configured path of presistence data storage directory for the local node.
     * Example: {@code snapshotWorkDir/db/IgniteNodeName0}
     */
    public static String relativeNodePath(PdsFolderSettings pcfg) {
        return Paths.get(DB_DEFAULT_FOLDER, pcfg.folderName()).toString();
    }

    /**
     * @param snpName Snapshot name.
     * @return Local snapshot directory for snapshot with given name.
     */
    public File snapshotLocalDir(String snpName) {
        assert locSnpDir != null;

        return new File(locSnpDir, snpName);
    }

    /**
     * @return Node snapshot working directory.
     */
    public File snapshotTempDir() {
        assert tmpWorkDir != null;

        return tmpWorkDir;
    }

    /**
     * @param req Request on snapshot creation.
     * @return Future which will be completed when a snapshot has been started.
     */
    IgniteInternalFuture<SnapshotOperationResponse> takeSnapshot(SnapshotOperationRequest req) {
        if (cctx.kernalContext().clientNode())
            return new GridFinishedFuture<>();

        // Executed inside discovery notifier thread, prior to firing discovery custom event
        if (clusterSnpTask != null) {
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot operation has been rejected. " +
                "Another snapshot operation in progress [req=" + req + ", curr=" + clusterSnpTask + ']'));
        }

        // Collection of pairs group and appropratate cache partition to be snapshotted.
        Map<Integer, GridIntList> parts = req.grpIds.stream()
            .collect(Collectors.toMap(grpId -> grpId,
                grpId -> {
                    GridIntList grps = new GridIntList();

                    cctx.cache()
                        .cacheGroup(grpId)
                        .topology()
                        .currentLocalPartitions()
                        .forEach(p -> grps.add(p.id()));

                    if (cctx.kernalContext().query().moduleEnabled())
                        grps.add(INDEX_PARTITION);

                    return grps;
                }));

        SnapshotFutureTask task0;

        try {
            task0 = startLocalSnapshotTask(req.snpName,
                cctx.localNodeId(),
                parts,
                snpRunner,
                localSnapshotSender(req.snpName));
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        clusterSnpTask = task0;

        return task0
            .chain(f -> new SnapshotOperationResponse());
    }

    /**
     * @param id Request id.
     * @param res Results.
     * @param err Errors.
     */
    void takeSnapshotResult(UUID id, Map<UUID, SnapshotOperationResponse> res, Map<UUID, Exception> err) {
        synchronized (snpOpMux) {
            assert clusterSnpFut == null || clusterSnpFut.id.equals(id);

            SnapshotFutureTask task0 = clusterSnpTask;

            if (!F.isEmpty(err) && task0 != null)
                IgniteUtils.delete(snapshotLocalDir(clusterSnpTask.snapshotName()).toPath());

            clusterSnpTask = null;

            if (clusterSnpFut != null) {
                if (F.isEmpty(err)) {
                    clusterSnpFut.onDone();

                    if (log.isInfoEnabled())
                        log.info("Cluster-wide snapshot operation finished successfully [snpName=" + task0.snapshotName() + ']');
                }
                else {
                    clusterSnpFut.onDone(new IgniteCheckedException("Snapshot operation has been failed due to an error " +
                        "on remote nodes [err=" + err + ']'));
                }
            }

            clusterSnpFut = null;
        }
    }

    /**
     * @param snpLocDir Snapshot local directory.
     * @return Snapshot meta instance.
     * @throws IgniteCheckedException If fails.
     */
    private SnapshotMeta readNodeSnaspshotMeta(File snpLocDir) throws IgniteCheckedException {
        if (!snpLocDir.exists() || !snpLocDir.isDirectory())
            throw new IgniteCheckedException("Snapshot local directory not found on local node: " + snpLocDir);

        File metaFile = new File(snpLocDir,
            cctx.kernalContext()
                .pdsFolderResolver()
                .resolveFolders()
                .folderName() + SNAPSHOT_META_EXTENSION);

        if (!metaFile.exists())
            throw new IgniteCheckedException("Snapshot meta doesn't exists on local node for a given snapshot: " + snpLocDir.getName());

        SnapshotMeta meta;

        try (InputStream stream = new BufferedInputStream(new FileInputStream(metaFile))) {
            meta = marshaller.unmarshal(stream, U.resolveClassLoader(cctx.gridConfig()));
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        if (meta == null)
            throw new IgniteCheckedException("Snapshot meta hasn't been resolved on local node for a given snapshot: " + snpLocDir.getName());

        return meta;
    }

    /**
     * @param meta Snapshot meta to check.
     * @param blt Current baseline topology.
     * @throws IgniteCheckedException If validation fails.
     */
    private static void validateSnapshotClusterTopology(SnapshotMeta meta, BaselineTopology blt) throws IgniteCheckedException {
        Set<String> currConsIds = blt.consistentIds()
            .stream()
            .map(Object::toString)
            .collect(Collectors.toSet());

        Set<String> snpConsIds = meta.hist.consistentIds()
            .stream()
            .map(Object::toString)
            .collect(Collectors.toSet());

        if (!currConsIds.equals(snpConsIds)) {
            throw new IgniteCheckedException("Snapshot can be restored only on the same cluster topology " +
                "[currConsIds=" + currConsIds + ", snapshotConsIds=" + snpConsIds + ']');
        }
    }

    /**
     * @param cctx Shared cache context to validate.
     */
    private static void validateSnapshotCachesDestroyed(GridCacheSharedContext<?, ?> cctx) throws IgniteCheckedException {
        Collection<String> userCaches = cctx.cache().publicCacheNames();

        if (!userCaches.isEmpty()) {
            throw new IgniteCheckedException("Snapshot can be restored only when cluster node doesn't contains " +
                "any of user caches [userCaches=" + userCaches + ']');
        }
    }

    /**
     * @param snpName Snapshot name which offered to restore.
     * @return Future which will be completed on restore operation prepared.
     */
    IgniteInternalFuture<Boolean> prepareSnapshotRestore(String snpName) {
        // todo disable any further activation requests

        if (log.isInfoEnabled()) {
            log.info("Start preparation of snapshot restore operation on local node " +
                "[snpName=" + snpName + ", nodeId=" + cctx.localNode().id() + ']');
        }

        if (cctx.kernalContext().clientNode())
            return new GridFinishedFuture<>();

        try {
            validateSnapshotClusterTopology(readNodeSnaspshotMeta(snapshotLocalDir(snpName)), getBaselineTopology(cctx));
            validateSnapshotCachesDestroyed(cctx);

            restoringSnpName = snpName;
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }

        return new GridFinishedFuture<>(Boolean.TRUE);
    }

    /**
     * @param id Restore process id.
     * @param res Results of prepare restore process.
     * @param err Errors if occurred.
     */
    void prepareSnapshotRestoreResult(UUID id, Map<UUID, Boolean> res, Map<UUID, Exception> err) {
        Optional<Exception> err0 = err.values()
            .stream()
            .filter(Objects::nonNull)
            .findAny();

        if (log.isInfoEnabled()) {
            log.info("End preparation of snapshot restore operation on local node " +
                "[snpName=" + restoringSnpName + ", nodeId=" + cctx.localNode().id() +
                ", err=" + err0.orElse(null) + ']');
        }

        synchronized (snpOpMux) {
            if (err0.isPresent()) {
                if (restoreSnpFut != null) {
                    restoreSnpFut.onDone(err0.get());

                    restoreSnpFut = null;
                }
            }
            else if (isLocalNodeCoordinator(cctx.discovery()))
                restoreSnpProc.start(id, restoringSnpName);
        }
    }

    /**
     * @param snpName Snapshot name which offered to restore.
     * @return Future which will be completed on restore operation prepared.
     */
    IgniteInternalFuture<Boolean> snapshotRestoreTask(String snpName) {
        if (log.isInfoEnabled())
            log.info("Start snapshot restore on local node [snpName=" + snpName + ", nodeId=" + cctx.localNode().id() + ']');

        return new GridFinishedFuture<>();
    }

    /**
     * @param id Restore process id.
     * @param res Results of prepare restore process.
     * @param err Errors if occurred.
     */
    void snapshotRestoreTaskResult(UUID id, Map<UUID, Boolean> res, Map<UUID, Exception> err) {
        if (log.isInfoEnabled())
            log.info("Finish snapshot restore on local node [snpName=" + restoringSnpName + ", nodeId=" + cctx.localNode().id() + ']');

        if (err.isEmpty() && restoreSnpFut != null)
            restoreSnpFut.onDone();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> restoreSnapshot(String name) {
        if (cctx.kernalContext().clientNode()) {
            return new IgniteFinishedFutureImpl<>(new UnsupportedOperationException("Client and daemon nodes can not " +
                "perform snapshot restore operation."));
        }

        if (active(cctx.kernalContext().state().clusterState().state())) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Snapshot restore operation has been rejected. " +
                "Restore must be performed on deactivated cluster."));
        }

        if (!IgniteFeatures.allNodesSupports(cctx.discovery().allNodes(), PERSISTENCE_CACHE_SNAPSHOT)) {
            return new IgniteFinishedFutureImpl<>(new IllegalStateException("Not all nodes in the cluster support " +
                "a snapshot operation."));
        }

        synchronized (snpOpMux) {
            try {
                if (restoreSnpFut != null && !restoreSnpFut.isDone()) {
                    throw new IgniteCheckedException("Snapshot restore operation has been rejected. " +
                        "The previous snapshot restore was not completed gracefully.");
                }

                validateSnapshotClusterTopology(readNodeSnaspshotMeta(snapshotLocalDir(name)), getBaselineTopology(cctx));
                validateSnapshotCachesDestroyed(cctx);

                ClusterSnapshotFuture restoreFut0 = new ClusterSnapshotFuture(UUID.randomUUID());

                restoreSnpFut = restoreFut0;

                prepareRestoreSnpProc.start(restoreFut0.id, name);

                if (log.isInfoEnabled())
                    log.info("Cluster-wide snapshot restore operation started [snpName=" + name + ']');

                return new IgniteFutureImpl<>(restoreSnpFut);
            }
            catch (IgniteCheckedException e) {
                return new IgniteFinishedFutureImpl<>(e);
            }
        }
    }

    /**
     * @return {@code True} if snapshot operation started.
     */
    public boolean snapshotInProgress() {
        return clusterSnpTask != null;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Void> createSnapshot(String name, List<Integer> grps) {
        if (cctx.kernalContext().clientNode()) {
            return new IgniteFinishedFutureImpl<>(new UnsupportedOperationException("Client and daemon nodes can not " +
                "perform this operation."));
        }

        if (!IgniteFeatures.allNodesSupports(cctx.discovery().allNodes(), PERSISTENCE_CACHE_SNAPSHOT)) {
            return new IgniteFinishedFutureImpl<>(new IllegalStateException("Not all nodes in the cluster support " +
                "a snapshot operation."));
        }

        if (!active(cctx.kernalContext().state().clusterState().state())) {
            return new IgniteFinishedFutureImpl<>(new IgniteException("Snapshot operation has been rejected. " +
                "The cluster is inactive."));
        }

        // todo check all of baseline nodes are alive (here and on start)

        synchronized (snpOpMux) {
            if (clusterSnpFut != null && !clusterSnpFut.isDone()) {
                return new IgniteFinishedFutureImpl<>(new IgniteException("Create snapshot request has been rejected. " +
                    "The previous snapshot operation was not completed."));
            }

            if (clusterSnpTask != null) {
                return new IgniteFinishedFutureImpl<>(new IgniteException("Create snapshot request has been rejected. " +
                    "Parallel snapshot processes are not allowed."));
            }

            ClusterSnapshotFuture clsFut = new ClusterSnapshotFuture(UUID.randomUUID());

            clusterSnpFut = clsFut;

            takeSnpProc.start(clsFut.id, new SnapshotOperationRequest(name, grps));


            if(log.isInfoEnabled())
                log.info("Cluster-wide snapshot operation started [snpName=" + name + ", gprs=" + grps + ']');

            return new IgniteFutureImpl<>(clusterSnpFut);
        }
    }

    /** {@inheritDoc} */
    @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        // This method is called under exchange thread, order is guarandeed
        if (clusterSnpTask == null || cctx.kernalContext().clientNode())
            return;

        SnapshotFutureTask snpTask = clusterSnpTask;

        snpTask.start();

        dbMgr.forceCheckpoint(String.format(SNAPSHOT_CP_REASON, snpTask.snapshotName()));

        // schedule task on checkpoint and wait when it starts
        try {
            snpTask.awaitStarted();
        }
        catch (IgniteCheckedException e) {
            snpTask.acceptException(e);

            U.error(log, "Fail to wait while cluster-wide snapshot operation started", e);
        }
    }

    /**
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param rmtNodeId The remote node to connect to.
     * @param partConsumer Received partition handler.
     * @return Snapshot name.
     */
    public IgniteInternalFuture<Boolean> createRemoteSnapshot(
        UUID rmtNodeId,
        Map<Integer, Set<Integer>> parts,
        BiConsumer<File, GroupPartitionId> partConsumer
    ) {
        ClusterNode rmtNode = cctx.discovery().node(rmtNodeId);

        if (!nodeSupports(rmtNode, PERSISTENCE_CACHE_SNAPSHOT))
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot on remote node is not supported: " + rmtNode.id()));

        if (rmtNode == null) {
            return new GridFinishedFuture<>(new ClusterTopologyCheckedException("Snapshot request cannot be performed. " +
                "Remote node left the grid [rmtNodeId=" + rmtNodeId + ']'));
        }

        for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
            int grpId = e.getKey();

            GridDhtPartitionMap partMap = cctx.cache()
                .cacheGroup(grpId)
                .topology()
                .partitions(rmtNodeId);

            Set<Integer> owningParts = partMap.entrySet()
                .stream()
                .filter(p -> p.getValue() == GridDhtPartitionState.OWNING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            if (!owningParts.containsAll(e.getValue())) {
                Set<Integer> substract = new HashSet<>(e.getValue());

                substract.removeAll(owningParts);

                return new GridFinishedFuture<>(new IgniteCheckedException("Only owning partitions allowed to be " +
                    "requested from the remote node [rmtNodeId=" + rmtNodeId + ", grpId=" + grpId +
                    ", missed=" + substract + ']'));
            }
        }

        String snpName = "snapshot_" + UUID.randomUUID().toString();

        RemoteSnapshotFuture snpTransFut = new RemoteSnapshotFuture(rmtNodeId, snpName,
            parts.values().stream().mapToInt(Set::size).sum(), partConsumer);

        busyLock.enterBusy();
        SnapshotRequestMessage msg0;

        try {
            msg0 = new SnapshotRequestMessage(snpName,
                parts.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> GridIntList.valueOf(e.getValue()))));

            RemoteSnapshotFuture fut = rmtSnpReq.get();

            try {
                if (fut != null)
                    fut.get(DFLT_SNAPSHOT_TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info("The previous snapshot request finished with an excpetion:" + e.getMessage());
            }

            try {
                if (rmtSnpReq.compareAndSet(null, snpTransFut)) {
                    cctx.gridIO().sendOrderedMessage(rmtNode, DFLT_INITIAL_SNAPSHOT_TOPIC, msg0, SYSTEM_POOL,
                        Long.MAX_VALUE, true);
                }
                else
                    return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot request has been concurrently interrupted."));

            }
            catch (IgniteCheckedException e) {
                rmtSnpReq.compareAndSet(snpTransFut, null);

                return new GridFinishedFuture<>(e);
            }
        }
        finally {
            busyLock.leaveBusy();
        }

        if (log.isInfoEnabled()) {
            log.info("Snapshot request is sent to the remote node [rmtNodeId=" + rmtNodeId +
                ", msg0=" + msg0 + ", snpTransFut=" + snpTransFut +
                ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
        }

        return snpTransFut;
    }

    /**
     * @param grps List of cache groups which will be destroyed.
     */
    public void onCacheGroupsStopped(List<Integer> grps) {
        for (SnapshotFutureTask sctx : locSnpTasks.values()) {
            Set<Integer> snpGrps = sctx.partitions().stream()
                .map(GroupPartitionId::getGroupId)
                .collect(Collectors.toSet());

            Set<Integer> retain = new HashSet<>(grps);
            retain.retainAll(snpGrps);

            if (!retain.isEmpty()) {
                sctx.acceptException(new IgniteCheckedException("Snapshot has been interrupted due to some of the required " +
                    "cache groups stopped: " + retain));
            }
        }
    }

    /**
     * @param snpName Unique snapshot name.
     * @param srcNodeId Node id which cause snapshot operation.
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param snpSndr Sender which used for snapshot sub-task processing.
     * @return Future which will be completed when snapshot is done.
     */
    SnapshotFutureTask startLocalSnapshotTask(
        String snpName,
        UUID srcNodeId,
        Map<Integer, GridIntList> parts,
        Executor exec,
        SnapshotFileSender snpSndr
    ) {
        if (!busyLock.enterBusy())
            return new SnapshotFutureTask(new IgniteCheckedException("Snapshot manager is stopping [locNodeId=" + cctx.localNodeId() + ']'));

        try {
            SnapshotFutureTask snpFutTask = startSnapshotTask(snpName, cctx.localNodeId(), parts, snpSndr);

            // Snapshot is still in the INIT state. beforeCheckpoint has been skipped
            // due to checkpoint aready running and we need to schedule the next one
            // right afther current will be completed.
            dbMgr.forceCheckpoint(String.format(SNAPSHOT_CP_REASON, snpName));

            snpFutTask.awaitStarted();

            return snpFutTask;
        }
        catch (IgniteCheckedException e) {
            return new SnapshotFutureTask(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param snpName Unique snapshot name.
     * @param srcNodeId Node id which cause snapshot operation.
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @return Snapshot operation task which should be registered on checkpoint to run.
     */
    private SnapshotFutureTask startSnapshotTask(
        String snpName,
        UUID srcNodeId,
        Map<Integer, GridIntList> parts,
        SnapshotFileSender snpSndr
    ) {
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
                parts));

        if (prev != null)
            return new SnapshotFutureTask(new IgniteCheckedException("Snapshot with requested name is already scheduled: " + snpName));

        snpFutTask.listen(f -> locSnpTasks.remove(snpName));

        snpFutTask.start();

        return snpFutTask;
    }

    /**
     * @param snpName Snapshot name to associate sender with.
     * @return Snapshot receiver instance.
     */
    SnapshotFileSender localSnapshotSender(String snpName) throws IgniteCheckedException {
        File snpLocDir = snapshotLocalDir(snpName);

        // This will be called inside discovery thread which initiates snapshot operation.
        // So discoCache will be calculated correctly.
        BaselineTopologyHistoryItem hist = BaselineTopologyHistoryItem.fromBaseline(getBaselineTopology(cctx));

        if (hist == null)
            throw new IgniteCheckedException("Snapshot operation is allowed only for baseline toplogy enabled.");

        return new LocalSnapshotFileSender(log,
            snpRunner,
            () -> {
                U.ensureDirectory(snpLocDir, "snapshot local directory", log);

                // Write snapshot meta file
                PdsFolderSettings settings = cctx.kernalContext().pdsFolderResolver().resolveFolders();

                File metaFile = new File(snpLocDir,  settings.folderName() + SNAPSHOT_META_EXTENSION);

                if (metaFile.exists())
                    throw new IgniteCheckedException("Snapshot meta is already exist and cannot be written: " + snpName);

                try {
                    metaFile.createNewFile();
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }

                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(metaFile))) {
                    U.marshal(marshaller, new SnapshotMeta(snpName, hist), out);
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }

                return U.resolveWorkDirectory(snpLocDir.getAbsolutePath(), relativeNodePath(settings), false);
            },
            ioFactory,
            storeFactory,
            types -> cctx.kernalContext()
                .cacheObjects()
                .saveMetadata(types, snpLocDir),
            mappings -> saveMappings(cctx.kernalContext(), mappings, snpLocDir),
            pageSize);
    }

    /**
     * @param snpName Snapshot name.
     * @param rmtNodeId Remote node id to send snapshot to.
     * @return Snapshot sender instance.
     */
    SnapshotFileSender remoteSnapshotSender(String snpName, UUID rmtNodeId) {
        // Remote snapshots can be send only by single threaded executor since only one transmissionSender created.
        return new RemoteSnapshotFileSender(log,
            new SequentialExecutorWrapper(log, snpRunner),
            () -> relativeNodePath(cctx.kernalContext().pdsFolderResolver().resolveFolders()),
            cctx.gridIO().openTransmissionSender(rmtNodeId, DFLT_INITIAL_SNAPSHOT_TOPIC),
            errMsg -> cctx.gridIO().sendToCustomTopic(rmtNodeId,
                DFLT_INITIAL_SNAPSHOT_TOPIC,
                new SnapshotResponseMessage(snpName, errMsg),
                SYSTEM_POOL),
            snpName);
    }

    /**
     * @return The executor service used to run snapshot tasks.
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
     * @param nodeId Remote node id on which requests has been registered.
     * @return Snapshot future related to given node id.
     */
    SnapshotFutureTask lastScheduledRemoteSnapshotTask(UUID nodeId) {
        return locSnpTasks.values().stream()
            .filter(t -> t.type() == RemoteSnapshotFileSender.class && t.sourceNodeId().equals(nodeId))
            .findFirst()
            .orElse(null);
    }

    /**
     * @param rslvr RDS resolver.
     * @param dirPath Relative working directory path.
     * @param errorMsg Error message in case of make direcotry fail.
     * @return Resolved working direcory.
     * @throws IgniteCheckedException If fails.
     */
    private static File initWorkDirectory(
        PdsFolderSettings rslvr,
        String dirPath,
        IgniteLogger log,
        String errorMsg
    ) throws IgniteCheckedException {
        File rmtSnpDir = U.resolveWorkDirectory(rslvr.persistentStoreRootPath().getAbsolutePath(), dirPath, false);

        File target = new File(rmtSnpDir, rslvr.folderName());

        U.ensureDirectory(target, errorMsg, log);

        return target;
    }

    /**
     * @param dbNodePath Persistence node path.
     * @param snpName Snapshot name.
     * @param cacheDirName Cache directory name.
     * @return Relative cache path.
     */
    private static String cacheSnapshotPath(String snpName, String dbNodePath, String cacheDirName) {
        return Paths.get(snpName, dbNodePath, cacheDirName).toString();
    }

    /**
     * Snapshot meta to write to disk.
     */
    private static class SnapshotMeta implements Serializable {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Snapshot name. */
        private final String snpName;

        /** Baseline topology at the moment of snapshot creation. */
        private final BaselineTopologyHistoryItem hist;

        /**
         * @param snpName Snapshot name.
         * @param hist aseline topology at the moment of snapshot creation.
         */
        public SnapshotMeta(String snpName, BaselineTopologyHistoryItem hist) {
            this.snpName = snpName;
            this.hist = hist;
        }

        /**
         * @return Snapshot name.
         */
        public String snapshotName() {
            return snpName;
        }

        /**
         * @return Baseline on which snapshot has been created.
         */
        public BaselineTopologyHistoryItem getHist() {
            return hist;
        }
    }

    /**
     *
     */
    private static class SnapshotFuture extends GridFutureAdapter<Boolean> {
        /** Snapshot name to create. */
        protected final String snpName;

        /**
         * @param snpName Snapshot name to create.
         */
        public SnapshotFuture(String snpName) {
            this.snpName = snpName;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SnapshotFuture future = (SnapshotFuture)o;

            return Objects.equals(snpName, future.snpName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(snpName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SnapshotFuture.class, this);
        }
    }

    /**
     *
     */
    private class RemoteSnapshotFuture extends SnapshotFuture {
        /** Remote node id to request snapshot from. */
        private final UUID rmtNodeId;

        /** Collection of partition to be received. */
        private final Map<GroupPartitionId, FilePageStore> stores = new ConcurrentHashMap<>();

        private final BiConsumer<File, GroupPartitionId> partConsumer;

        /** Counter which show how many partitions left to be received. */
        private final AtomicInteger partsLeft;

        /**
         * @param cnt Partitions to receive.
         */
        public RemoteSnapshotFuture(UUID rmtNodeId, String snpName, int cnt, BiConsumer<File, GroupPartitionId> partConsumer) {
            super(snpName);

            this.rmtNodeId = rmtNodeId;
            partsLeft = new AtomicInteger(cnt);
            this.partConsumer = partConsumer;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            if (onCancelled()) {
                // Close non finished file storages
                for (Map.Entry<GroupPartitionId, FilePageStore> entry : stores.entrySet()) {
                    FilePageStore store = entry.getValue();

                    try {
                        store.stop(true);
                    }
                    catch (StorageException e) {
                        log.warning("Error stopping received file page store", e);
                    }
                }
            }

            return isCancelled();
        }

        /** {@inheritDoc} */
        @Override protected boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
            assert err != null || cancel || stores.isEmpty() : "Not all file storages processed: " + stores;

            rmtSnpReq.compareAndSet(this, null);

            return super.onDone(res, err, cancel);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoteSnapshotFuture.class, this);
        }
    }

    /**
     * Such an executor can executes tasks not in a single thread, but executes them
     * on different threads sequentially. It's important for some {@link SnapshotFileSender}'s
     * to process sub-task sequentially due to all these sub-tasks may share a signle socket
     * channel to send data to.
     */
    private static class SequentialExecutorWrapper implements Executor {
        /** Ignite logger. */
        private final IgniteLogger log;

        /** Queue of task to execute. */
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        /** Delegate executor. */
        private final Executor executor;

        /** Currently running task. */
        private volatile Runnable active;

        /** If wrapped executor is shutting down. */
        private volatile boolean stopping;

        /**
         * @param executor Executor to run tasks on.
         */
        public SequentialExecutorWrapper(IgniteLogger log, Executor executor) {
            this.log = log.getLogger(SequentialExecutorWrapper.class);
            this.executor = executor;
        }

        /** {@inheritDoc} */
        @Override public synchronized void execute(final Runnable r) {
            assert !stopping : "Task must be cancelled prior to the wrapped executor is shutting down.";

            tasks.offer(() -> {
                try {
                    r.run();
                }
                finally {
                    scheduleNext();
                }
            });

            if (active == null)
                scheduleNext();
        }

        /** */
        protected synchronized void scheduleNext() {
            if ((active = tasks.poll()) != null) {
                try {
                    executor.execute(active);
                }
                catch (RejectedExecutionException e) {
                    tasks.clear();

                    stopping = true;

                    log.warning("Task is outdated. Wrapped executor is shutting down.", e);
                }
            }
        }
    }

    /**
     *
     */
    private static class RemoteSnapshotFileSender extends SnapshotFileSender {
        /** The sender which sends files to remote node. */
        private final GridIoManager.TransmissionSender sndr;

        /** Error handler which will be triggered in case of transmission sedner not started yet. */
        private final IgniteThrowableConsumer<String> errHnd;

        /** Relative node path initializer. */
        private final IgniteThrowableSupplier<String> initPath;

        /** Snapshot name */
        private final String snpName;

        /** Local node persistent directory with consistent id. */
        private String relativeNodePath;

        /**
         * @param log Ignite logger.
         * @param sndr File sender instance.
         * @param errHnd Snapshot error handler if transmission sender not started yet.
         * @param snpName Snapshot name.
         */
        public RemoteSnapshotFileSender(
            IgniteLogger log,
            Executor exec,
            IgniteThrowableSupplier<String> initPath,
            GridIoManager.TransmissionSender sndr,
            IgniteThrowableConsumer<String> errHnd,
            String snpName
        ) {
            super(log, exec);

            this.sndr = sndr;
            this.errHnd = errHnd;
            this.snpName = snpName;
            this.initPath = initPath;
        }

        /** {@inheritDoc} */
        @Override protected void init() throws IgniteCheckedException {
            relativeNodePath = initPath.get();

            if (relativeNodePath == null)
                throw new IgniteException("Relative node path cannot be empty.");
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
            try {
                assert part.exists();
                assert len > 0 : "Requested partitions has incorrect file length " +
                    "[pair=" + pair + ", cacheDirName=" + cacheDirName + ']';

                sndr.send(part, 0, len, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.FILE);

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
            try {
                sndr.send(delta, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.CHUNK);

                if (log.isInfoEnabled())
                    log.info("Delta pages storage has been send [part=" + delta.getName() + ", pair=" + pair + ']');
            }
            catch (TransmissionCancelledException e) {
                if (log.isInfoEnabled()) {
                    log.info("Transmission delta pages has been interrupted [part=" + delta.getName() +
                        ", pair=" + pair + ']');
                }
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                U.error(log, "Error sending delta file  [part=" + delta.getName() + ", pair=" + pair + ']', e);

                throw new IgniteException(e);
            }
        }

        /**
         * @param cacheDirName Cache directory name.
         * @param pair Cache group id with corresponding partition id.
         * @return Map of params.
         */
        private Map<String, Serializable> transmissionParams(String snpName, String cacheDirName,
            GroupPartitionId pair) {
            Map<String, Serializable> params = new HashMap<>();

            params.put(SNP_GRP_ID_PARAM, pair.getGroupId());
            params.put(SNP_PART_ID_PARAM, pair.getPartitionId());
            params.put(SNP_DB_NODE_PATH_PARAM, relativeNodePath);
            params.put(SNP_CACHE_DIR_NAME_PARAM, cacheDirName);
            params.put(SNP_NAME_PARAM, snpName);

            return params;
        }

        /** {@inheritDoc} */
        @Override public void close0(@Nullable Throwable th) {
            try {
                if (th != null && !sndr.opened())
                    errHnd.accept(th.getMessage());
            }
            catch (IgniteCheckedException e) {
                th.addSuppressed(e);
            }

            U.closeQuiet(sndr);

            if (th == null) {
                if (log.isInfoEnabled())
                    log.info("The remote snapshot sender closed normally [snpName=" + snpName + ']');
            }
            else {
                U.warn(log, "The remote snapshot sender closed due to an error occurred while processing " +
                    "snapshot operation [snpName=" + snpName + ']', th);
            }
        }
    }

    /**
     *
     */
    private static class LocalSnapshotFileSender extends SnapshotFileSender {
        /**
         * Local node snapshot directory calculated on snapshot directory.
         */
        private File dbNodeSnpDir;

        /** Facotry to produce IO interface over a file. */
        private final FileIOFactory ioFactory;

        /** Factory to create page store for restore. */
        private final BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory;

        /** Store binary files. */
        private final Consumer<Collection<BinaryType>> binaryWriter;

        /** Marshaller mapping writer. */
        private final Consumer<List<Map<Integer, MappedName>>> mappingWriter;

        /** Size of page. */
        private final int pageSize;

        /** Additional snapshot meta information which will be written on disk. */
        private final IgniteThrowableSupplier<File> initPath;

        /**
         * @param log Ignite logger to use.
         * @param ioFactory Facotry to produce IO interface over a file.
         * @param storeFactory Factory to create page store for restore.
         * @param pageSize Size of page.
         */
        public LocalSnapshotFileSender(
            IgniteLogger log,
            Executor exec,
            IgniteThrowableSupplier<File> initPath,
            FileIOFactory ioFactory,
            BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory,
            Consumer<Collection<BinaryType>> binaryWriter,
            Consumer<List<Map<Integer, MappedName>>> mappingWriter,
            int pageSize
        ) {
            super(log, exec);

            this.ioFactory = ioFactory;
            this.storeFactory = storeFactory;
            this.pageSize = pageSize;
            this.binaryWriter = binaryWriter;
            this.mappingWriter = mappingWriter;
            this.initPath = initPath;
        }

        /** {@inheritDoc} */
        @Override protected void init() throws IgniteCheckedException {
            dbNodeSnpDir = initPath.get();

            if (dbNodeSnpDir == null)
                throw new IgniteException("Local snapshot directory cannot be null");
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig0(File ccfg, String cacheDirName) {
            assert dbNodeSnpDir != null;

            try {
                File cacheDir = U.resolveWorkDirectory(dbNodeSnpDir.getAbsolutePath(), cacheDirName, false);

                copy(ccfg, new File(cacheDir, ccfg.getName()), ccfg.length());
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta0(List<Map<Integer, MappedName>> mappings) {
            if (mappings == null)
                return;

            mappingWriter.accept(mappings);
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta0(Collection<BinaryType> types) {
            if (types == null)
                return;

            binaryWriter.accept(types);
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
            try {
                if (len == 0)
                    return;

                File cacheDir = U.resolveWorkDirectory(dbNodeSnpDir.getAbsolutePath(), cacheDirName, false);

                File snpPart = new File(cacheDir, part.getName());

                if (!snpPart.exists() || snpPart.delete())
                    snpPart.createNewFile();

                copy(part, snpPart, len);

                if (log.isInfoEnabled()) {
                    log.info("Partition has been snapshotted [snapshotDir=" + dbNodeSnpDir.getAbsolutePath() +
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
            File snpPart = getPartitionFile(dbNodeSnpDir, cacheDirName, pair.getPartitionId());

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

                    long pageId = PageIO.getPageId(pageBuf);

                    int crc32 = FastCrc.calcCrc(pageBuf, pageBuf.limit());

                    int crc = PageIO.getCrc(pageBuf);

                    if (log.isDebugEnabled()) {
                        log.debug("Read page given delta file [path=" + delta.getName() +
                            ", pageId=" + pageId + ", pos=" + pos + ", pages=" + (totalBytes / pageSize) +
                            ", crcBuff=" + crc32 + ", crcPage=" + crc + ']');
                    }

                    pageBuf.rewind();

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
                    log.info("Local snapshot sender closed, resouces released [dbNodeSnpDir=" + dbNodeSnpDir + ']');
            }
            else {
                dbNodeSnpDir.delete();

                U.error(log, "Local snapshot sender closed due to an error occurred", th);
            }
        }

        /**
         * @param from Copy from file.
         * @param to Copy data to file.
         * @param length Number of bytes to copy from beginning.
         * @throws IOException If fails.
         */
        private void copy(File from, File to, long length) throws IOException {
            try (FileIO src = ioFactory.create(from, READ);
                 FileChannel dest = new FileOutputStream(to).getChannel()) {
                if (src.size() < length)
                    throw new IgniteException("The source file to copy has to enought length [expected=" + length + ", actual=" + src.size() + ']');

                src.position(0);

                long written = 0;

                while (written < length)
                    written += src.transferTo(written, length - written, dest);
            }
        }
    }

    /** Snapshot start operation request. */
    private static class SnapshotOperationRequest implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Snapshot name. */
        private final String snpName;

        /** The list of cache groups to include into snapshot. */
        private final List<Integer> grpIds;

        /**
         * @param snpName Snapshot name.
         * @param grpIds Cache groups to include into snapshot.
         */
        public SnapshotOperationRequest(String snpName, List<Integer> grpIds) {
            this.snpName = snpName;
            this.grpIds = grpIds;
        }

        /**
         * @return Snapshot name.
         */
        public String snapshotName() {
            return snpName;
        }

        /**
         * @return Cache groups included into snapshot.
         */
        public List<Integer> cacheGroups() {
            return grpIds;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SnapshotOperationRequest request = (SnapshotOperationRequest)o;

            return Objects.equals(snpName, request.snpName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(snpName);
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

    /**
     * Snapshot operation start message.
     */
    private static class SnapshotStartDiscoveryMessage implements SnapshotDiscoveryMessage {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Discovery cache. */
        private final DiscoCache discoCache;

        /** Snapshot request id */
        private final IgniteUuid id;

        /**
         * @param discoCache Discovery cache.
         * @param id Snapshot request id.
         */
        public SnapshotStartDiscoveryMessage(DiscoCache discoCache, UUID id) {
            this.discoCache = discoCache;
            this.id = new IgniteUuid(id, 0);
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
        @Override public IgniteUuid id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public @Nullable DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean stopProcess() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            return this.discoCache;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SnapshotStartDiscoveryMessage message = (SnapshotStartDiscoveryMessage)o;

            return id.equals(message.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SnapshotStartDiscoveryMessage.class, this);
        }
    }

    /**
     *
     */
    private static class ClusterSnapshotFuture extends GridFutureAdapter<Void> {
        /** Initial request id. */
        private final UUID id;

        /**
         * @param id Initial request id.
         */
        public ClusterSnapshotFuture(UUID id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof ClusterSnapshotFuture))
                return false;

            ClusterSnapshotFuture future = (ClusterSnapshotFuture)o;

            return Objects.equals(id, future.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ClusterSnapshotFuture{" +
                "id=" + id +
                '}';
        }
    }
}
