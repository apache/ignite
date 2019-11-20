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

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.MarshallerMappingWriter;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.communication.TransmissionCancelledException;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageWriteListener;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
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
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cacheobject.BinaryTypeWriter;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.internal.IgniteFeatures.PERSISTENCE_CACHE_SNAPSHOT;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.MarshallerContextImpl.addPlatformMappings;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileName;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getFlagByPartId;

/** */
public class IgniteSnapshotManager extends GridCacheSharedManagerAdapter {
    /** File with delta pages suffix. */
    public static final String DELTA_SUFFIX = ".delta";

    /** File name template consists of delta pages. */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** File name template for index delta pages. */
    public static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** The reason of checkpoint start for needs of bakcup. */
    public static final String SNAPSHOT_CP_REASON = "Wakeup for checkpoint to take snapshot [name=%s]";

    /** Default working directory for snapshot temporary files. */
    public static final String DFLT_LOCAL_SNAPSHOT_DIRECTORY = "snapshots";

    /** Default snapshot directory for loading remote snapshots. */
    public static final String DFLT_SNAPSHOT_WORK_DIRECTORY = "snp";

    /** Prefix for snapshot threads. */
    private static final String SNAPSHOT_RUNNER_THREAD_PREFIX = "snapshot-runner";

    /** Total number of thread to perform local snapshot. */
    private static final int SNAPSHOT_THREAD_POOL_SIZE = 4;

    /** Default snapshot topic to receive snapshots from remote node. */
    private static final Object DFLT_INITIAL_SNAPSHOT_TOPIC = GridTopic.TOPIC_SNAPSHOT.topic("0");

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
    private final ConcurrentMap<String, LocalSnapshotContext> localSnpCtxs = new ConcurrentHashMap<>();

    /** Lock to protect the resources is used. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Requested snapshot from remote node. */
    private final AtomicReference<SnapshotTransmissionFuture> snpRq = new AtomicReference<>();

    /** Main snapshot directory to save created snapshots. */
    private File locSnpDir;

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
    private IgniteThreadPoolExecutor snpRunner;

    /** Checkpoint listener to handle scheduled snapshot requests. */
    private DbCheckpointListener cpLsnr;

    /** Snapshot listener on created snapshots. */
    private volatile SnapshotListener snpLsnr;

    /** Database manager for enabled persistence. */
    private GridCacheDatabaseSharedManager dbMgr;

    /** Configured data storage page size. */
    private int pageSize;

    /**
     * @param ctx Kernal context.
     */
    public IgniteSnapshotManager(GridKernalContext ctx) {
        // No-op.
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

        if (kctx.clientNode())
            return;

        if (!CU.isPersistenceEnabled(cctx.kernalContext().config()))
            return;

        pageSize = kctx.config()
            .getDataStorageConfiguration()
            .getPageSize();

        assert pageSize > 0;

        snpRunner = new IgniteThreadPoolExecutor(
            SNAPSHOT_RUNNER_THREAD_PREFIX,
            cctx.igniteInstanceName(),
            SNAPSHOT_THREAD_POOL_SIZE,
            SNAPSHOT_THREAD_POOL_SIZE,
            30_000,
            new LinkedBlockingQueue<>(),
            SYSTEM_POOL,
            (t, e) -> kctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e)));

        assert cctx.pageStore() instanceof FilePageStoreManager;

        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        // todo must be available on storage configuration
        locSnpDir = U.resolveWorkDirectory(kctx.config().getWorkDirectory(), DFLT_LOCAL_SNAPSHOT_DIRECTORY, false);
        tmpWorkDir = Paths.get(storeMgr.workDir().getAbsolutePath(), DFLT_SNAPSHOT_WORK_DIRECTORY).toFile();

        U.ensureDirectory(locSnpDir, "local snapshots directory", log);
        U.ensureDirectory(tmpWorkDir, "work directory for snapshots creation", log);

        storeFactory = ((FilePageStoreManager)storeMgr)::getPageStoreFactory;
        dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        dbMgr.addCheckpointListener(cpLsnr = new DbCheckpointListener() {
            @Override public void beforeCheckpointBegin(Context ctx) {
                for (LocalSnapshotContext sctx0 : localSnpCtxs.values()) {
                    if (sctx0.started)
                        continue;

                    // Gather partitions metainfo for thouse which will be copied.
                    ctx.collectPartStat(sctx0.parts);
                }
            }

            @Override public void onMarkCheckpointBegin(Context ctx) {
                // No-op.
            }

            @Override public void onMarkCheckpointEnd(Context ctx) {
                // Under the write lock here. It's safe to add new stores.
                for (LocalSnapshotContext sctx0 : localSnpCtxs.values()) {
                    if (sctx0.started)
                        continue;

                    try {
                        PartitionAllocationMap allocationMap = ctx.partitionStatMap();

                        allocationMap.prepareForSnapshot();

                        for (GroupPartitionId pair : sctx0.parts) {
                            PagesAllocationRange allocRange = allocationMap.get(pair);

                            // Partition can be reserved.
                            // Partition can be MOVING\RENTING states.
                            // Index partition will be excluded if not all partition OWNING.
                            // There is no data assigned to partition, thus it haven't been created yet.
                            assert allocRange != null : "Partition counters has not been collected " +
                                "[pair=" + pair + ", snpName=" + sctx0.snpName +
                                ", part=" + cctx.cache().cacheGroup(pair.getGroupId()).topology()
                                .localPartition(pair.getPartitionId()) + ']';

                            PageStore store = storeMgr.getStore(pair.getGroupId(), pair.getPartitionId());

                            sctx0.partFileLengths.put(pair, store.size());
                            sctx0.partDeltaWriters.get(pair).init(allocRange.getCurrAllocatedPageCnt());
                        }
                    }
                    catch (IgniteCheckedException e) {
                        sctx0.snpFut.onDone(e);
                    }
                }
            }

            @Override public void onCheckpointBegin(Context ctx) {
                for (LocalSnapshotContext sctx0 : localSnpCtxs.values()) {
                    if (sctx0.started || sctx0.snpFut.isDone())
                        continue;

                    // Submit all tasks for partitions and deltas processing.
                    List<CompletableFuture<Void>> futs = new ArrayList<>();
                    FilePageStoreManager storeMgr = (FilePageStoreManager) cctx.pageStore();

                    if (log.isInfoEnabled())
                        log.info("Submit partition processings tasks with partition allocated lengths: " + sctx0.partFileLengths);

                    // Process binary meta.
                    futs.add(CompletableFuture.runAsync(
                        wrapExceptionally(() ->
                                sctx0.snpSndr.sendBinaryMeta(cctx.kernalContext()
                                    .cacheObjects()
                                    .metadataTypes()),
                            sctx0.snpFut),
                        sctx0.exec));

                    // Process marshaller meta.
                    futs.add(CompletableFuture.runAsync(
                        wrapExceptionally(() ->
                                sctx0.snpSndr.sendMarshallerMeta(cctx.kernalContext()
                                    .marshallerContext()
                                    .getCachedMappings()),
                            sctx0.snpFut),
                        sctx0.exec));

                    // Process partitions.
                    for (GroupPartitionId pair : sctx0.parts) {
                        CacheConfiguration ccfg = cctx.cache().cacheGroup(pair.getGroupId()).config();
                        String cacheDirName = cacheDirName(ccfg);
                        Long partLen = sctx0.partFileLengths.get(pair);

                        try {
                            // Initialize empty partition file.
                            if (partLen == 0) {
                                FilePageStore filePageStore = (FilePageStore)storeMgr.getStore(pair.getGroupId(),
                                    pair.getPartitionId());

                                filePageStore.init();
                            }
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }

                        CompletableFuture<Void> fut0 = CompletableFuture.runAsync(
                            wrapExceptionally(() -> {
                                    sctx0.snpSndr.sendPart(
                                        getPartitionFile(storeMgr.workDir(), cacheDirName, pair.getPartitionId()),
                                        cacheDirName,
                                        pair,
                                        partLen);

                                    // Stop partition writer.
                                    sctx0.partDeltaWriters.get(pair).markPartitionProcessed();
                                },
                                sctx0.snpFut),
                            sctx0.exec)
                            // Wait for the completion of both futures - checkpoint end, copy partition.
                            .runAfterBothAsync(sctx0.cpEndFut,
                                wrapExceptionally(() -> {
                                        File delta = getPartionDeltaFile(cacheWorkDir(sctx0.nodeSnpDir, cacheDirName),
                                            pair.getPartitionId());

                                        sctx0.snpSndr.sendDelta(delta, cacheDirName, pair);

                                        boolean deleted = delta.delete();

                                        assert deleted;
                                    },
                                    sctx0.snpFut),
                                sctx0.exec)
                            .thenRunAsync(
                                wrapExceptionally(() ->
                                        sctx0.snpSndr
                                            .sendCacheConfig(storeMgr.cacheConfiguration(ccfg), cacheDirName, pair),
                                    sctx0.snpFut),
                                sctx0.exec);

                        futs.add(fut0);
                    }

                    int futsSize = futs.size();

                    CompletableFuture.allOf(futs.toArray(new CompletableFuture[futsSize]))
                        .whenComplete((res, t) -> {
                            if (t == null)
                                sctx0.snpFut.onDone(true);
                            else
                                sctx0.snpFut.onDone(t);
                        });

                    sctx0.started = true;
                }
            }
        });

        // Receive remote snapshots requests.
        cctx.gridIO().addMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof RequestSnapshotMessage) {
                    if (!busyLock.enterBusy())
                        return;

                    RequestSnapshotMessage msg0 = (RequestSnapshotMessage) msg;

                    try {
                        String snpName = msg0.snapshotName();

                        scheduleSnapshot(snpName,
                            msg0.parts(),
                            new SerialExecutor(cctx.kernalContext()
                                .pools()
                                .poolForPolicy(plc)),
                            remoteSnapshotSender(snpName,
                                nodeId));
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to create remote snapshot [from=" + nodeId + ", msg=" + msg0 + ']');
                    }
                    finally {
                        busyLock.leaveBusy();
                    }
                }
            }
        });

        // Remote snapshot handler.
        cctx.kernalContext().io().addTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC, new TransmissionHandler() {
            /** {@inheritDoc} */
            @Override public void onException(UUID nodeId, Throwable err) {
                SnapshotTransmissionFuture fut = snpRq.get();

                if (fut == null)
                    return;

                if (fut.rmtNodeId.equals(nodeId)) {
                    fut.onDone(err);

                    snpRq.set(null);

                    if(snpLsnr != null)
                        snpLsnr.onException(nodeId, err);
                }
            }

            /** {@inheritDoc} */
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                Integer partId = (Integer)fileMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)fileMeta.params().get(SNP_NAME_PARAM);
                String rmtDbNodePath = (String)fileMeta.params().get(SNP_DB_NODE_PATH_PARAM);
                String cacheDirName = (String)fileMeta.params().get(SNP_CACHE_DIR_NAME_PARAM);

                SnapshotTransmissionFuture transFut = snpRq.get();

                if (transFut == null) {
                    throw new IgniteException("Snapshot transmission request is missing " +
                        "[snpName=" + snpName + ", cacheDirName=" + cacheDirName + ", partId=" + partId + ']');
                }

                assert transFut.snpName.equals(snpName) &&  transFut.rmtNodeId.equals(nodeId) :
                    "Another transmission in progress [fut=" + transFut + ", nodeId=" + snpName + ", nodeId=" + nodeId +']';

                if (transFut.isCancelled()) {
                    snpRq.compareAndSet(transFut, null);

                    throw new TransmissionCancelledException("Snapshot request is cancelled [snpName=" + snpName +
                        ", cacheDirName=" + cacheDirName + ", partId=" + partId + ']');
                }

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
             * @param rmtNodeId Remote node which sends partition.
             * @param snpName Snapshot name to notify listener with.
             * @param grpPartId Pair of group id and its partition id.
             */
            private void finishRecover(
                SnapshotTransmissionFuture snpTrans,
                UUID rmtNodeId,
                String snpName,
                GroupPartitionId grpPartId
            ) {
                FilePageStore pageStore = null;

                try {
                    pageStore = snpTrans.stores.remove(grpPartId);

                    pageStore.finishRecover();

                    String partAbsPath = pageStore.getFileAbsolutePath();

                    cctx.kernalContext().closure().runLocalSafe(() -> {
                        if (snpLsnr == null)
                            return;

                        snpLsnr.onPartition(rmtNodeId,
                            new File(partAbsPath),
                            grpPartId.getGroupId(),
                            grpPartId.getPartitionId());
                    });

                    if (snpTrans.partsLeft.decrementAndGet() == 0) {
                        cctx.kernalContext().closure().runLocalSafe(() -> {
                            if (snpLsnr == null)
                                return;

                            snpLsnr.onEnd(rmtNodeId);
                        });

                        snpTrans.onDone(true);

                        snpRq.compareAndSet(snpTrans, null);
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
                SnapshotTransmissionFuture transFut = snpRq.get();

                if (transFut == null) {
                    throw new IgniteException("Snapshot transmission with given name doesn't exists " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                assert transFut.snpName.equals(snpName) &&  transFut.rmtNodeId.equals(nodeId) :
                    "Another transmission in progress [fut=" + transFut + ", nodeId=" + snpName + ", nodeId=" + nodeId +']';

                FilePageStore pageStore = transFut.stores.get(grpPartId);

                if (pageStore == null) {
                    throw new IgniteException("Partition must be loaded before applying snapshot delta pages " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                // todo this should be inverted\hided to snapshot transmission
                pageStore.beginRecover();

                // No snapshot delta pages received. Finalize recovery.
                if (initMeta.count() == 0) {
                    finishRecover(transFut,
                        nodeId,
                        snpName,
                        grpPartId);
                }

                return new Consumer<ByteBuffer>() {
                    final LongAdder transferred = new LongAdder();

                    @Override public void accept(ByteBuffer buff) {
                        try {
                            assert initMeta.count() != 0 : initMeta;

                            if (transFut.isCancelled()) {
                                snpRq.compareAndSet(transFut, null);

                                throw new TransmissionCancelledException("Snapshot request is cancelled " +
                                    "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                            }

                            pageStore.write(PageIO.getPageId(buff), buff, 0, false);

                            transferred.add(buff.capacity());

                            if (transferred.longValue() == initMeta.count()) {
                                finishRecover(transFut,
                                    nodeId,
                                    snpName,
                                    grpPartId);
                            }
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

                SnapshotTransmissionFuture transFut = snpRq.get();

                if (transFut == null) {
                    throw new IgniteException("Snapshot transmission with given name doesn't exists " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                return new Consumer<File>() {
                    @Override public void accept(File file) {
                        if (transFut.isCancelled()) {
                            snpRq.compareAndSet(transFut, null);

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
            dbMgr.removeCheckpointListener(cpLsnr);

            for (LocalSnapshotContext ctx : localSnpCtxs.values())
                closeSnapshotResources(ctx);

            SnapshotTransmissionFuture fut = snpRq.get();

            if (fut != null) {
                fut.cancel();

                snpRq.compareAndSet(fut, null);
            }

            snpRunner.shutdown();

            cctx.kernalContext().io().removeMessageListener(DFLT_INITIAL_SNAPSHOT_TOPIC);
            cctx.kernalContext().io().removeTransmissionHandler(DFLT_INITIAL_SNAPSHOT_TOPIC);
        }
        finally {
            busyLock.unblock();
        }
    }

    /**
     * @param snpLsnr Snapshot listener instance.
     */
    public void addSnapshotListener(SnapshotListener snpLsnr) {
        this.snpLsnr = snpLsnr;
    }

    /**
     * @param snpName Snapshot name.
     * @return Local snapshot directory for snapshot with given name.
     */
    public File localSnapshotDir(String snpName) {
        return new File(localSnapshotWorkDir(), snpName);
    }

    /**
     * @return Snapshot directory used by manager for local snapshots.
     */
    public File localSnapshotWorkDir() {
        assert locSnpDir != null;

        return locSnpDir;
    }

    /**
     * @return Node snapshot working directory.
     */
    public File snapshotWorkDir() {
        assert tmpWorkDir != null;

        return tmpWorkDir;
    }

    /**
     * @return Node snapshot working directory with given snapshot name.
     */
    public File snapshotWorkDir(String snpName) {
        return new File(snapshotWorkDir(), snpName);
    }

    /**
     * @param snpName Unique snapshot name.
     * @return Future which will be completed when snapshot is done.
     */
    public IgniteInternalFuture<?> createLocalSnapshot(String snpName, List<Integer> grpIds) {
        // Collection of pairs group and appropratate cache partition to be snapshotted.
        Map<Integer, GridIntList> parts = grpIds.stream()
            .collect(Collectors.toMap(grpId -> grpId,
                grpId -> {
                    Set<Integer> grpParts = new HashSet<>();

                    cctx.cache()
                        .cacheGroup(grpId)
                        .topology()
                        .currentLocalPartitions()
                        .forEach(p -> grpParts.add(p.id()));

                    grpParts.add(INDEX_PARTITION);

                    return GridIntList.valueOf(grpParts);
                }));

        File rootSnpDir0 = localSnapshotDir(snpName);

        try {
            return scheduleSnapshot(snpName,
                parts,
                snpRunner,
                localSnapshotSender(rootSnpDir0));
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param rmtNodeId The remote node to connect to.
     * @return Snapshot name.
     * @throws IgniteCheckedException If initialiation fails.
     */
    public IgniteInternalFuture<Boolean> createRemoteSnapshot(UUID rmtNodeId, Map<Integer, Set<Integer>> parts) throws IgniteCheckedException {
        String snpName = "snapshot_" + UUID.randomUUID().getMostSignificantBits();

        ClusterNode rmtNode = cctx.discovery().node(rmtNodeId);

        assert nodeSupports(rmtNode, PERSISTENCE_CACHE_SNAPSHOT) : "Snapshot on remote node is not supported: " + rmtNode.id();

        if (rmtNode == null)
            throw new IgniteCheckedException("Requested snpashot node doesn't exists [rmtNodeId=" + rmtNodeId + ']');

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

                throw new IgniteCheckedException("Only owning partitions allowed to be requested from the remote node " +
                    "[rmtNodeId=" + rmtNodeId + ", grpId=" + grpId + ", missed=" + substract + ']');
            }
        }

        SnapshotTransmissionFuture snpTransFut = new SnapshotTransmissionFuture(rmtNodeId, snpName,
            parts.values().stream().mapToInt(Set::size).sum());

        busyLock.enterBusy();

        try {
            RequestSnapshotMessage msg0 =
                new RequestSnapshotMessage(snpName,
                    parts.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> GridIntList.valueOf(e.getValue()))));

            SnapshotTransmissionFuture fut = snpRq.get();

            if (fut != null && !fut.isCancelled())
                throw new IgniteCheckedException("Previous snapshot request has not been finished yet: " + fut);

            try {
                while (true) {
                    if (snpRq.compareAndSet(null, snpTransFut)) {
                        cctx.gridIO().sendToCustomTopic(rmtNodeId, DFLT_INITIAL_SNAPSHOT_TOPIC, msg0, SYSTEM_POOL);

                        break;
                    }

                    U.sleep(200);
                }
            }
            catch (IgniteCheckedException e) {
                snpRq.set(null);

                throw e;
            }
        }
        finally {
            busyLock.leaveBusy();
        }

        if (log.isInfoEnabled())
            log.info("Snapshot request message is sent to remote node [rmtNodeId=" + rmtNodeId + "]");

        return snpTransFut;
    }

    /**
     * @param snpName Unique snapshot name.
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @return Future which will be completed when snapshot is done.
     * @throws IgniteCheckedException If initialiation fails.
     */
    IgniteInternalFuture<Boolean> scheduleSnapshot(
        String snpName,
        Map<Integer, GridIntList> parts,
        Executor exec,
        SnapshotSender snpSndr
    ) throws IgniteCheckedException {
        if (localSnpCtxs.containsKey(snpName))
            throw new IgniteCheckedException("Snapshot with requested name is already scheduled: " + snpName);

        isCacheSnapshotSupported(parts.keySet(),
            (grpId) -> !CU.isPersistentCache(cctx.cache().cacheGroup(grpId).config(),
            cctx.kernalContext().config().getDataStorageConfiguration()),
            "in-memory cache groups are not allowed");
        isCacheSnapshotSupported(parts.keySet(),
            (grpId) -> cctx.cache().cacheGroup(grpId).config().isEncryptionEnabled(),
            "encryption cache groups are not allowed");

        LocalSnapshotContext sctx = null;

        if (!busyLock.enterBusy())
            return new GridFinishedFuture<>(new IgniteCheckedException("Snapshot manager is stopping"));

        File nodeSnpDir = null;

        try {
            String dbNodePath = cctx.kernalContext().pdsFolderResolver().resolveFolders().pdsNodePath();
            nodeSnpDir = U.resolveWorkDirectory(new File(tmpWorkDir, snpName).getAbsolutePath(), dbNodePath, false);

            sctx = new LocalSnapshotContext(snpName,
                nodeSnpDir,
                parts,
                exec,
                snpSndr);

            final LocalSnapshotContext sctx0 = sctx;

            // todo future should be included to context, or context to the future?
            sctx.snpFut.listen(f -> {
                localSnpCtxs.remove(snpName);

                closeSnapshotResources(sctx0);
            });

            for (Map.Entry<Integer, GridIntList> e : parts.entrySet()) {
                final CacheGroupContext gctx = cctx.cache().cacheGroup(e.getKey());

                // Create cache snapshot directory if not.
                File grpDir = U.resolveWorkDirectory(sctx.nodeSnpDir.getAbsolutePath(),
                    cacheDirName(gctx.config()), false);

                U.ensureDirectory(grpDir,
                    "bakcup directory for cache group: " + gctx.groupId(),
                    null);

                CompletableFuture<Boolean> cpEndFut0 = sctx.cpEndFut;

                GridIntIterator iter = e.getValue().iterator();

                while (iter.hasNext()) {
                    int partId = iter.next();

                    GroupPartitionId pair = new GroupPartitionId(e.getKey(), partId);
                    PageStore store = ((FilePageStoreManager)cctx.pageStore()).getStore(pair.getGroupId(),
                        pair.getPartitionId());

                    sctx.partDeltaWriters.put(pair,
                        new PageStoreSerialWriter(log,
                            store,
                            () -> cpEndFut0.isDone() && !cpEndFut0.isCompletedExceptionally(),
                            sctx.snpFut,
                            getPartionDeltaFile(grpDir, partId),
                            ioFactory,
                            pageSize));
                }
            }

            LocalSnapshotContext ctx0 = localSnpCtxs.putIfAbsent(snpName, sctx);

            assert ctx0 == null : ctx0;

            CheckpointFuture cpFut = dbMgr.forceCheckpoint(String.format(SNAPSHOT_CP_REASON, snpName));

            cpFut.finishFuture()
                .listen(f -> {
                    if (f.error() == null)
                        sctx0.cpEndFut.complete(true);
                    else
                        sctx0.cpEndFut.completeExceptionally(f.error());
                });

            cpFut.beginFuture()
                .get();

            U.log(log, "Snapshot operation scheduled with the following context: " + sctx);
        }
        catch (IOException e) {
            closeSnapshotResources(sctx);

            if (nodeSnpDir != null)
                nodeSnpDir.delete();

            throw new IgniteCheckedException(e);
        }
        finally {
            busyLock.leaveBusy();
        }

        return sctx.snpFut;
    }

    /**
     *
     * @param rootSnpDir Absolute snapshot directory.
     * @return Snapshot receiver instance.
     */
    SnapshotSender localSnapshotSender(File rootSnpDir) throws IgniteCheckedException {
        // Relative path to snapshot storage of local node.
        // Example: snapshotWorkDir/db/IgniteNodeName0
        String dbNodePath = cctx.kernalContext()
            .pdsFolderResolver()
            .resolveFolders()
            .pdsNodePath();

        U.ensureDirectory(new File(rootSnpDir, dbNodePath), "local snapshot directory", log);

        return new LocalSnapshotSender(log,
            new File(rootSnpDir, dbNodePath),
            ioFactory,
            storeFactory,
            cctx.kernalContext()
                .cacheObjects()
                .binaryWriter(rootSnpDir.getAbsolutePath()),
            cctx.kernalContext()
                .marshallerContext()
                .marshallerMappingWriter(cctx.kernalContext(), rootSnpDir.getAbsolutePath()),
            pageSize);
    }

    /**
     * @param snpName Snapshot name.
     * @param rmtNodeId Remote node id to send snapshot to.
     * @return Snapshot sender instance.
     */
    SnapshotSender remoteSnapshotSender(
        String snpName,
        UUID rmtNodeId
    ) throws IgniteCheckedException {
        // Relative path to snapshot storage of local node.
        // Example: snapshotWorkDir/db/IgniteNodeName0
        String dbNodePath = cctx.kernalContext()
            .pdsFolderResolver()
            .resolveFolders()
            .pdsNodePath();

        return new RemoteSnapshotSender(log,
            cctx.gridIO().openTransmissionSender(rmtNodeId, DFLT_INITIAL_SNAPSHOT_TOPIC),
            snpName,
            dbNodePath);
    }

    /**
     * @return The executor service used to run snapshot tasks.
     */
    ExecutorService snapshotExecutorService() {
        assert snpRunner != null;

        return snpRunner;
    }

    /**
     * @param sctx Context to clouse all resources.
     */
    private void closeSnapshotResources(LocalSnapshotContext sctx) {
        if (sctx == null)
            return;

        for (PageStoreSerialWriter writer : sctx.partDeltaWriters.values())
            U.closeQuiet(writer);

        U.closeQuiet(sctx.snpSndr);
        U.delete(sctx.nodeSnpDir);

        // Delete snapshot directory if no other files exists.
        try {
            if (U.fileCount(snapshotWorkDir(sctx.snpName).toPath()) == 0)
                U.delete(snapshotWorkDir(sctx.snpName).toPath());

        }
        catch (IOException e) {
            log.error("Snapshot directory doesn't exist [snpName=" + sctx.snpName + ", dir=" + snapshotWorkDir() + ']');
        }
    }

    /**
     * @param snpName Unique snapshot name.
     */
    public void stopCacheSnapshot(String snpName) {

    }

    /**
     * @param ioFactory Factory to create IO interface over a page stores.
     */
    void ioFactory(FileIOFactory ioFactory) {
        this.ioFactory = ioFactory;
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

        File target = new File (rmtSnpDir, rslvr.folderName());

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
     * @param grps Set of cache groups to check.
     * @param grpPred Checking predicate.
     * @param errCause Cause of error message if fails.
     */
    private static void isCacheSnapshotSupported(Set<Integer> grps, Predicate<Integer> grpPred, String errCause) {
        Set<Integer> notAllowdGrps = grps.stream()
            .filter(grpPred)
            .collect(Collectors.toSet());

        if (!notAllowdGrps.isEmpty()) {
            throw new IgniteException("Snapshot is not supported for these groups [cause=" + errCause +
                ", grps=" + notAllowdGrps + ']');
        }
    }

    /**
     * @param exec Runnable task to execute.
     * @param fut Future to notify.
     * @return Wrapped task.
     */
    private static Runnable wrapExceptionally(Runnable exec, GridFutureAdapter<?> fut) {
        return () -> {
            try {
                if (fut.isDone())
                    return;

                exec.run();
            }
            catch (Throwable t) {
                fut.onDone(t);
            }
        };
    }

    /**
     *
     */
    private static class PageStoreSerialWriter implements PageWriteListener, Closeable {
        /** Ignite logger to use. */
        @GridToStringExclude
        private final IgniteLogger log;

        /** Page store to which current writer is related to. */
        private final PageStore store;

        /** Busy lock to protect write opertions. */
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Local buffer to perpform copy-on-write operations. */
        private final ThreadLocal<ByteBuffer> localBuff;

        /** {@code true} if need the original page from PageStore instead of given buffer. */
        private final BooleanSupplier checkpointComplete;

        /** If snapshot has been stopped due to an error. */
        private final GridFutureAdapter<?> snpFut;

        /** IO over the underlying file */
        private volatile FileIO fileIo;

        /** {@code true} if partition file has been copied to external resource. */
        private volatile boolean partProcessed;

        /** {@code true} means current writer is allowed to handle page writes. */
        private volatile boolean inited;
        /**
         * Array of bits. 1 - means pages written, 0 - the otherwise.
         * Size of array can be estimated only under checkpoint write lock.
         */
        private volatile AtomicIntegerArray pagesWrittenBits;

        /**
         * @param log Ignite logger to use.
         * @param checkpointComplete Checkpoint finish flag.
         * @param pageSize Size of page to use for local buffer.
         * @param cfgFile Configuration file provider.
         * @param factory Factory to produce an IO interface over underlying file.
         */
        public PageStoreSerialWriter(
            IgniteLogger log,
            PageStore store,
            BooleanSupplier checkpointComplete,
            GridFutureAdapter<?> snpFut,
            File cfgFile,
            FileIOFactory factory,
            int pageSize
        ) throws IOException {
            assert store != null;

            this.checkpointComplete = checkpointComplete;
            this.snpFut = snpFut;
            this.log = log.getLogger(PageStoreSerialWriter.class);

            localBuff = ThreadLocal.withInitial(() ->
                ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));

            fileIo = factory.create(cfgFile);

            this.store = store;

            store.addWriteListener(this);
        }

        /**
         * @param allocPages Total number of tracking pages.
         */
        public void init(int allocPages) {
            lock.writeLock().lock();

            try {
                pagesWrittenBits = new AtomicIntegerArray(allocPages);
                inited = true;
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * @return {@code true} if writer is stopped and cannot write pages.
         */
        public boolean stopped() {
            return (checkpointComplete.getAsBoolean() && partProcessed) || snpFut.isDone();
        }

        /**
         * Mark partition has been processed by another thread.
         */
        public void markPartitionProcessed() {
            lock.writeLock().lock();

            try {
                partProcessed = true;
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void accept(long pageId, ByteBuffer buf) {
            assert buf.position() == 0 : buf.position();
            assert buf.order() == ByteOrder.nativeOrder() : buf.order();

            Throwable t = null;

            lock.readLock().lock();

            try {
                if (!inited)
                    return;

                if (stopped())
                    return;

                if (checkpointComplete.getAsBoolean()) {
                    int pageIdx = PageIdUtils.pageIndex(pageId);

                    // Page out of snapshot scope.
                    if (pageIdx > pagesWrittenBits.length())
                        return;

                    // Page already written.
                    if (!pagesWrittenBits.compareAndSet(pageIdx, 0, 1))
                        return;

                    final ByteBuffer locBuf = localBuff.get();

                    assert locBuf.capacity() == store.getPageSize();

                    locBuf.clear();

                    if (!store.read(pageId, locBuf, true))
                        return;

                    locBuf.flip();

                    writePage0(pageId, locBuf);
                }
                else {
                    // Direct buffre is needs to be written, associated checkpoint not finished yet.
                    writePage0(pageId, buf);
                }
            }
            catch (Throwable ex) {
                t = ex;
            }
            finally {
                lock.readLock().unlock();
            }

            if (t != null)
                snpFut.onDone(t);
        }

        /**
         * @param pageId Page ID.
         * @param pageBuf Page buffer to write.
         * @throws IOException If page writing failed (IO error occurred).
         */
        private void writePage0(long pageId, ByteBuffer pageBuf) throws IOException {
            assert fileIo != null : "Delta pages storage is not inited: " + this;
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                + " should be same with " + ByteOrder.nativeOrder();

            int crc = PageIO.getCrc(pageBuf);
            int crc32 = FastCrc.calcCrc(new CRC32(), pageBuf, pageBuf.limit());

            if (log.isDebugEnabled()) {
                log.debug("onPageWrite [pageId=" + pageId +
                    ", pageIdBuff=" + PageIO.getPageId(pageBuf) +
                    ", fileSize=" + fileIo.size() +
                    ", crcBuff=" + crc32 +
                    ", crcPage=" + crc + ']');
            }

            pageBuf.rewind();

            // Write buffer to the end of the file.
            fileIo.writeFully(pageBuf);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            lock.writeLock().lock();

            try {
                U.closeQuiet(fileIo);

                fileIo = null;

                store.removeWriteListener(this);

                inited = false;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     *
     */
    private class SnapshotTransmissionFuture extends GridFutureAdapter<Boolean> {
        /** Remote node id to request snapshot from. */
        private final UUID rmtNodeId;

        /** Snapshot name to create on remote. */
        private final String snpName;

        /** Collection of partition to be received. */
        private final Map<GroupPartitionId, FilePageStore> stores = new ConcurrentHashMap<>();

        /** Counter which show how many partitions left to be received. */
        private final AtomicInteger partsLeft;

        /**
         * @param cnt Partitions to receive.
         */
        public SnapshotTransmissionFuture(UUID rmtNodeId, String snpName, int cnt) {
            this.rmtNodeId = rmtNodeId;
            this.snpName = snpName;
            partsLeft = new AtomicInteger(cnt);
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

            return super.onDone(res, err, cancel);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SnapshotTransmissionFuture.class, this);
        }
    }

    /**
     *
     */
    private static class LocalSnapshotContext {
        /** Unique identifier of snapshot process. */
        private final String snpName;

        /** Absolute snapshot storage path. */
        private final File nodeSnpDir;

        /** Service to perform partitions copy. */
        private final Executor exec;

        /**
         * The length of file size per each cache partiton file.
         * Partition has value greater than zero only for partitons in OWNING state.
         * Information collected under checkpoint write lock.
         */
        private final Map<GroupPartitionId, Long> partFileLengths = new HashMap<>();

        /**
         * Map of partitions to snapshot and theirs corresponding delta PageStores.
         * Writers are pinned to the snapshot context due to controlling partition
         * processing supplier.
         */
        private final Map<GroupPartitionId, PageStoreSerialWriter> partDeltaWriters = new HashMap<>();

        /** Future of result completion. */
        @GridToStringExclude
        private final GridFutureAdapter<Boolean> snpFut = new GridFutureAdapter<>();

        /** Snapshot data sender. */
        @GridToStringExclude
        private final SnapshotSender snpSndr;

        /** Collection of partition to be snapshotted. */
        private final List<GroupPartitionId> parts = new ArrayList<>();

        /** Checkpoint end future. */
        private final CompletableFuture<Boolean> cpEndFut = new CompletableFuture<>();

        /** Flag idicates that this snapshot is start copying partitions. */
        private volatile boolean started;

        /**
         * @param snpName Unique identifier of snapshot process.
         * @param nodeSnpDir snapshot storage directory.
         * @param exec Service to perform partitions copy.
         */
        public LocalSnapshotContext(
            String snpName,
            File nodeSnpDir,
            Map<Integer, GridIntList> parts,
            Executor exec,
            SnapshotSender snpSndr
        ) {
            A.notNull(snpName, "snapshot name cannot be empty or null");
            A.notNull(nodeSnpDir, "You must secify correct snapshot directory");
            A.ensure(nodeSnpDir.isDirectory(), "Specified path is not a directory");
            A.notNull(exec, "Executor service must be not null");
            A.notNull(snpSndr, "Snapshot sender which handles execution tasks must be not null");

            this.snpName = snpName;
            this.nodeSnpDir = nodeSnpDir;
            this.exec = exec;
            this.snpSndr = snpSndr;

            for (Map.Entry<Integer, GridIntList> e : parts.entrySet()) {
                GridIntIterator iter = e.getValue().iterator();

                while(iter.hasNext())
                    this.parts.add(new GroupPartitionId(e.getKey(), iter.next()));
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            LocalSnapshotContext ctx = (LocalSnapshotContext)o;

            return snpName.equals(ctx.snpName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(snpName);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LocalSnapshotContext.class, this);
        }
    }

    /**
     *
     */
    private static class SerialExecutor implements Executor {
        /** */
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        /** */
        private final Executor executor;

        /** */
        private volatile Runnable active;

        /**
         * @param executor Executor to run tasks on.
         */
        public SerialExecutor(Executor executor) {
            this.executor = executor;
        }

        /** {@inheritDoc} */
        @Override public synchronized void execute(final Runnable r) {
            tasks.offer(new Runnable() {
                /** {@inheritDoc} */
                @Override public void run() {
                    try {
                        r.run();
                    }
                    finally {
                        scheduleNext();
                    }
                }
            });

            if (active == null) {
                scheduleNext();
            }
        }

        /**
         *
         */
        protected synchronized void scheduleNext() {
            if ((active = tasks.poll()) != null) {
                executor.execute(active);
            }
        }
    }

    /**
     *
     */
    private static class RemoteSnapshotSender implements SnapshotSender {
        /** Ignite logger to use. */
        private final IgniteLogger log;

        /** The sender which sends files to remote node. */
        private final GridIoManager.TransmissionSender sndr;

        /** Snapshot name */
        private final String snpName;

        /** Local node persistent directory with consistent id. */
        private final String dbNodePath;

        /**
         * @param log Ignite logger.
         * @param sndr File sender instance.
         * @param snpName Snapshot name.
         */
        public RemoteSnapshotSender(
            IgniteLogger log,
            GridIoManager.TransmissionSender sndr,
            String snpName,
            String dbNodePath
        ) {
            this.log = log.getLogger(RemoteSnapshotSender.class);
            this.sndr = sndr;
            this.snpName = snpName;
            this.dbNodePath = dbNodePath;
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig(File ccfg, String cacheDirName, GroupPartitionId pair) {
            // There is no need send it to a remote node.
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta(List<Map<Integer, MappedName>> mappings) {
            // There is no need send it to a remote node.
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta(Map<Integer, BinaryType> types) {
            // There is no need send it to a remote node.
        }

        /** {@inheritDoc} */
        @Override public void sendPart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            try {
                assert part.exists();

                sndr.send(part, 0, length, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.FILE);

                if (log.isInfoEnabled()) {
                    log.info("Partition file has been send [part=" + part.getName() + ", pair=" + pair +
                        ", length=" + length + ']');
                }
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                U.error(log, "Error sending partition file [part=" + part.getName() + ", pair=" + pair +
                    ", length=" + length + ']', e);

                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendDelta(File delta, String cacheDirName, GroupPartitionId pair) {
            try {
                sndr.send(delta, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.CHUNK);

                if (log.isInfoEnabled())
                    log.info("Delta pages storage has been send [part=" + delta.getName() + ", pair=" + pair + ']');
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
        private Map<String, Serializable> transmissionParams(String snpName, String cacheDirName, GroupPartitionId pair) {
            Map<String, Serializable> params = new HashMap<>();

            params.put(SNP_GRP_ID_PARAM, pair.getGroupId());
            params.put(SNP_PART_ID_PARAM, pair.getPartitionId());
            params.put(SNP_DB_NODE_PATH_PARAM, dbNodePath);
            params.put(SNP_CACHE_DIR_NAME_PARAM, cacheDirName);
            params.put(SNP_NAME_PARAM, snpName);

            return params;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            U.closeQuiet(sndr);
        }
    }

    /**
     *
     */
    private static class LocalSnapshotSender implements SnapshotSender {
        /** Ignite logger to use. */
        private final IgniteLogger log;

        /**
         * Local node snapshot directory calculated on snapshot directory.
         */
        private final File dbNodeSnpDir;

        /** Facotry to produce IO interface over a file. */
        private final FileIOFactory ioFactory;

        /** Factory to create page store for restore. */
        private final BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory;

        /** Store binary files. */
        private final BinaryTypeWriter binaryWriter;

        /** Marshaller mapping writer. */
        private final MarshallerMappingWriter mappingWriter;

        /** Size of page. */
        private final int pageSize;

        /**
         * @param log Ignite logger to use.
         * @param snpDir Local node snapshot directory.
         * @param ioFactory Facotry to produce IO interface over a file.
         * @param storeFactory Factory to create page store for restore.
         * @param pageSize Size of page.
         */
        public LocalSnapshotSender(
            IgniteLogger log,
            File snpDir,
            FileIOFactory ioFactory,
            BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory,
            BinaryTypeWriter binaryWriter,
            MarshallerMappingWriter mappingWriter,
            int pageSize
        ) {
            this.log = log.getLogger(LocalSnapshotSender.class);
            dbNodeSnpDir = snpDir;
            this.ioFactory = ioFactory;
            this.storeFactory = storeFactory;
            this.pageSize = pageSize;
            this.binaryWriter = binaryWriter;
            this.mappingWriter = mappingWriter;
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig(File ccfg, String cacheDirName, GroupPartitionId pair) {
            try {
                File cacheDir = U.resolveWorkDirectory(dbNodeSnpDir.getAbsolutePath(), cacheDirName, false);

                copy(ccfg, new File(cacheDir, ccfg.getName()), ccfg.length());
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta(List<Map<Integer, MappedName>> mappings) {
            if (mappings == null)
                return;

            for (int platformId = 0; platformId < mappings.size(); platformId++) {
                Map<Integer, MappedName> cached = mappings.get(platformId);

                try {
                    addPlatformMappings((byte)platformId,
                        cached,
                        (typeId, clsName) -> true,
                        (typeId, mapping) -> {
                        },
                        mappingWriter);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta(Map<Integer, BinaryType> types) {
            if (types == null)
                return;

            for (Map.Entry<Integer, BinaryType> e : types.entrySet())
                binaryWriter.writeMeta(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void sendPart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            try {
                File cacheDir = U.resolveWorkDirectory(dbNodeSnpDir.getAbsolutePath(), cacheDirName, false);

                File snpPart = new File(cacheDir, part.getName());

                if (!snpPart.exists() || snpPart.delete())
                    snpPart.createNewFile();

                if (length == 0)
                    return;

                copy(part, snpPart, length);

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
        @Override public void sendDelta(File delta, String cacheDirName, GroupPartitionId pair) {
            File snpPart = getPartitionFile(dbNodeSnpDir, cacheDirName, pair.getPartitionId());

            U.log(log, "Start partition snapshot recovery with the given delta page file [part=" + snpPart +
                ", delta=" + delta + ']');

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

                    int crc32 = FastCrc.calcCrc(new CRC32(), pageBuf, pageBuf.limit());

                    int crc = PageIO.getCrc(pageBuf);

                    U.log(log, "Read page given delta file [path=" + delta.getName() +
                        ", pageId=" + pageId + ", pos=" + pos + ", pages=" + (totalBytes / pageSize) +
                        ", crcBuff=" + crc32 + ", crcPage=" + crc + ']');

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
        @Override public void close() throws IOException {
            // No-op.
        }

        /**
         * @param from Copy from file.
         * @param to Copy data to file.
         * @param length Number of bytes to copy from beginning.
         * @throws IOException If fails.
         */
        private void copy(File from, File to, long length) throws IOException {
            try (FileIO src = ioFactory.create(from);
                 FileChannel dest = new FileOutputStream(to).getChannel()) {
                src.position(0);

                long written = 0;

                while (written < length)
                    written += src.transferTo(written, length - written, dest);
            }
        }
    }
}
