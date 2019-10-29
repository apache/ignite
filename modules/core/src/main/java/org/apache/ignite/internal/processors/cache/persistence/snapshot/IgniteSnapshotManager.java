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
import java.util.Iterator;
import java.util.LinkedList;
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
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.communication.TransmissionHandler;
import org.apache.ignite.internal.managers.communication.TransmissionMeta;
import org.apache.ignite.internal.managers.communication.TransmissionPolicy;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
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
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

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
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFileEx;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionNameEx;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getFlagByPartId;

/** */
public class IgniteSnapshotManager extends GridCacheSharedManagerAdapter implements PartitionsExchangeAware  {
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
    private static final int SNAPSHOT_THEEAD_POOL_SIZE = 4;

    /** Default snapshot topic to receive snapshots from remote node. */
    private static final Object DFLT_RMT_SNAPSHOT_TOPIC = GridTopic.TOPIC_RMT_SNAPSHOT.topic("0");

    /** Cache group id parameter name for a file transmission. */
    private static final String SNP_GRP_ID_PARAM = "grpId";

    /** Cache partition id parameter name for a file transmission. */
    private static final String SNP_PART_ID_PARAM = "partId";

    /** Cache local node directory path name (e.g. db/IgniteNode0). */
    private static final String SNP_DB_NODE_PATH_PARAM = "dbNodePath";

    /** Cache directory parameter name for a file transmission. */
    private static final String SNP_CACHE_DIR_NAME_PARAM = "cacheDirName";

    /** Snapshot parameter name for a file transmission. */
    private static final String SNP_NAME_PARAM = "snpName";

    /** Map of registered cache snapshot processes and their corresponding contexts. */
    private final ConcurrentMap<String, LocalSnapshotContext> localSnpCtxs = new ConcurrentHashMap<>();

    /** Map of requested snapshot from remote node. */
    private final ConcurrentMap<T2<UUID, String>, SnapshotTransmission> reqSnps = new ConcurrentHashMap<>();

    /** All registered page writers of all running snapshot processes. */
    private final ConcurrentMap<GroupPartitionId, List<PageStoreSerialWriter>> partWriters = new ConcurrentHashMap<>();

    /** Lock to protect the resources is used. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Main snapshot directory to store files. */
    private File localSnpDir;

    /** Working directory for loaded snapshots from remote nodes. */
    private File snpWorkDir;

    /** Factory to working with delta as file storage. */
    private volatile FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** Factory to create page store for restore. */
    private volatile BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory;

    /** snapshot thread pool. */
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
            SNAPSHOT_THEEAD_POOL_SIZE,
            SNAPSHOT_THEEAD_POOL_SIZE,
            30_000,
            new LinkedBlockingQueue<>(),
            SYSTEM_POOL,
            (t, e) -> kctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e)));

        assert cctx.pageStore() instanceof FilePageStoreManager;

        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        PdsFolderSettings rslvDir = kctx.pdsFolderResolver().resolveFolders();

        // todo must be available on storage configuration
        localSnpDir = U.resolveWorkDirectory(kctx.config().getWorkDirectory(), DFLT_LOCAL_SNAPSHOT_DIRECTORY, false);
        snpWorkDir = Paths.get(storeMgr.workDir().getAbsolutePath(), DFLT_SNAPSHOT_WORK_DIRECTORY).toFile();

        U.ensureDirectory(localSnpDir, "local snapshots directory", log);
        U.ensureDirectory(snpWorkDir, "work directory for snapshots creation", log);

        storeFactory = ((FilePageStoreManager)storeMgr)::getPageStoreFactory;
        dbMgr = (GridCacheDatabaseSharedManager)cctx.database();

        dbMgr.addCheckpointListener(cpLsnr = new DbCheckpointListener() {
            @Override public void beforeCheckpointBegin(Context ctx) {
                for (LocalSnapshotContext sctx0 : localSnpCtxs.values()) {
                    if (sctx0.started)
                        continue;

                    // Gather partitions metainfo for thouse which will be copied.
                    ctx.gatherPartStats(sctx0.parts);
                }
            }

            @Override public void onMarkCheckpointBegin(Context ctx) {
                // No-op.
            }

            @Override public void onMarkCheckpointEnd(Context ctx) {
                // Under the write lock here. It's safe to add new stores
                for (LocalSnapshotContext sctx0 : localSnpCtxs.values()) {
                    if (sctx0.started)
                        continue;

                    try {
                        PartitionAllocationMap allocationMap = ctx.partitionStatMap();

                        allocationMap.prepareForSnapshot();

                        for (GroupPartitionId pair : sctx0.parts) {
                            PagesAllocationRange allocRange = allocationMap.get(pair);

                            // Partition can be reserved
                            // Partition can be MOVING\RENTING states
                            // Index partition will be excluded if not all partition OWNING
                            // There is no data assigned to partition, thus it haven't been created yet
                            if (allocRange == null) {
                                log.warning("Allocated info about requested partition is missing during snapshot " +
                                    "operation [pair=" + pair + ", snmName=" + sctx0.snpName + ']');
                            }

                            PageStore store = storeMgr.getStore(pair.getGroupId(), pair.getPartitionId());

                            sctx0.partFileLengths.put(pair, allocRange == null ? 0L : store.size());
                            sctx0.partDeltaWriters.get(pair)
                                .init(allocRange == null ? 0 : allocRange.getCurrAllocatedPageCnt());
                        }

                        for (Map.Entry<GroupPartitionId, PageStoreSerialWriter> e : sctx0.partDeltaWriters.entrySet()) {
                            partWriters.computeIfAbsent(e.getKey(), p -> new LinkedList<>())
                                .add(e.getValue());
                        }
                    }
                    catch (IgniteCheckedException e) {
                        sctx0.snpFut.onDone(e);
                    }
                }

                // Remove not used delta stores.
                for (List<PageStoreSerialWriter> list0 : partWriters.values())
                    list0.removeIf(PageStoreSerialWriter::stopped);
            }

            @Override public void onCheckpointBegin(Context ctx) {
                for (LocalSnapshotContext sctx0 : localSnpCtxs.values()) {
                    if (sctx0.started || sctx0.snpFut.isDone())
                        continue;

                    // Submit all tasks for partitions and deltas processing.
                    List<CompletableFuture<Void>> futs = new ArrayList<>();
                    FilePageStoreManager storeMgr = (FilePageStoreManager) cctx.pageStore();

                    if (log.isInfoEnabled())
                        log.info("Submit partition processings tasks wiht partition allocated lengths: " + sctx0.partFileLengths);

                    // Process binary meta
                    futs.add(CompletableFuture.runAsync(() ->
                            sctx0.snpRcv.receiveBinaryMeta(cctx.kernalContext()
                                .cacheObjects()
                                .metadataTypes()),
                        sctx0.exec));

                    // Process marshaller meta
                    futs.add(CompletableFuture.runAsync(() ->
                            sctx0.snpRcv.receiveMarshallerMeta(cctx.kernalContext()
                                .marshallerContext()
                                .getCachedMappings()),
                        sctx0.exec));

                    // Process partitions
                    for (GroupPartitionId pair : sctx0.parts) {
                        CacheConfiguration ccfg = cctx.cache().cacheGroup(pair.getGroupId()).config();
                        String cacheDirName = cacheDirName(ccfg);
                        Long length = sctx0.partFileLengths.get(pair);

                        try {
                            // Initialize empty partition file.
                            if (length == 0) {
                                FilePageStore filePageStore = (FilePageStore) storeMgr.getStore(pair.getGroupId(),
                                    pair.getPartitionId());

                                filePageStore.init();
                            }
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }

                        CompletableFuture<Void> fut0 = CompletableFuture.runAsync(() -> {
                                sctx0.snpRcv.receivePart(
                                    getPartitionFileEx(storeMgr.workDir(), cacheDirName, pair.getPartitionId()),
                                    cacheDirName,
                                    pair,
                                    length);

                                // Stop partition writer.
                                sctx0.partDeltaWriters.get(pair).partProcessed = true;
                            },
                            sctx0.exec)
                            // Wait for the completion of both futures - checkpoint end, copy partition
                            .runAfterBothAsync(sctx0.cpEndFut,
                                () -> {
                                    File delta = getPartionDeltaFile(cacheWorkDir(sctx0.nodeSnpDir, cacheDirName),
                                        pair.getPartitionId());

                                    sctx0.snpRcv.receiveDelta(delta, cacheDirName, pair);

                                    boolean deleted = delta.delete();

                                    assert deleted;
                                },
                                sctx0.exec)
                            .thenRunAsync(() -> sctx0.snpRcv.receiveCacheConfig(storeMgr.cacheConfiguration(ccfg), cacheDirName, pair));

                        futs.add(fut0);
                    }

                    int futsSize = futs.size();

                    CompletableFuture.allOf(futs.toArray(new CompletableFuture[futsSize]))
                        .whenComplete(new BiConsumer<Void, Throwable>() {
                            @Override public void accept(Void res, Throwable t) {
                                if (t == null)
                                    sctx0.snpFut.onDone(sctx0.snpName);
                                else
                                    sctx0.snpFut.onDone(t);
                            }
                        });

                    sctx0.started = true;
                }
            }
        });

        // Receive remote snapshots requests.
        cctx.gridIO().addMessageListener(DFLT_RMT_SNAPSHOT_TOPIC, new GridMessageListener() {
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
                            remoteSnapshotReceiver(snpName,
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
        cctx.kernalContext().io().addTransmissionHandler(DFLT_RMT_SNAPSHOT_TOPIC, new TransmissionHandler() {
            /** {@inheritDoc} */
            @Override public void onException(UUID nodeId, Throwable err) {
                Iterator<Map.Entry<T2<UUID, String>, SnapshotTransmission>> iter0 = reqSnps.entrySet().iterator();

                while (iter0.hasNext()) {
                    Map.Entry<T2<UUID, String>, SnapshotTransmission> e = iter0.next();

                    if (e.getKey().get1().equals(nodeId)) {
                        iter0.remove();

                        U.closeQuiet(e.getValue());

                        if (snpLsnr != null)
                            snpLsnr.onException(nodeId, e.getKey().get2(), err);
                    }
                }
            }

            /** {@inheritDoc} */
            @Override public String filePath(UUID nodeId, TransmissionMeta fileMeta) {
                Integer partId = (Integer)fileMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)fileMeta.params().get(SNP_NAME_PARAM);
                String rmtDbNodePath = (String)fileMeta.params().get(SNP_DB_NODE_PATH_PARAM);
                String cacheDirName = (String)fileMeta.params().get(SNP_CACHE_DIR_NAME_PARAM);

                if (reqSnps.get(new T2<>(nodeId, snpName)) == null) {
                    throw new IgniteException("Snapshot transmission with given name doesn't exists " +
                        "[snpName=" + snpName + ", cacheDirName=" + cacheDirName + ", partId=" + partId + ']');
                }

                try {
                    File cacheDir = U.resolveWorkDirectory(snpWorkDir.getAbsolutePath(),
                        cacheSnapshotPath(snpName, rmtDbNodePath, cacheDirName),
                        false);

                    return new File(cacheDir, getPartitionNameEx(partId)).getAbsolutePath();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            /**
             * @param pageStore Page store to finish recovery.
             * @param snpName Snapshot name to notify listener with.
             * @param part Partition file.
             * @param grpPartId Pair of group id and its partition id.
             */
            private void finishRecover(
                FilePageStore pageStore,
                UUID rmtNodeId,
                String snpName,
                File part,
                GroupPartitionId grpPartId
            ) {
                try {
                    pageStore.finishRecover();

                    U.closeQuiet(pageStore);

                    cctx.kernalContext().closure().runLocalSafe(() -> {
                        if (snpLsnr == null)
                            return;

                        snpLsnr.onPartition(rmtNodeId,
                            snpName,
                            part,
                            grpPartId.getGroupId(),
                            grpPartId.getPartitionId());
                    });
                }
                catch (StorageException e) {
                    throw new IgniteException(e);
                }
            }

            /** {@inheritDoc} */
            @Override public Consumer<ByteBuffer> chunkHandler(UUID nodeId, TransmissionMeta initMeta) {
                Integer grpId = (Integer)initMeta.params().get(SNP_GRP_ID_PARAM);
                Integer partId = (Integer)initMeta.params().get(SNP_PART_ID_PARAM);
                String snpName = (String)initMeta.params().get(SNP_NAME_PARAM);

                GroupPartitionId grpPartId = new GroupPartitionId(grpId, partId);
                SnapshotTransmission snpTrans = reqSnps.get(new T2<>(nodeId, snpName));

                if (snpTrans == null) {
                    throw new IgniteException("Snapshot transmission with given name doesn't exists " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                FilePageStore pageStore = snpTrans.stores.get(grpPartId);

                if (pageStore == null) {
                    throw new IgniteException("Partition must be loaded before applying snapshot delta pages " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                // todo this should be inverted\hided to snapshot transmission
                pageStore.beginRecover();

                // No snapshot delta pages received. Finalize recovery.
                if (initMeta.count() == 0) {
                    finishRecover(pageStore,
                        nodeId,
                        snpName,
                        new File(snpTrans.stores.remove(grpPartId).getFileAbsolutePath()),
                        grpPartId);
                }

                return new Consumer<ByteBuffer>() {
                    final LongAdder transferred = new LongAdder();

                    @Override public void accept(ByteBuffer buff) {
                        try {
                            assert initMeta.count() != 0 : initMeta;

                            if (snpTrans.stopped)
                                return;

                            pageStore.write(PageIO.getPageId(buff), buff, 0, false);

                            transferred.add(buff.capacity());

                            if (transferred.longValue() == initMeta.count()) {
                                finishRecover(pageStore,
                                    nodeId,
                                    snpName,
                                    new File(snpTrans.stores.remove(grpPartId).getFileAbsolutePath()),
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

                SnapshotTransmission snpTrans = reqSnps.get(new T2<>(nodeId, snpName));

                if (snpTrans == null) {
                    throw new IgniteException("Snapshot transmission with given name doesn't exists " +
                        "[snpName=" + snpName + ", grpId=" + grpId + ", partId=" + partId + ']');
                }

                return new Consumer<File>() {
                    @Override public void accept(File file) {
                        if (snpTrans.stopped)
                            return;

                        busyLock.enterBusy();

                        try {
                            FilePageStore pageStore = (FilePageStore)storeFactory
                                .apply(grpId, false)
                                .createPageStore(getFlagByPartId(partId),
                                    file::toPath,
                                    new LongAdderMetric("NO_OP", null));

                            pageStore.init();

                            snpTrans.stores.put(new GroupPartitionId(grpId, partId), pageStore);
                            //loadedPageStores.put(new T4<>(nodeId, snpName, grpId, partId), pageStore);
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

            for (SnapshotTransmission trs : reqSnps.values())
                U.closeQuiet(trs);

            partWriters.clear();
            snpRunner.shutdown();

            cctx.kernalContext().io().removeMessageListener(DFLT_RMT_SNAPSHOT_TOPIC);
            cctx.kernalContext().io().removeTransmissionHandler(DFLT_RMT_SNAPSHOT_TOPIC);
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
        assert localSnpDir != null;

        return localSnpDir;
    }

    /**
     * @return Node snapshot working directory.
     */
    public File snapshotWorkDir() {
        assert snpWorkDir != null;

        return snpWorkDir;
    }

    /**
     * @return Node snapshot working directory with given snapshot name.
     */
    public File snapshotWorkDir(String snpName) {
        return new File(snapshotWorkDir(), snpName);
    }

    /** {@inheritDoc} */
    @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
        Iterator<Map.Entry<T2<UUID, String>, SnapshotTransmission>> rqIter = reqSnps.entrySet().iterator();

        while (rqIter.hasNext()) {
            Map.Entry<T2<UUID, String>, SnapshotTransmission> e = rqIter.next();

            rqIter.remove();

            e.getValue().stopped = true;

            U.closeQuiet(e.getValue());

            if (snpLsnr != null) {
                snpLsnr.onException(fut.firstEvent().eventNode().id(),
                    e.getKey().get2(),
                    new ClusterTopologyCheckedException("Requesting snapshot from remote node has been stopped due to topology changed " +
                        "[snpName" + e.getKey().get1() + ", rmtNodeId=" + e.getKey().get2() + ']'));
            }
        }

        Iterator<LocalSnapshotContext> snpIter = localSnpCtxs.values().iterator();

        while (snpIter.hasNext()) {
            LocalSnapshotContext sctx = snpIter.next();

            snpIter.remove();

            sctx.snpFut.onDone(new ClusterTopologyCheckedException("Snapshot interrupted due to topology changed"));

            closeSnapshotResources(sctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
        // No-op.
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
                    int partsCnt = cctx.cache()
                        .cacheGroup(grpId)
                        .affinity()
                        .partitions();

                    Set<Integer> grpParts = Stream.iterate(0, n -> n + 1)
                        .limit(partsCnt)
                        .collect(Collectors.toSet());

                    grpParts.add(INDEX_PARTITION);

                    return GridIntList.valueOf(grpParts);
                }));

        File rootSnpDir0 = localSnapshotDir(snpName);

        try {
            return scheduleSnapshot(snpName,
                parts,
                snpRunner,
                localSnapshotReceiver(rootSnpDir0));
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
    public String createRemoteSnapshot(UUID rmtNodeId, Map<Integer, Set<Integer>> parts) throws IgniteCheckedException {
        String snpName = "snapshot_" + UUID.randomUUID().getMostSignificantBits();

        ClusterNode rmtNode = cctx.discovery().node(rmtNodeId);

        assert nodeSupports(rmtNode, PERSISTENCE_CACHE_SNAPSHOT) : "Snapshot on remote node is not supported: " + rmtNode.id();

        if (rmtNode == null)
            throw new IgniteCheckedException("Requested snpashot node doesn't exists [rmtNodeId=" + rmtNodeId + ']');

        busyLock.enterBusy();

        try {
            RequestSnapshotMessage msg0 =
                new RequestSnapshotMessage(snpName,
                    parts.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> GridIntList.valueOf(e.getValue()))));

            SnapshotTransmission prev = reqSnps.putIfAbsent(new T2<>(rmtNodeId, snpName), new SnapshotTransmission(log, parts));

            assert prev == null : prev;

            cctx.gridIO().sendToCustomTopic(rmtNodeId, DFLT_RMT_SNAPSHOT_TOPIC, msg0, SYSTEM_POOL);
        }
        finally {
            busyLock.leaveBusy();
        }

        if (log.isInfoEnabled())
            log.info("Snapshot request message is sent to remote node [rmtNodeId=" + rmtNodeId + "]");

        return snpName;
    }

    /**
     * @param snpName Unique snapshot name.
     * @param parts Collection of pairs group and appropratate cache partition to be snapshotted.
     * @param snpRcv Factory which produces snapshot receiver instance.
     * @return Future which will be completed when snapshot is done.
     * @throws IgniteCheckedException If initialiation fails.
     */
    IgniteInternalFuture<String> scheduleSnapshot(
        String snpName,
        Map<Integer, GridIntList> parts,
        Executor exec,
        SnapshotReceiver snpRcv
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
            nodeSnpDir = U.resolveWorkDirectory(new File(snpWorkDir, snpName).getAbsolutePath(), dbNodePath, false);

            sctx = new LocalSnapshotContext(snpName,
                nodeSnpDir,
                parts,
                exec,
                snpRcv);

            final LocalSnapshotContext sctx0 = sctx;

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

                    final GroupPartitionId pair = new GroupPartitionId(e.getKey(), partId);

                    sctx.partDeltaWriters.put(pair,
                        new PageStoreSerialWriter(log,
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
    SnapshotReceiver localSnapshotReceiver(File rootSnpDir) throws IgniteCheckedException {
        // Relative path to snapshot storage of local node.
        // Example: snapshotWorkDir/db/IgniteNodeName0
        String dbNodePath = cctx.kernalContext()
            .pdsFolderResolver()
            .resolveFolders()
            .pdsNodePath();

        U.ensureDirectory(new File(rootSnpDir, dbNodePath), "local snapshot directory", log);

        return new LocalSnapshotReceiver(log,
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
     * @return Snapshot receiver instance.
     */
    SnapshotReceiver remoteSnapshotReceiver(
        String snpName,
        UUID rmtNodeId
    ) throws IgniteCheckedException {
        // Relative path to snapshot storage of local node.
        // Example: snapshotWorkDir/db/IgniteNodeName0
        String dbNodePath = cctx.kernalContext()
            .pdsFolderResolver()
            .resolveFolders()
            .pdsNodePath();

        return new RemoteSnapshotReceiver(log,
            cctx.gridIO().openTransmissionSender(rmtNodeId, DFLT_RMT_SNAPSHOT_TOPIC),
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

        U.closeQuiet(sctx.snpRcv);
        U.delete(sctx.nodeSnpDir);

        // Delete snapshot directory if no other files exists.
        try {
            if (U.fileCount(snapshotWorkDir(sctx.snpName).toPath()) == 0)
                U.delete(snapshotWorkDir(sctx.snpName).toPath());

        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param snpName Unique snapshot name.
     */
    public void stopCacheSnapshot(String snpName) {

    }

    /**
     * @param pairId Cache group, partition identifiers pair.
     * @param pageId Tracked page id.
     * @param buf Buffer with page data.
     */
    public void beforeStoreWrite(GroupPartitionId pairId, long pageId, ByteBuffer buf, PageStore store) {
        assert buf.position() == 0 : buf.position();
        assert buf.order() == ByteOrder.nativeOrder() : buf.order();

        if (!busyLock.enterBusy())
            return;

        try {
            List<PageStoreSerialWriter> writers = partWriters.get(pairId);

            if (writers == null || writers.isEmpty())
                return;

            for (PageStoreSerialWriter writer : writers)
                writer.write(pageId, buf, store);
        }
        finally {
            busyLock.leaveBusy();
        }
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
     *
     */
    private static class PageStoreSerialWriter implements Closeable {
        /** Ignite logger to use. */
        @GridToStringExclude
        private final IgniteLogger log;

        /** Buse lock to perform write opertions. */
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Local buffer to perpform copy-on-write operations. */
        private final ThreadLocal<ByteBuffer> localBuff;

        /** {@code true} if need the original page from PageStore instead of given buffer. */
        private final BooleanSupplier checkpointComplete;

        /** If snapshot has been stopped due to an error. */
        private final GridFutureAdapter<?> snpFut;

        /** IO over the underlying file */
        private volatile FileIO fileIo;

        /** {@code true} if current writer is stopped. */
        private volatile boolean partProcessed;

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
            BooleanSupplier checkpointComplete,
            GridFutureAdapter<?> snpFut,
            File cfgFile,
            FileIOFactory factory,
            int pageSize
        ) throws IOException {
            this.checkpointComplete = checkpointComplete;
            this.snpFut = snpFut;
            this.log = log.getLogger(PageStoreSerialWriter.class);

            localBuff = ThreadLocal.withInitial(() ->
                ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));

            fileIo = factory.create(cfgFile);
        }

        /**
         * @param allocPages Total number of tracking pages.
         * @return This for chaining.
         */
        public PageStoreSerialWriter init(int allocPages) {
            pagesWrittenBits = new AtomicIntegerArray(allocPages);

            return this;
        }

        /**
         * @return {@code true} if writer is stopped and cannot write pages.
         */
        public boolean stopped() {
            return (checkpointComplete.getAsBoolean() && partProcessed) || snpFut.isDone();
        }

        /**
         * @param pageId Page id to write.
         * @param buf Page buffer.
         * @param store Storage to write to.
         */
        public void write(long pageId, ByteBuffer buf, PageStore store) {
            assert pagesWrittenBits != null;

            if (stopped())
                return;

            try {
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

                    if (store.readPage(pageId, locBuf, true) < 0)
                        return;

                    locBuf.flip();

                    writePage0(pageId, locBuf);
                }
                else {
                    // Direct buffre is needs to be written, associated checkpoint not finished yet.
                    writePage0(pageId, buf);

                    buf.rewind();
                }
            }
            catch (Throwable t) {
                snpFut.onDone(t);
            }
        }

        /**
         * @param pageId Page ID.
         * @param pageBuf Page buffer to write.
         * @throws IOException If page writing failed (IO error occurred).
         */
        private void writePage0(long pageId, ByteBuffer pageBuf) throws IOException {
            lock.readLock().lock();

            try {
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
            finally {
                lock.readLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void close() {
            lock.writeLock().lock();

            try {
                U.closeQuiet(fileIo);

                fileIo = null;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     *
     */
    private static class SnapshotTransmission implements Closeable {
        /** Logger to use. */
        private final IgniteLogger log;

        /** Collection of partition to be received. */
        private final Map<GroupPartitionId, FilePageStore> stores = new HashMap<>();

        /** {@code True} if snapshot transmission must be interrupted. */
        private volatile boolean stopped;

        /**
         * @param parts Partitions to receive.
         */
        public SnapshotTransmission(IgniteLogger log, Map<Integer, Set<Integer>> parts) {
            this.log = log.getLogger(SnapshotTransmission.class);

            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                for (Integer part : e.getValue())
                    stores.put(new GroupPartitionId(e.getKey(), part), null);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            for (Map.Entry<GroupPartitionId, FilePageStore> entry : stores.entrySet()) {
                FilePageStore store = entry.getValue();

                if (store == null)
                    continue;

                try {
                    store.stop(true);
                }
                catch (StorageException e) {
                    log.warning("Error stopping received file page store", e);
                }
            }
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
        private final GridFutureAdapter<String> snpFut = new GridFutureAdapter<>();

        /** Snapshot data receiver. */
        @GridToStringExclude
        private final SnapshotReceiver snpRcv;

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
            SnapshotReceiver snpRcv
        ) {
            A.notNull(snpName, "snapshot name cannot be empty or null");
            A.notNull(nodeSnpDir, "You must secify correct snapshot directory");
            A.ensure(nodeSnpDir.isDirectory(), "Specified path is not a directory");
            A.notNull(exec, "Executor service must be not null");
            A.notNull(snpRcv, "Snapshot receiver which handles execution tasks must be not null");

            this.snpName = snpName;
            this.nodeSnpDir = nodeSnpDir;
            this.exec = exec;
            this.snpRcv = snpRcv;

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
    private static class RemoteSnapshotReceiver implements SnapshotReceiver {
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
        public RemoteSnapshotReceiver(
            IgniteLogger log,
            GridIoManager.TransmissionSender sndr,
            String snpName,
            String dbNodePath
        ) {
            this.log = log.getLogger(RemoteSnapshotReceiver.class);
            this.sndr = sndr;
            this.snpName = snpName;
            this.dbNodePath = dbNodePath;
        }

        /** {@inheritDoc} */
        @Override public void receiveCacheConfig(File ccfg, String cacheDirName, GroupPartitionId pair) {
            // There is no need send it to a remote node.
        }

        /** {@inheritDoc} */
        @Override public void receiveMarshallerMeta(List<Map<Integer, MappedName>> mappings) {
            // There is no need send it to a remote node.
        }

        /** {@inheritDoc} */
        @Override public void receiveBinaryMeta(Map<Integer, BinaryType> types) {
            // There is no need send it to a remote node.
        }

        /** {@inheritDoc} */
        @Override public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            try {
                assert part.exists();

                sndr.send(part, 0, length, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.FILE);

                if (log.isInfoEnabled()) {
                    log.info("Partition file has been send [part=" + part.getName() + ", pair=" + pair +
                        ", length=" + length + ']');
                }
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void receiveDelta(File delta, String cacheDirName, GroupPartitionId pair) {
            try {
                sndr.send(delta, transmissionParams(snpName, cacheDirName, pair), TransmissionPolicy.CHUNK);

                if (log.isInfoEnabled())
                    log.info("Delta pages storage has been send [part=" + delta.getName() + ", pair=" + pair + ']');
            }
            catch (IgniteCheckedException | InterruptedException | IOException e) {
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
        @Override public void close() throws IOException {
            U.closeQuiet(sndr);
        }
    }

    /**
     *
     */
    private static class LocalSnapshotReceiver implements SnapshotReceiver {
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
        public LocalSnapshotReceiver(
            IgniteLogger log,
            File snpDir,
            FileIOFactory ioFactory,
            BiFunction<Integer, Boolean, FilePageStoreFactory> storeFactory,
            BinaryTypeWriter binaryWriter,
            MarshallerMappingWriter mappingWriter,
            int pageSize
        ) {
            this.log = log.getLogger(LocalSnapshotReceiver.class);
            dbNodeSnpDir = snpDir;
            this.ioFactory = ioFactory;
            this.storeFactory = storeFactory;
            this.pageSize = pageSize;
            this.binaryWriter = binaryWriter;
            this.mappingWriter = mappingWriter;
        }

        /** {@inheritDoc} */
        @Override public void receiveCacheConfig(File ccfg, String cacheDirName, GroupPartitionId pair) {
            try {
                File cacheDir = U.resolveWorkDirectory(dbNodeSnpDir.getAbsolutePath(), cacheDirName, false);

                copy(ccfg, new File(cacheDir, ccfg.getName()), ccfg.length());
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void receiveMarshallerMeta(List<Map<Integer, MappedName>> mappings) {
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
        @Override public void receiveBinaryMeta(Map<Integer, BinaryType> types) {
            if (types == null)
                return;

            for (Map.Entry<Integer, BinaryType> e : types.entrySet())
                binaryWriter.writeMeta(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void receivePart(File part, String cacheDirName, GroupPartitionId pair, Long length) {
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
        @Override public void receiveDelta(File delta, String cacheDirName, GroupPartitionId pair) {
            File snpPart = getPartitionFileEx(dbNodeSnpDir, cacheDirName, pair.getPartitionId());

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
