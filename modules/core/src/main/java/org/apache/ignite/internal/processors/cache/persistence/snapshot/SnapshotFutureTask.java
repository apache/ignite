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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageWriteListener;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.copy;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.partDeltaFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.partDeltaIndexFile;

/**
 * The requested map of cache groups and its partitions to include into snapshot represented as <tt>Map<Integer, Set<Integer>></tt>.
 * If array of partitions is {@code null} than all OWNING partitions for given cache groups will be included into snapshot.
 * In this case if all partitions have OWNING state the index partition also will be included.
 * <p>
 * If partitions for particular cache group are not provided that they will be collected and added
 * on checkpoint under the write-lock.
 */
class SnapshotFutureTask extends AbstractSnapshotFutureTask<Set<GroupPartitionId>> implements CheckpointListener {
    /** File page store manager for accessing cache group associated files. */
    private final FilePageStoreManager pageStore;

    /** Local buffer to perform copy-on-write operations for {@link PageStoreSerialWriter}. */
    private final ThreadLocal<ByteBuffer> locBuff;

    /**
     * The length of file size per each cache partition file.
     * Partition has value greater than zero only for partitions in OWNING state.
     * Information collected under checkpoint write lock.
     */
    private final Map<GroupPartitionId, Long> partFileLengths = new HashMap<>();

    /**
     * Map of partitions to snapshot and theirs corresponding delta PageStores.
     * Writers are pinned to the snapshot context due to controlling partition
     * processing supplier.
     */
    private final Map<GroupPartitionId, PageStoreSerialWriter> partDeltaWriters = new HashMap<>();

    /**
     * List of cache configuration senders. Each sender associated with particular cache
     * configuration file to monitor it change (e.g. via SQL add/drop column or SQL index
     * create/drop operations).
     */
    private final List<CacheConfigurationSender> ccfgSndrs = new CopyOnWriteArrayList<>();

    /** {@code true} if all metastorage data must be also included into snapshot. */
    private final boolean withMetaStorage;

    /** Cache group and corresponding partitions collected under the checkpoint write lock. */
    private final Map<Integer, Set<Integer>> processed = new HashMap<>();

    /** Checkpoint end future. */
    private final CompletableFuture<Boolean> cpEndFut = new CompletableFuture<>();

    /** Future to wait until checkpoint mark phase will be finished and snapshot tasks scheduled. */
    private final GridFutureAdapter<Void> startedFut = new GridFutureAdapter<>();

    /** Absolute path to save intermediate results of cache partitions of this node. */
    private volatile File tmpConsIdDir;

    /** Future which will be completed when task requested to be closed. Will be executed on system pool. */
    private volatile CompletableFuture<Void> closeFut;

    /** Flag indicates that task already scheduled on checkpoint. */
    private final AtomicBoolean started = new AtomicBoolean();

    /** Estimated snapshot size in bytes. The value may grow during snapshot creation. */
    private final AtomicLong totalSize = new AtomicLong();

    /** Processed snapshot size in bytes. */
    private final AtomicLong processedSize = new AtomicLong();

    /** Delta writer factory. */
    private final C3<PageStore, File, Integer, PageStoreSerialWriter> deltaWriterFactory =
        cctx.snapshotMgr().sequentialWrite() ? IndexedPageStoreSerialWriter::new : PageStoreSerialWriter::new;

    /**
     * @param cctx Shared context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param snpName Unique identifier of snapshot process.
     * @param tmpWorkDir Working directory for intermediate snapshot results.
     * @param ioFactory Factory to working with snapshot files.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @param parts Map of cache groups and its partitions to include into snapshot, if set of partitions
     * is {@code null} than all OWNING partitions for given cache groups will be included into snapshot.
     */
    public SnapshotFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqId,
        String snpName,
        File tmpWorkDir,
        FileIOFactory ioFactory,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts,
        boolean withMetaStorage,
        ThreadLocal<ByteBuffer> locBuff
    ) {
        super(cctx, srcNodeId, reqId, snpName, tmpWorkDir, ioFactory, snpSndr, parts);

        assert snpName != null : "Snapshot name cannot be empty or null.";
        assert snpSndr != null : "Snapshot sender which handles execution tasks must be not null.";
        assert snpSndr.executor() != null : "Executor service must be not null.";
        assert cctx.pageStore() instanceof FilePageStoreManager : "Snapshot task can work only with physical files.";
        assert !parts.containsKey(MetaStorage.METASTORAGE_CACHE_ID) : "The withMetaStorage must be used instead.";

        this.withMetaStorage = withMetaStorage;
        this.pageStore = (FilePageStoreManager)cctx.pageStore();
        this.locBuff = locBuff;
    }

    /**
     * @return Set of cache groups included into snapshot operation.
     */
    public Set<Integer> affectedCacheGroups() {
        return parts.keySet();
    }

    /**
     * @param th An exception which occurred during snapshot processing.
     */
    @Override public void acceptException(Throwable th) {
        if (th == null)
            return;

        if (err.compareAndSet(null, th))
            closeAsync();

        startedFut.onDone(th);

        if (!(th instanceof IgniteFutureCancelledCheckedException))
            U.error(log, "Snapshot task has accepted exception to stop", th);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Set<GroupPartitionId> res, @Nullable Throwable err) {
        for (PageStoreSerialWriter writer : partDeltaWriters.values())
            U.closeQuiet(writer);

        for (CacheConfigurationSender ccfgSndr : ccfgSndrs)
            U.closeQuiet(ccfgSndr);

        snpSndr.close(err);

        if (tmpConsIdDir != null)
            U.delete(tmpConsIdDir);

        // Delete snapshot directory if no other files exists.
        try {
            if (U.fileCount(tmpSnpWorkDir.toPath()) == 0 || err != null)
                U.delete(tmpSnpWorkDir.toPath());
        }
        catch (IOException e) {
            log.error("Snapshot directory doesn't exist [snpName=" + snpName + ", dir=" + tmpSnpWorkDir + ']');
        }

        if (err != null)
            startedFut.onDone(err);

        return super.onDone(res, err);
    }

    /**
     * @return Started future.
     */
    public IgniteInternalFuture<?> started() {
        return startedFut;
    }

    /**
     * @return {@code true} if current task requested to be stopped.
     */
    private boolean stopping() {
        return err.get() != null;
    }

    /**
     * Initiates snapshot task.
     *
     * @return {@code true} if task started by this call.
     */
    @Override public boolean start() {
        if (stopping())
            return false;

        try {
            if (!started.compareAndSet(false, true))
                return false;

            tmpConsIdDir = U.resolveWorkDirectory(tmpSnpWorkDir.getAbsolutePath(),
                databaseRelativePath(cctx.kernalContext().pdsFolderResolver().resolveFolders().folderName()),
                false);

            for (Integer grpId : parts.keySet()) {
                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                if (gctx == null)
                    throw new IgniteCheckedException("Cache group context not found: " + grpId);

                if (!CU.isPersistentCache(gctx.config(), cctx.kernalContext().config().getDataStorageConfiguration()))
                    throw new IgniteCheckedException("In-memory cache groups are not allowed to be snapshot: " + grpId);

                // Create cache group snapshot directory on start in a single thread.
                U.ensureDirectory(cacheWorkDir(tmpConsIdDir, FilePageStoreManager.cacheDirName(gctx.config())),
                    "directory for snapshotting cache group",
                    log);
            }

            if (withMetaStorage) {
                U.ensureDirectory(cacheWorkDir(tmpConsIdDir, MetaStorage.METASTORAGE_DIR_NAME),
                    "directory for snapshotting metastorage",
                    log);
            }

            startedFut.listen(f ->
                ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(this)
            );

            // Listener will be removed right after first execution.
            ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(this);

            if (log.isInfoEnabled()) {
                log.info("Snapshot operation is scheduled on local node and will be handled by the checkpoint " +
                    "listener [sctx=" + this + ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
            }
        }
        catch (IgniteCheckedException e) {
            acceptException(e);

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
        if (stopping())
            return;

        ctx.finishedStateFut().listen(f -> {
            if (f.error() == null)
                cpEndFut.complete(true);
            else
                cpEndFut.completeExceptionally(f.error());
        });

        if (withMetaStorage) {
            try {
                long start = U.currentTimeMillis();

                U.get(((DistributedMetaStorageImpl)cctx.kernalContext().distributedMetastorage()).flush());

                if (log.isInfoEnabled()) {
                    log.info("Finished waiting for all the concurrent operations over the metadata store before snapshot " +
                        "[snpName=" + snpName + ", time=" + (U.currentTimeMillis() - start) + "ms]");
                }
            }
            catch (IgniteCheckedException ignore) {
                // Flushing may be cancelled or interrupted due to the local node stopping.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        // Write lock is hold. Partition pages counters has been collected under write lock.
        if (stopping())
            return;

        try {
            // Here we have the following warranties:
            // 1. Checkpoint holds write acquire lock and Snapshot holds PME. Then there are not any concurrent updates.
            // 2. This record is written before the related CheckpointRecord, and is flushed with CheckpointRecord or instead it.
            if (cctx.wal() != null) {
                cctx.wal().log(new ClusterSnapshotRecord(snpName));

                ctx.walFlush(true);
            }

            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                int grpId = e.getKey();
                Set<Integer> grpParts = e.getValue();

                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                Iterator<GridDhtLocalPartition> iter;

                if (grpParts == null)
                    iter = gctx.topology().currentLocalPartitions().iterator();
                else {
                    if (grpParts.contains(INDEX_PARTITION)) {
                        throw new IgniteCheckedException("Index partition cannot be included into snapshot if " +
                            " set of cache group partitions has been explicitly provided [grpId=" + grpId + ']');
                    }

                    iter = F.iterator(grpParts, gctx.topology()::localPartition, false);
                }

                Set<Integer> owning = new HashSet<>();
                Set<Integer> missed = new HashSet<>();

                // Iterate over partitions in particular cache group.
                while (iter.hasNext()) {
                    GridDhtLocalPartition part = iter.next();

                    // Partition can be in MOVING\RENTING states.
                    // Index partition will be excluded if not all partition OWNING.
                    // There is no data assigned to partition, thus it haven't been created yet.
                    if (part.state() == GridDhtPartitionState.OWNING)
                        owning.add(part.id());
                    else
                        missed.add(part.id());
                }

                boolean affNode = gctx.nodeFilter() == null || gctx.nodeFilter().apply(cctx.localNode());

                if (grpParts != null) {
                    // Partition has been provided for cache group, but some of them are not in OWNING state.
                    // Exit with an error.
                    if (!missed.isEmpty()) {
                        throw new IgniteCheckedException("Snapshot operation cancelled due to " +
                            "not all of requested partitions has OWNING state on local node [grpId=" + grpId +
                            ", missed=" + S.toStringSortedDistinct(missed) + ']');
                    }
                }
                else {
                    // Partitions have not been provided for snapshot task and all partitions have
                    // OWNING state, so index partition must be included into snapshot.
                    if (!missed.isEmpty()) {
                        log.warning("All local cache group partitions in OWNING state have been included into a snapshot. " +
                            "Partitions which have different states skipped. Index partitions has also been skipped " +
                            "[snpName=" + snpName + ", grpId=" + grpId + ", missed=" + S.toStringSortedDistinct(missed) + ']');
                    }
                    else if (affNode && missed.isEmpty() && cctx.kernalContext().query().moduleEnabled())
                        owning.add(INDEX_PARTITION);
                }

                processed.put(grpId, owning);
            }

            List<CacheConfiguration<?, ?>> ccfgs = new ArrayList<>();

            for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
                int grpId = e.getKey();

                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                if (gctx == null)
                    throw new IgniteCheckedException("Cache group is stopped : " + grpId);

                ccfgs.add(gctx.config());
                addPartitionWriters(grpId, e.getValue(), FilePageStoreManager.cacheDirName(gctx.config()));
            }

            if (withMetaStorage) {
                processed.put(MetaStorage.METASTORAGE_CACHE_ID, MetaStorage.METASTORAGE_PARTITIONS);

                addPartitionWriters(MetaStorage.METASTORAGE_CACHE_ID, MetaStorage.METASTORAGE_PARTITIONS,
                    MetaStorage.METASTORAGE_DIR_NAME);
            }

            cctx.cache().configManager().readConfigurationFiles(ccfgs,
                (ccfg, ccfgFile) -> ccfgSndrs.add(new CacheConfigurationSender(ccfg.getName(),
                    FilePageStoreManager.cacheDirName(ccfg), ccfgFile)));
        }
        catch (IgniteCheckedException e) {
            acceptException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        if (stopping())
            return;

        assert !processed.isEmpty() : "Partitions to process must be collected under checkpoint mark phase";

        wrapExceptionIfStarted(() -> snpSndr.init(processed.values().stream().mapToInt(Set::size).sum()))
            .run();

        // Snapshot task can now be started since checkpoint write lock released and
        // there is no error happen on task init.
        if (!startedFut.onDone())
            return;

        // Submit all tasks for partitions and deltas processing.
        List<CompletableFuture<Void>> futs = new ArrayList<>();

        if (log.isInfoEnabled()) {
            log.info("Submit partition processing tasks to the snapshot execution pool " +
                "[map=" + groupByGroupId(partFileLengths.keySet()) +
                ", totalSize=" + U.humanReadableByteCount(partFileLengths.values().stream().mapToLong(v -> v).sum()) + ']');
        }

        Collection<BinaryType> binTypesCopy = cctx.kernalContext()
            .cacheObjects()
            .metadata(Collections.emptyList())
            .values();

        // Process binary meta.
        futs.add(CompletableFuture.runAsync(
            wrapExceptionIfStarted(() -> snpSndr.sendBinaryMeta(binTypesCopy)),
            snpSndr.executor()));

        List<Map<Integer, MappedName>> mappingsCopy = cctx.kernalContext()
            .marshallerContext()
            .getCachedMappings();

        // Process marshaller meta.
        futs.add(CompletableFuture.runAsync(
            wrapExceptionIfStarted(() -> snpSndr.sendMarshallerMeta(mappingsCopy)),
            snpSndr.executor()));

        // Send configuration files of all cache groups.
        for (CacheConfigurationSender ccfgSndr : ccfgSndrs)
            futs.add(CompletableFuture.runAsync(wrapExceptionIfStarted(ccfgSndr::sendCacheConfig), snpSndr.executor()));

        try {
            for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
                int grpId = e.getKey();
                String cacheDirName = pageStore.cacheDirName(grpId);

                // Process partitions for a particular cache group.
                for (int partId : e.getValue()) {
                    GroupPartitionId pair = new GroupPartitionId(grpId, partId);

                    Long partLen = partFileLengths.get(pair);

                    totalSize.addAndGet(partLen);

                    CompletableFuture<Void> fut0 = CompletableFuture.runAsync(
                        wrapExceptionIfStarted(() -> {
                            snpSndr.sendPart(
                                getPartitionFile(pageStore.workDir(), cacheDirName, partId),
                                cacheDirName,
                                pair,
                                partLen);

                            // Stop partition writer.
                            partDeltaWriters.get(pair).markPartitionProcessed();

                            processedSize.addAndGet(partLen);
                        }),
                        snpSndr.executor())
                        // Wait for the completion of both futures - checkpoint end, copy partition.
                        .runAfterBothAsync(cpEndFut,
                            wrapExceptionIfStarted(() -> {
                                PageStoreSerialWriter writer = partDeltaWriters.get(pair);

                                writer.close();

                                File delta = writer.deltaFile;

                                try {
                                    // Atomically creates a new, empty delta file if and only if
                                    // a file with this name does not yet exist.
                                    delta.createNewFile();
                                }
                                catch (IOException ex) {
                                    throw new IgniteCheckedException(ex);
                                }

                                snpSndr.sendDelta(delta, cacheDirName, pair);

                                processedSize.addAndGet(delta.length());

                                boolean deleted = delta.delete();

                                assert deleted;

                                File deltaIdx = partDeltaIndexFile(delta);

                                if (deltaIdx.exists()) {
                                    deleted = deltaIdx.delete();

                                    assert deleted;
                                }
                            }),
                            snpSndr.executor());

                    futs.add(fut0);
                }
            }

            int futsSize = futs.size();

            CompletableFuture.allOf(futs.toArray(new CompletableFuture[futsSize]))
                .whenComplete((res, t) -> {
                    assert t == null : "Exception must never be thrown since a wrapper is used " +
                        "for each snapshot task: " + t;

                    closeAsync();
                });
        }
        catch (IgniteCheckedException e) {
            acceptException(e);
        }
    }

    /**
     * @param grpId Cache group id.
     * @param parts Set of partitions to be processed.
     * @param dirName Directory name to init.
     * @throws IgniteCheckedException If fails.
     */
    private void addPartitionWriters(int grpId, Set<Integer> parts, String dirName) throws IgniteCheckedException {
        Integer encGrpId = cctx.cache().isEncrypted(grpId) ? grpId : null;

        for (int partId : parts) {
            GroupPartitionId pair = new GroupPartitionId(grpId, partId);

            PageStore store = pageStore.getStore(grpId, partId);
            File delta = partDeltaFile(cacheWorkDir(tmpConsIdDir, dirName), partId);

            partDeltaWriters.put(pair, deltaWriterFactory.apply(store, delta, encGrpId));

            partFileLengths.put(pair, store.size());
        }
    }

    /**
     * @param exec Runnable task to execute.
     * @return Wrapped task.
     */
    private Runnable wrapExceptionIfStarted(IgniteThrowableRunner exec) {
        return () -> {
            if (stopping())
                return;

            try {
                exec.run();
            }
            catch (Throwable t) {
                acceptException(t);
            }
        };
    }

    /**
     * @return Future which will be completed when operations truly stopped.
     */
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFut == null) {
            Throwable err0 = err.get();

            // Zero partitions haven't to be written on disk.
            Set<GroupPartitionId> taken = partFileLengths.entrySet().stream()
                .filter(e -> e.getValue() > 0)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            closeFut = CompletableFuture.runAsync(() -> onDone(taken, err0),
                cctx.kernalContext().pools().getSystemExecutorService());
        }

        return closeFut;
    }

    /** @return Estimated snapshot size in bytes. The value may grow during snapshot creation. */
    public long totalSize() {
        return totalSize.get();
    }

    /** @return Processed snapshot size in bytes. */
    public long processedSize() {
        return processedSize.get();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        super.cancel();

        try {
            closeAsync().get();
        }
        catch (InterruptedException | ExecutionException e) {
            U.error(log, "SnapshotFutureTask cancellation failed", e);

            return false;
        }

        return true;
    }

    /**
     * @param grps List of processing pairs.
     *
     * @return Map with cache group id's associated to corresponding partitions.
     */
    private static Map<Integer, String> groupByGroupId(Collection<GroupPartitionId> grps) {
        return grps.stream()
            .collect(Collectors.groupingBy(GroupPartitionId::getGroupId,
                Collectors.mapping(GroupPartitionId::getPartitionId,
                    Collectors.toSet())))
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> S.toStringSortedDistinct(e.getValue())));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SnapshotFutureTask ctx = (SnapshotFutureTask)o;

        return snpName.equals(ctx.snpName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(snpName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotFutureTask.class, this, super.toString());
    }

    /** */
    private class CacheConfigurationSender implements BiConsumer<String, File>, Closeable {
        /** Cache name associated with configuration file. */
        private final String cacheName;

        /** Cache directory associated with configuration file. */
        private final String cacheDirName;

        /** Lock for cache configuration processing. */
        private final Lock lock = new ReentrantLock();

        /** Configuration file to send. */
        private volatile File ccfgFile;

        /** {@code true} if configuration file already sent. */
        private volatile boolean sent;

        /**
         * {@code true} if an old configuration file written to the temp directory and
         * waiting to be sent.
         */
        private volatile boolean fromTemp;

        /**
         * @param ccfgFile Cache configuration to send.
         * @param cacheDirName Cache directory.
         */
        public CacheConfigurationSender(String cacheName, String cacheDirName, File ccfgFile) {
            this.cacheName = cacheName;
            this.cacheDirName = cacheDirName;
            this.ccfgFile = ccfgFile;

            cctx.cache().configManager().addConfigurationChangeListener(this);
        }

        /**
         * Send the original cache configuration file or the temp one instead saved due to
         * concurrent configuration change operation happened (e.g. SQL add/drop column).
         */
        public void sendCacheConfig() {
            lock.lock();

            try {
                snpSndr.sendCacheConfig(ccfgFile, cacheDirName);

                close0();
            }
            finally {
                lock.unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void accept(String cacheName, File ccfgFile) {
            assert ccfgFile.exists() :
                "Cache configuration file must exist [cacheName=" + cacheName +
                    ", ccfgFile=" + ccfgFile.getAbsolutePath() + ']';

            if (stopping())
                return;

            if (!cacheName.equals(this.cacheName) || sent || fromTemp)
                return;

            lock.lock();

            try {
                if (sent || fromTemp)
                    return;

                File cacheWorkDir = cacheWorkDir(tmpSnpWorkDir, cacheDirName);

                if (!U.mkdirs(cacheWorkDir))
                    throw new IOException("Unable to create temp directory to copy original configuration file: " + cacheWorkDir);

                File newCcfgFile = new File(cacheWorkDir, ccfgFile.getName());
                newCcfgFile.createNewFile();

                copy(ioFactory, ccfgFile, newCcfgFile, ccfgFile.length());

                this.ccfgFile = newCcfgFile;
                fromTemp = true;
            }
            catch (IOException e) {
                acceptException(e);
            }
            finally {
                lock.unlock();
            }
        }

        /** Close writer and remove listener. */
        private void close0() {
            sent = true;
            cctx.cache().configManager().removeConfigurationChangeListener(this);

            if (fromTemp)
                U.delete(ccfgFile);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            lock.lock();

            try {
                close0();
            }
            finally {
                lock.unlock();
            }
        }
    }

    /** */
    private class PageStoreSerialWriter implements PageWriteListener, Closeable {
        /** Page store to which current writer is related to. */
        @GridToStringExclude
        protected final PageStore store;

        /** Partition delta file to store delta pages into. */
        protected final File deltaFile;

        /** Id of encrypted cache group. If {@code null}, no encrypted IO is used. */
        private final Integer encryptedGrpId;

        /** Busy lock to protect write operations. */
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** {@code true} if need the original page from PageStore instead of given buffer. */
        @GridToStringExclude
        private final BooleanSupplier checkpointComplete = () ->
            cpEndFut.isDone() && !cpEndFut.isCompletedExceptionally();

        /**
         * Array of bits. 1 - means pages written, 0 - the otherwise.
         * Size of array can be estimated only under checkpoint write lock.
         */
        private final AtomicBitSet writtenPages;

        /** IO over the underlying delta file. */
        @GridToStringExclude
        private volatile FileIO deltaFileIo;

        /** {@code true} if partition file has been copied to external resource. */
        private volatile boolean partProcessed;

        /**
         * @param store Partition page store.
         * @param deltaFile Destination file to write pages to.
         * @param encryptedGrpId Id of encrypted cache group. If {@code null}, no encrypted IO is used.
         */
        public PageStoreSerialWriter(PageStore store, File deltaFile, @Nullable Integer encryptedGrpId) {
            assert store != null;
            assert cctx.database().checkpointLockIsHeldByThread();

            this.deltaFile = deltaFile;
            this.store = store;
            // It is important to init {@link AtomicBitSet} under the checkpoint write-lock.
            // This guarantee us that no pages will be modified and it's safe to init pages
            // list which needs to be processed.
            writtenPages = new AtomicBitSet(store.pages());
            this.encryptedGrpId = encryptedGrpId;

            store.addWriteListener(this);
        }

        /**
         * @return {@code true} if writer is stopped and cannot write pages.
         */
        public boolean stopped() {
            return (checkpointComplete.getAsBoolean() && partProcessed) || stopping();
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

        /** */
        protected void init() throws IOException {
            deltaFileIo = (encryptedGrpId == null ? ioFactory :
                pageStore.encryptedFileIoFactory(ioFactory, encryptedGrpId)).create(deltaFile);
        }

        /** {@inheritDoc} */
        @Override public void accept(long pageId, ByteBuffer buf) {
            assert buf.position() == 0 : buf.position();
            assert buf.order() == ByteOrder.nativeOrder() : buf.order();

            if (deltaFileIo == null) {
                lock.writeLock().lock();

                try {
                    if (stopped())
                        return;

                    if (deltaFileIo == null)
                        init();
                }
                catch (IOException e) {
                    acceptException(e);
                }
                finally {
                    lock.writeLock().unlock();
                }
            }

            int pageIdx = -1;

            lock.readLock().lock();

            try {
                if (stopped())
                    return;

                pageIdx = PageIdUtils.pageIndex(pageId);

                if (checkpointComplete.getAsBoolean()) {
                    // Page already written.
                    if (!writtenPages.touch(pageIdx))
                        return;

                    final ByteBuffer locBuf = locBuff.get();

                    assert locBuf.capacity() == store.getPageSize();

                    locBuf.clear();

                    if (!store.read(pageId, locBuf, true))
                        return;

                    locBuf.flip();

                    writePage0(pageId, locBuf);
                }
                else {
                    // Direct buffer is needs to be written, associated checkpoint not finished yet.
                    writePage0(pageId, buf);

                    // Page marked as written to delta file, so there is no need to
                    // copy it from file when the first checkpoint associated with
                    // current snapshot task ends.
                    writtenPages.touch(pageIdx);
                }
            }
            catch (Throwable ex) {
                acceptException(new IgniteCheckedException("Error during writing pages to delta partition file " +
                    "[pageIdx=" + pageIdx + ", writer=" + this + ']', ex));
            }
            finally {
                lock.readLock().unlock();
            }
        }

        /**
         * @param pageId Page ID.
         * @param pageBuf Page buffer to write.
         * @throws IOException If page writing failed (IO error occurred).
         */
        protected synchronized void writePage0(long pageId, ByteBuffer pageBuf) throws IOException {
            assert deltaFileIo != null : "Delta pages storage is not inited: " + this;
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                + " should be same with " + ByteOrder.nativeOrder();

            if (log.isDebugEnabled()) {
                log.debug("onPageWrite [pageId=" + pageId +
                    ", pageIdBuff=" + PageIO.getPageId(pageBuf) +
                    ", fileSize=" + deltaFileIo.size() +
                    ", crcBuff=" + FastCrc.calcCrc(pageBuf, pageBuf.limit()) +
                    ", crcPage=" + PageIO.getCrc(pageBuf) + ']');

                pageBuf.rewind();
            }

            // Write buffer to the end of the file.
            int len = deltaFileIo.writeFully(pageBuf);

            totalSize.addAndGet(len);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            lock.writeLock().lock();

            try {
                U.closeQuiet(deltaFileIo);

                deltaFileIo = null;

                store.removeWriteListener(this);
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PageStoreSerialWriter.class, this);
        }
    }

    /** @see IgniteSnapshotManager.DeltaSortedIterator */
    private class IndexedPageStoreSerialWriter extends PageStoreSerialWriter {
        /** Delta index file IO. */
        @GridToStringExclude
        private volatile FileIO idxIo;

        /** Buffer of page indexes written to the delta. */
        private volatile ByteBuffer pageIdxs;

        /** */
        public IndexedPageStoreSerialWriter(PageStore store, File deltaFile, @Nullable Integer encryptedGrpId) {
            super(store, deltaFile, encryptedGrpId);
        }

        /** {@inheritDoc} */
        @Override protected void init() throws IOException {
            super.init();

            idxIo = ioFactory.create(partDeltaIndexFile(deltaFile));

            pageIdxs = ByteBuffer.allocate(store.getPageSize()).order(ByteOrder.nativeOrder());

            assert pageIdxs.capacity() % 4 == 0;
        }

        /** {@inheritDoc} */
        @Override protected synchronized void writePage0(long pageId, ByteBuffer pageBuf) throws IOException {
            super.writePage0(pageId, pageBuf);

            pageIdxs.putInt(PageIdUtils.pageIndex(pageId));

            if (!pageIdxs.hasRemaining())
                flush();
        }

        /** Flush buffer with page indexes to the file. */
        private void flush() throws IOException {
            pageIdxs.flip();

            idxIo.writeFully(pageIdxs);

            pageIdxs.clear();
        }

        /** {@inheritDoc} */
        @Override public void close() {
            super.close();

            try {
                if (idxIo != null)
                    flush();
            }
            catch (IOException e) {
                acceptException(new IgniteCheckedException("Error during writing page indexes to delta " +
                    "partition index file [writer=" + this + ']', e));
            }

            U.closeQuiet(idxIo);

            idxIo = null;
        }
    }

    /**
     *
     */
    private static class AtomicBitSet {
        /** Container of bits. */
        private final AtomicIntegerArray arr;

        /** Size of array of bits. */
        private final int size;

        /**
         * @param size Size of array.
         */
        public AtomicBitSet(int size) {
            this.size = size;

            arr = new AtomicIntegerArray((size + 31) >>> 5);
        }

        /**
         * @param off Bit position to change.
         * @return {@code true} if bit has been set,
         * {@code false} if bit changed by another thread or out of range.
         */
        public boolean touch(long off) {
            if (off >= size)
                return false;

            int bit = 1 << off;
            int bucket = (int)(off >>> 5);

            while (true) {
                int cur = arr.get(bucket);
                int val = cur | bit;

                if (cur == val)
                    return false;

                if (arr.compareAndSet(bucket, cur, val))
                    return true;
            }
        }
    }
}
