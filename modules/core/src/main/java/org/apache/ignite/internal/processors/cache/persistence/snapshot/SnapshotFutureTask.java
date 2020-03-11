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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageWriteListener;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.getPartionDeltaFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.relativeNodePath;

/**
 *
 */
class SnapshotFutureTask extends GridFutureAdapter<Boolean> implements DbCheckpointListener {
    /** Shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Ignite logger */
    private final IgniteLogger log;

    /** Node id which cause snapshot operation. */
    private final UUID srcNodeId;

    /** Unique identifier of snapshot process. */
    private final String snpName;

    /** Snapshot working directory on file system. */
    private final File tmpTaskWorkDir;

    /** IO factory which will be used for creating snapshot delta-writers. */
    private final FileIOFactory ioFactory;

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

    /** Snapshot data sender. */
    @GridToStringExclude
    private final SnapshotFileSender snpSndr;

    /**
     * Initial map of cache groups and its partitions to include into snapshot. If array of partitions
     * is {@code null} than all OWNING partitions for given cache groups will be included into snapshot.
     * In this case if all of partitions have OWNING state the index partition also will be included.
     * <p>
     * If partitions for particular cache group are not provided that they will be collected and added
     * on checkpoint under the write lock.
     */
    private final Map<Integer, Optional<Set<Integer>>> parts;

    /** Cache group and corresponding partitions collected under the checkpoint write lock. */
    private final Map<Integer, Set<Integer>> processed = new HashMap<>();

    /** Collection of partition to be snapshotted. */
    private final List<GroupPartitionId> pairs = new ArrayList<>();

    /** Checkpoint end future. */
    private final CompletableFuture<Boolean> cpEndFut = new CompletableFuture<>();

    /** Future to wait until checkpoint mark pahse will be finished and snapshot tasks scheduled. */
    private final GridFutureAdapter<Void> startedFut = new GridFutureAdapter<>();

    /** Absolute snapshot storage path. */
    private File tmpSnpDir;

    /** {@code true} if operation has been cancelled. */
    private volatile boolean cancelled;

    /** An exception which has been ocurred during snapshot processing. */
    private final AtomicReference<Throwable> err = new AtomicReference<>();

    /** Flag indicates the task must be interrupted. */
    private final BooleanSupplier stopping = () -> cancelled || err.get() != null;

    /**
     * @param e Finished snapshot tosk future with particular exception.
     */
    public SnapshotFutureTask(IgniteCheckedException e) {
        A.notNull(e, "Exception for a finished snapshot task must be not null");

        cctx = null;
        log = null;
        snpName = null;
        srcNodeId = null;
        tmpTaskWorkDir = null;
        snpSndr = null;

        err.set(e);
        startedFut.onDone(e);
        onDone(e);
        parts = null;
        ioFactory = null;
    }

    /**
     * @param snpName Unique identifier of snapshot task.
     * @param ioFactory Factory to working with delta as file storage.
     * @param parts Map of cache groups and its partitions to include into snapshot, if array of partitions
     * is {@code null} than all OWNING partitions for given cache groups will be included into snapshot.
     */
    public SnapshotFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        String snpName,
        File tmpWorkDir,
        FileIOFactory ioFactory,
        SnapshotFileSender snpSndr,
        Map<Integer, Optional<Set<Integer>>> parts
    ) {
        A.notNull(snpName, "Snapshot name cannot be empty or null");
        A.notNull(snpSndr, "Snapshot sender which handles execution tasks must be not null");
        A.notNull(snpSndr.executor(), "Executor service must be not null");

        this.parts = parts;
        this.cctx = cctx;
        this.log = cctx.logger(SnapshotFutureTask.class);
        this.snpName = snpName;
        this.srcNodeId = srcNodeId;
        this.tmpTaskWorkDir = new File(tmpWorkDir, snpName);
        this.snpSndr = snpSndr;
        this.ioFactory = ioFactory;
    }

    /**
     * @return Node id which triggers this operation..
     */
    public UUID sourceNodeId() {
        return srcNodeId;
    }

    /**
     * @return Type of snapshot operation.
     */
    public Class<? extends SnapshotFileSender> type() {
        return snpSndr.getClass();
    }

    /**
     * @return List of partitions to be processed.
     */
    public Set<Integer> affectedCacheGroups() {
        return parts.keySet();
    }

    /**
     * @param th An exception which occurred during snapshot processing.
     */
    public void acceptException(Throwable th) {
        if (th == null)
            return;

        if (err.compareAndSet(null, th))
            closeAsync();

        startedFut.onDone(th);

        log.error("Exception occurred during snapshot operation", th);
    }

    /**
     * Close snapshot operation and release resources being used.
     */
    private void close() {
        if (isDone())
            return;

        Throwable err0 = err.get();

        if (onDone(true, err0, cancelled)) {
            for (PageStoreSerialWriter writer : partDeltaWriters.values())
                U.closeQuiet(writer);

            snpSndr.close(err0);

            if (tmpSnpDir != null)
                U.delete(tmpSnpDir);

            // Delete snapshot directory if no other files exists.
            try {
                if (U.fileCount(tmpTaskWorkDir.toPath()) == 0 || err0 != null)
                    U.delete(tmpTaskWorkDir.toPath());
            }
            catch (IOException e) {
                log.error("Snapshot directory doesn't exist [snpName=" + snpName + ", dir=" + tmpTaskWorkDir + ']');
            }

            if (err0 != null)
                startedFut.onDone(err0);
        }
    }

    /**
     * @throws IgniteCheckedException If fails.
     */
    public void awaitStarted() throws IgniteCheckedException {
        startedFut.get();
    }

    /**
     * Initiates snapshot task.
     */
    public void start() {
        if (stopping.getAsBoolean())
            return;

        try {
            tmpSnpDir = U.resolveWorkDirectory(tmpTaskWorkDir.getAbsolutePath(),
                relativeNodePath(cctx.kernalContext().pdsFolderResolver().resolveFolders()),
                false);

            snpSndr.init();

            for (Integer grpId : parts.keySet()) {
                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                if (gctx == null)
                    throw new IgniteCheckedException("Cache group context has not found. Cache group is stopped: " + grpId);

                if (!CU.isPersistentCache(gctx.config(), cctx.kernalContext().config().getDataStorageConfiguration()))
                    throw new IgniteCheckedException("In-memory cache groups are not allowed to be snapshotted: " + grpId);

                if (gctx.config().isEncryptionEnabled())
                    throw new IgniteCheckedException("Encrypted cache groups are note allowed to be snapshotted: " + grpId);

                // Create cache group snapshot directory on start in a single thread.
                U.ensureDirectory(cacheWorkDir(tmpSnpDir, cacheDirName(gctx.config())),
                    "directory for snapshotting cache group",
                    log);
            }

            startedFut.listen(f ->
                ((GridCacheDatabaseSharedManager)cctx.database()).removeCheckpointListener(this)
            );

            // Listener will be removed right after first execution
            ((GridCacheDatabaseSharedManager)cctx.database()).addCheckpointListener(this);

            if (log.isInfoEnabled()) {
                log.info("Snapshot operation is scheduled on local node and will be handled by the checkpoint " +
                    "listener [sctx=" + this + ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
            }
        }
        catch (IgniteCheckedException e) {
            acceptException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        if (stopping.getAsBoolean())
            return;

        ctx.finishedStateFut().listen(f -> {
            if (f.error() == null)
                cpEndFut.complete(true);
            else
                cpEndFut.completeExceptionally(f.error());
        });
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        // Write lock is helded. Partition pages counters has been collected under write lock.
        if (stopping.getAsBoolean())
            return;

        for (Map.Entry<Integer, Optional<Set<Integer>>> e : parts.entrySet()) {
            int grpId = e.getKey();

            GridDhtPartitionTopology top = cctx.cache().cacheGroup(grpId).topology();

            Iterator<GridDhtLocalPartition> iter = e.getValue()
                .map(new Function<Set<Integer>, Iterator<GridDhtLocalPartition>>() {
                    @Override public Iterator<GridDhtLocalPartition> apply(Set<Integer> p) {
                        return new Iterator<GridDhtLocalPartition>() {
                            Iterator<Integer> iter = p.iterator();

                            @Override public boolean hasNext() {
                                return iter.hasNext();
                            }

                            @Override public GridDhtLocalPartition next() {
                                int partId = iter.next();

                                return top.localPartition(partId);
                            }
                        };
                    }
                }).orElse(top.currentLocalPartitions().iterator());

            Set<Integer> owning = processed.computeIfAbsent(grpId, g -> new HashSet<>());
            Set<Integer> missed = new HashSet<>();

            // Iterate over partition in particular cache group
            while (iter.hasNext()) {
                GridDhtLocalPartition part = iter.next();

                // Partition can be reserved.
                // Partition can be MOVING\RENTING states.
                // Index partition will be excluded if not all partition OWNING.
                // There is no data assigned to partition, thus it haven't been created yet.
                if (part.state() == GridDhtPartitionState.OWNING)
                    owning.add(part.id());
                else
                    missed.add(part.id());
            }

            // Partitions has not been provided for snapshot task and all partitions have
            // OWNING state, so index partition must be included into snapshot.
            if (!e.getValue().isPresent()) {
                if (missed.isEmpty())
                    owning.add(INDEX_PARTITION);
                else {
                    log.warning("All local cache group partitions in OWNING state have been included into a snapshot. " +
                        "Partitions which have different states skipped. Index partitions has also been skipped " +
                        "[snpName=" + snpName + ", missed=" + missed + ']');
                }
            }

            // Partition has been provided for cache group, but some of them are not in OWNING state.
            // Exit with an error
            if (!missed.isEmpty() && e.getValue().isPresent()) {
                acceptException(new IgniteCheckedException("Snapshot operation cancelled due to " +
                    "not all of requested partitions has OWNING state on local node [grpId=" + grpId +
                    ", missed" + missed + ']'));
            }
        }

        if (stopping.getAsBoolean())
            return;

        try {
            CompletableFuture<Boolean> cpEndFut0 = cpEndFut;

            for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
                int grpId = e.getKey();

                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                for (int partId : e.getValue()) {
                    GroupPartitionId pair = new GroupPartitionId(grpId, partId);

                    PageStore store = ((FilePageStoreManager)cctx.pageStore()).getStore(grpId, partId);

                    partDeltaWriters.put(pair,
                        new PageStoreSerialWriter(log,
                            store,
                            () -> cpEndFut0.isDone() && !cpEndFut0.isCompletedExceptionally(),
                            stopping,
                            this::acceptException,
                            getPartionDeltaFile(cacheWorkDir(tmpSnpDir, cacheDirName(gctx.config())),
                                partId),
                            ioFactory,
                            cctx.kernalContext()
                                .config()
                                .getDataStorageConfiguration()
                                .getPageSize()));

                    partFileLengths.put(pair, store.size());
                    partDeltaWriters.get(pair).init(store.pages());
                }
            }
        }
        catch (IgniteCheckedException e) {
            acceptException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        if (stopping.getAsBoolean())
            return;

        // Snapshot task is now started since checkpoint writelock released.
        if (!startedFut.onDone())
            return;

        // Submit all tasks for partitions and deltas processing.
        List<CompletableFuture<Void>> futs = new ArrayList<>();

        if (log.isInfoEnabled())
            log.info("Submit partition processings tasks with partition allocated lengths: " + partFileLengths);

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

        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
            int grpId = e.getKey();

            CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

            if (gctx == null) {
                acceptException(new IgniteCheckedException("Cache group context has not found " +
                    "due to the cache group is stopped: " + grpId));

                break;
            }

            // Process the cache group configuration files.
            futs.add(CompletableFuture.runAsync(
                wrapExceptionIfStarted(() -> {
                    List<File> ccfgs = storeMgr.configurationFiles(gctx.config());

                    if (ccfgs == null)
                        return;

                    for (File ccfg0 : ccfgs)
                        snpSndr.sendCacheConfig(ccfg0, cacheDirName(gctx.config()));
                }),
                snpSndr.executor())
            );

            // Process partitions for a particular cache group.
            for (int partId : e.getValue()) {
                GroupPartitionId pair = new GroupPartitionId(grpId, partId);

                CacheConfiguration<?, ?> ccfg = gctx.config();

                assert ccfg != null : "Cache configuraction cannot be empty on snapshot creation: " + pair;

                String cacheDirName = cacheDirName(ccfg);
                Long partLen = partFileLengths.get(pair);

                CompletableFuture<Void> fut0 = CompletableFuture.runAsync(
                    wrapExceptionIfStarted(() -> {
                        snpSndr.sendPart(
                            getPartitionFile(storeMgr.workDir(), cacheDirName, partId),
                            cacheDirName,
                            pair,
                            partLen);

                        // Stop partition writer.
                        partDeltaWriters.get(pair).markPartitionProcessed();
                    }),
                    snpSndr.executor())
                    // Wait for the completion of both futures - checkpoint end, copy partition.
                    .runAfterBothAsync(cpEndFut,
                        wrapExceptionIfStarted(() -> {
                            File delta = getPartionDeltaFile(cacheWorkDir(tmpSnpDir, cacheDirName), partId);

                            snpSndr.sendDelta(delta, cacheDirName, pair);

                            boolean deleted = delta.delete();

                            assert deleted;
                        }),
                        snpSndr.executor());

                futs.add(fut0);
            }
        }

        int futsSize = futs.size();

        CompletableFuture.allOf(futs.toArray(new CompletableFuture[futsSize]))
            .whenComplete((res, t) -> {
                assert t == null : "Excepction must never be thrown since a wrapper is used " +
                    "for each snapshot task: " + t;

                close();
            });
    }

    /**
     * @param exec Runnable task to execute.
     * @return Wrapped task.
     */
    private Runnable wrapExceptionIfStarted(IgniteThrowableRunner exec) {
        return () -> {
            if (stopping.getAsBoolean())
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
     * @return Future which will be completed when operations truhly stopped.
     */
    public CompletableFuture<Void> closeAsync() {
        // Execute on SYSTEM_POOL
        return CompletableFuture.runAsync(this::close, cctx.kernalContext().getSystemExecutorService());
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        cancelled = true;

        try {
            closeAsync().get();
        }
        catch (InterruptedException | ExecutionException e) {
            U.error(log, "SnapshotFutureTask cancellation failed", e);

            return false;
        }

        return true;
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
        return S.toString(SnapshotFutureTask.class, this);
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

        /** {@code true} if snapshot process is stopping or alredy stopped. */
        private final BooleanSupplier interrupt;

        /** Callback to stop snapshot if an error occurred. */
        private final Consumer<Throwable> exConsumer;

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
        private volatile AtomicBitSet pagesWrittenBits;

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
            BooleanSupplier interrupt,
            Consumer<Throwable> exConsumer,
            File cfgFile,
            FileIOFactory factory,
            int pageSize
        ) throws IgniteCheckedException {
            assert store != null;

            try {
                fileIo = factory.create(cfgFile);
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }

            this.checkpointComplete = checkpointComplete;
            this.interrupt = interrupt;
            this.exConsumer = exConsumer;
            this.log = log.getLogger(PageStoreSerialWriter.class);

            localBuff = ThreadLocal.withInitial(() ->
                ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder()));

            this.store = store;

            store.addWriteListener(this);
        }

        /**
         * It is important to init {@link AtomicBitSet} under the checkpoint write-lock.
         * This guarantee us that no pages will be modified and it's safe to init pages list
         * which needs to be processed.
         *
         * @param allocPages Total number of tracking pages.
         */
        public void init(int allocPages) {
            lock.writeLock().lock();

            try {
                pagesWrittenBits = new AtomicBitSet(allocPages);
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
            return (checkpointComplete.getAsBoolean() && partProcessed) || interrupt.getAsBoolean();
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

            lock.readLock().lock();

            try {
                if (!inited)
                    return;

                if (stopped())
                    return;

                if (checkpointComplete.getAsBoolean()) {
                    int pageIdx = PageIdUtils.pageIndex(pageId);

                    // Page already written.
                    if (!pagesWrittenBits.touch(pageIdx))
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
                exConsumer.accept(ex);
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
        private void writePage0(long pageId, ByteBuffer pageBuf) throws IOException {
            assert fileIo != null : "Delta pages storage is not inited: " + this;
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                + " should be same with " + ByteOrder.nativeOrder();

            int crc = PageIO.getCrc(pageBuf);
            int crc32 = FastCrc.calcCrc(pageBuf, pageBuf.limit());

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
            if (off > size)
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
