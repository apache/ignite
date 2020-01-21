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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageWriteListener;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.getPartionDeltaFile;

/**
 *
 */
class SnapshotTask implements DbCheckpointListener, Closeable {
    /** Shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Ignite logger */
    private final IgniteLogger log;

    /** Factory to working with delta as file storage. */
    private final FileIOFactory ioFactory;

    /** Node id which cause snapshot operation. */
    private final UUID srcNodeId;

    /** Unique identifier of snapshot process. */
    private final String snpName;

    /** Snapshot working directory on file system. */
    private final File snpWorkDir;

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
    private final SnapshotTaskFuture snpFut = new SnapshotTaskFuture(() -> {
        cancelled = true;

        closeAsync().get();
    });

    /** Snapshot data sender. */
    @GridToStringExclude
    private final SnapshotFileSender snpSndr;

    /** Collection of partition to be snapshotted. */
    private final List<GroupPartitionId> parts = new ArrayList<>();

    /** Checkpoint end future. */
    private final CompletableFuture<Boolean> cpEndFut = new CompletableFuture<>();

    /** Latch to wait until checkpoint mark pahse will be finished and snapshot tasks scheduled. */
    private final CountDownLatch startedLatch = new CountDownLatch(1);

    /** Absolute snapshot storage path. */
    private File nodeSnpDir;

    /** An exception which has been ocurred during snapshot processing. */
    private volatile Throwable lastTh;

    /** {@code true} if operation has been cancelled. */
    private volatile boolean cancelled;

    /** Phase of the current snapshot process run. */
    private volatile SnapshotState state = SnapshotState.INIT;

    /**
     * @param snpName Unique identifier of snapshot process.
     * @param exec Service to perform partitions copy.
     */
    public SnapshotTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        String snpName,
        File snpWorkDir,
        Executor exec,
        FileIOFactory ioFactory,
        SnapshotFileSender snpSndr,
        Map<Integer, GridIntList> parts
    ) {
        A.notNull(snpName, "snapshot name cannot be empty or null");
        A.notNull(exec, "Executor service must be not null");
        A.notNull(snpSndr, "Snapshot sender which handles execution tasks must be not null");

        this.cctx = cctx;
        this.log = cctx.logger(SnapshotTask.class);
        this.snpName = snpName;
        this.srcNodeId = srcNodeId;
        this.snpWorkDir = snpWorkDir;
        this.exec = exec;
        this.ioFactory = ioFactory;
        this.snpSndr = snpSndr;

        for (Map.Entry<Integer, GridIntList> e : parts.entrySet()) {
            GridIntIterator iter = e.getValue().iterator();

            while (iter.hasNext())
                this.parts.add(new GroupPartitionId(e.getKey(), iter.next()));
        }
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
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
    public List<GroupPartitionId> partitions() {
        return parts;
    }

    /**
     * @return Future which will be completed when snapshot operation ends.
     */
    public IgniteInternalFuture<Boolean> snapshotFuture() {
        return snpFut;
    }

    /**
     * Wait for the snapshot operation task started on checkpoint.
     */
    public void awaitStarted() {
        try {
            U.await(startedLatch);
        }
        catch (IgniteInterruptedCheckedException e) {
            acceptException(e);
        }
    }

    /**
     * @param state A new snapshot state to set.
     * @return {@code true} if given state has been set by this call.
     */
    public boolean state(SnapshotState state) {
        if (this.state == state)
            return false;

        synchronized (this) {
            if (this.state == state)
                return false;

            if (state == SnapshotState.STOPPING) {
                this.state = SnapshotState.STOPPING;

                startedLatch.countDown();

                return true;
            }

            if (state.ordinal() > this.state.ordinal()) {
                this.state = state;

                if (state == SnapshotState.STARTED)
                    startedLatch.countDown();

                return true;
            }
            else
                return false;
        }
    }

    /**
     * @param th An exception which occurred during snapshot processing.
     */
    public void acceptException(Throwable th) {
        if (state(SnapshotState.STOPPING))
            lastTh = th;
    }

    /**
     * @param th Occurred exception during processing or {@code null} if not.
     */
    public void close(Throwable th) {
        if (state(SnapshotState.STOPPED)) {
            if (lastTh == null)
                lastTh = th;

            for (PageStoreSerialWriter writer : partDeltaWriters.values())
                U.closeQuiet(writer);

            snpSndr.close(lastTh);

            if (nodeSnpDir != null)
                U.delete(nodeSnpDir);

            // Delete snapshot directory if no other files exists.
            try {
                if (U.fileCount(snpWorkDir.toPath()) == 0)
                    U.delete(snpWorkDir.toPath());
            }
            catch (IOException e) {
                log.error("Snapshot directory doesn't exist [snpName=" + snpName + ", dir=" + snpWorkDir + ']');
            }

            snpFut.onDone(true, lastTh, cancelled);
        }
    }

    /**
     * @param adder Register current task on.
     * @param remover Deregister current taks on.
     */
    public void submit(Consumer<DbCheckpointListener> adder, Consumer<DbCheckpointListener> remover) {
        try {
            // todo can be performed on the given executor
            nodeSnpDir = U.resolveWorkDirectory(snpWorkDir.getAbsolutePath(), IgniteSnapshotManager.relativeStoragePath(cctx), false);

            Set<Integer> grps = parts.stream()
                .map(GroupPartitionId::getGroupId)
                .collect(Collectors.toSet());

            Map<Integer, File> dirs = new HashMap<>();

            for (Integer grpId : grps) {
                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                if (gctx == null)
                    throw new IgniteCheckedException("Cache group context has not found. Cache group is stopped: " + grpId);

                if (!CU.isPersistentCache(gctx.config(), cctx.kernalContext().config().getDataStorageConfiguration()))
                    throw new IgniteCheckedException("In-memory cache groups are not allowed to be snapshotted: " + grpId);

                if (gctx.config().isEncryptionEnabled())
                    throw new IgniteCheckedException("Encrypted cache groups are note allowed to be snapshotted: " + grpId);

                // Create cache snapshot directory if not.
                File grpDir = U.resolveWorkDirectory(nodeSnpDir.getAbsolutePath(),
                    cacheDirName(gctx.config()), false);

                U.ensureDirectory(grpDir,
                    "snapshot directory for cache group: " + gctx.groupId(),
                    null);

                dirs.put(grpId, grpDir);
            }

            CompletableFuture<Boolean> cpEndFut0 = cpEndFut;

            for (GroupPartitionId pair : parts) {
                PageStore store = ((FilePageStoreManager)cctx.pageStore()).getStore(pair.getGroupId(),
                    pair.getPartitionId());

                partDeltaWriters.put(pair,
                    new PageStoreSerialWriter(log,
                        store,
                        () -> cpEndFut0.isDone() && !cpEndFut0.isCompletedExceptionally(),
                        () -> state == SnapshotState.STOPPED || state == SnapshotState.STOPPING,
                        this::acceptException,
                        getPartionDeltaFile(dirs.get(pair.getGroupId()), pair.getPartitionId()),
                        ioFactory,
                        cctx.kernalContext()
                            .config()
                            .getDataStorageConfiguration()
                            .getPageSize()));
            }

            if (log.isInfoEnabled()) {
                log.info("Snapshot operation is scheduled on local node and will be handled by the checkpoint " +
                    "listener [sctx=" + this + ", topVer=" + cctx.discovery().topologyVersionEx() + ']');
            }

            snpFut.listen(f -> remover.accept(this));

            adder.accept(this);
        }
        catch (IgniteCheckedException e) {
            close(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        // Gather partitions metainfo for thouse which will be copied.
        if (!state(SnapshotState.MARK))
            return;

        ctx.collectPartStat(parts);

        ctx.cpFinishFut().listen(f -> {
            if (f.error() == null)
                cpEndFut.complete(true);
            else
                cpEndFut.completeExceptionally(f.error());

            // Close snapshot operation if an error with operation occurred on checkpoint begin phase
            if (state == SnapshotState.STOPPING)
                closeAsync();
        });
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        // Write lock is helded. Partition counters has been collected under write lock
        // in another checkpoint listeners.
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointEnd(Context ctx) {
        // Under the write lock here. It's safe to add new stores.
        if (!state(SnapshotState.START))
            return;

        try {
            PartitionAllocationMap allocationMap = ctx.partitionStatMap();

            allocationMap.prepareForSnapshot();

            for (GroupPartitionId pair : parts) {
                PagesAllocationRange allocRange = allocationMap.get(pair);

                GridDhtLocalPartition part = pair.getPartitionId() == INDEX_PARTITION ? null :
                    cctx.cache()
                        .cacheGroup(pair.getGroupId())
                        .topology()
                        .localPartition(pair.getPartitionId());

                // Partition can be reserved.
                // Partition can be MOVING\RENTING states.
                // Index partition will be excluded if not all partition OWNING.
                // There is no data assigned to partition, thus it haven't been created yet.
                assert allocRange != null || part == null || part.state() != GridDhtPartitionState.OWNING :
                    "Partition counters has not been collected " +
                        "[pair=" + pair + ", snpName=" + snpName + ", part=" + part + ']';

                if (allocRange == null) {
                    List<GroupPartitionId> missed = parts.stream()
                        .filter(allocationMap::containsKey)
                        .collect(Collectors.toList());

                    acceptException(new IgniteCheckedException("Snapshot operation cancelled due to " +
                        "not all of requested partitions has OWNING state [missed=" + missed + ']'));

                    break;
                }

                PageStore store = ((FilePageStoreManager)cctx.pageStore()).getStore(pair.getGroupId(), pair.getPartitionId());

                partFileLengths.put(pair, store.size());
                partDeltaWriters.get(pair).init(allocRange.getCurrAllocatedPageCnt());
            }
        }
        catch (IgniteCheckedException e) {
            acceptException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        if (!state(SnapshotState.STARTED))
            return;

        // Submit all tasks for partitions and deltas processing.
        List<CompletableFuture<Void>> futs = new ArrayList<>();
        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        if (log.isInfoEnabled())
            log.info("Submit partition processings tasks with partition allocated lengths: " + partFileLengths);

        // Process binary meta.
        futs.add(CompletableFuture.runAsync(
            wrapExceptionally(() ->
                    snpSndr.sendBinaryMeta(cctx.kernalContext()
                        .cacheObjects()
                        .metadataTypes()),
                s -> s == SnapshotState.STARTED),
            exec));

        // Process marshaller meta.
        futs.add(CompletableFuture.runAsync(
            wrapExceptionally(() ->
                    snpSndr.sendMarshallerMeta(cctx.kernalContext()
                        .marshallerContext()
                        .getCachedMappings()),
                s -> s == SnapshotState.STARTED),
            exec));

        // Process cache group configuration files.
        parts.stream()
            .map(GroupPartitionId::getGroupId)
            .collect(Collectors.toSet())
            .forEach(grpId ->
                futs.add(CompletableFuture.runAsync(() ->
                        wrapExceptionally(() -> {
                                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                                if (gctx == null) {
                                    throw new IgniteCheckedException("Cache group configuration has not found " +
                                        "due to the cache group is stopped: " + grpId);
                                }

                                List<File> ccfgs = storeMgr.configurationFiles(gctx.config());

                                if (ccfgs == null)
                                    return;

                                for (File ccfg0 : ccfgs)
                                    snpSndr.sendCacheConfig(ccfg0, cacheDirName(gctx.config()));
                            },
                            s -> s == SnapshotState.STARTED),
                    exec)
                )
            );

        // Process partitions.
        for (GroupPartitionId pair : parts) {
            CacheGroupContext gctx = cctx.cache().cacheGroup(pair.getGroupId());

            if (gctx == null) {
                acceptException(new IgniteCheckedException("Cache group context has not found " +
                    "due to the cache group is stopped: " + pair));
            }

            CacheConfiguration ccfg = gctx.config();

            assert ccfg != null : "Cache configuraction cannot be empty on snapshot creation: " + pair;

            String cacheDirName = cacheDirName(ccfg);
            Long partLen = partFileLengths.get(pair);

            CompletableFuture<Void> fut0 = CompletableFuture.runAsync(
                wrapExceptionally(() -> {
                        snpSndr.sendPart(
                            getPartitionFile(storeMgr.workDir(), cacheDirName, pair.getPartitionId()),
                            cacheDirName,
                            pair,
                            partLen);

                        // Stop partition writer.
                        partDeltaWriters.get(pair).markPartitionProcessed();
                    },
                    s -> s == SnapshotState.STARTED),
                exec)
                // Wait for the completion of both futures - checkpoint end, copy partition.
                .runAfterBothAsync(cpEndFut,
                    wrapExceptionally(() -> {
                            File delta = getPartionDeltaFile(cacheWorkDir(nodeSnpDir, cacheDirName),
                                pair.getPartitionId());

                            snpSndr.sendDelta(delta, cacheDirName, pair);

                            boolean deleted = delta.delete();

                            assert deleted;
                        },
                        s -> s == SnapshotState.STARTED),
                    exec);

            futs.add(fut0);
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
     * @param cond Condition when task must be executed.
     * @return Wrapped task.
     */
    private Runnable wrapExceptionally(IgniteThrowableRunner exec, Predicate<SnapshotState> cond) {
        return () -> {
            try {
                if (cond.test(state))
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
    public IgniteInternalFuture<Void> closeAsync() {
        GridFutureAdapter<Void> cFut = new GridFutureAdapter<>();

        CompletableFuture.runAsync(this::close, exec)
            .whenComplete((v, t) -> {
                if (t == null)
                    cFut.onDone();
                else
                    cFut.onDone(t);
            });

        return cFut;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        close(null);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SnapshotTask ctx = (SnapshotTask)o;

        return snpName.equals(ctx.snpName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(snpName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotTask.class, this);
    }

    /**
     * Valid state transitions:
     * <p>
     * {@code INIT -> MARK -> START -> STARTED -> STOPPED}
     * <p>
     * {@code INIT (or any other) -> STOPPING}
     * <p>
     * {@code CANCELLING -> STOPPED}
     */
    private enum SnapshotState {
        /** Requested partitoins must be registered to collect its partition counters. */
        INIT,

        /** All counters must be collected under the checkpoint write lock. */
        MARK,

        /** Tasks must be scheduled to create requested snapshot. */
        START,

        /** Snapshot tasks has been started. */
        STARTED,

        /** Indicates that snapshot operation must be cancelled and is awaiting resources to be freed. */
        STOPPING,

        /** Snapshot operation has been interruped or an exception occurred. */
        STOPPED
    }

    /**
     *
     */
    private static class SnapshotTaskFuture extends GridFutureAdapter<Boolean> {
        /** Set cancelling state to snapshot. */
        private final IgniteThrowableRunner doCancel;

        /**
         * @param doCancel Set cancelling state to snapshot.
         */
        public SnapshotTaskFuture(IgniteThrowableRunner doCancel) {
            this.doCancel = doCancel;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            doCancel.run();

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err, boolean cancel) {
            return super.onDone(res, err, cancel);
        }
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
