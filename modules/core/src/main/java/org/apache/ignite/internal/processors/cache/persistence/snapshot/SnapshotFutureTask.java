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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.copy;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.partDeltaFile;

/**
 *
 */
class SnapshotFutureTask extends GridFutureAdapter<Boolean> implements DbCheckpointListener {
    /** Shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** File page store manager for accessing cache group associated files. */
    private final FilePageStoreManager pageStore;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Node id which cause snapshot operation. */
    private final UUID srcNodeId;

    /** Unique identifier of snapshot process. */
    private final String snpName;

    /** Snapshot working directory on file system. */
    private final File tmpSnpWorkDir;

    /** Local buffer to perform copy-on-write operations for {@link PageStoreSerialWriter}. */
    private final ThreadLocal<ByteBuffer> locBuff;

    /** IO factory which will be used for creating snapshot delta-writers. */
    private final FileIOFactory ioFactory;

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

    /** Snapshot data sender. */
    @GridToStringExclude
    private final SnapshotSender snpSndr;

    /**
     * Requested map of cache groups and its partitions to include into snapshot. If array of partitions
     * is {@code null} than all OWNING partitions for given cache groups will be included into snapshot.
     * In this case if all of partitions have OWNING state the index partition also will be included.
     * <p>
     * If partitions for particular cache group are not provided that they will be collected and added
     * on checkpoint under the write lock.
     */
    private final Map<Integer, Set<Integer>> parts;

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

    /** An exception which has been occurred during snapshot processing. */
    private final AtomicReference<Throwable> err = new AtomicReference<>();

    /** Flag indicates that task already scheduled on checkpoint. */
    private final AtomicBoolean started = new AtomicBoolean();

    /**
     * @param e Finished snapshot task future with particular exception.
     */
    public SnapshotFutureTask(IgniteCheckedException e) {
        assert e != null : "Exception for a finished snapshot task must be not null";

        cctx = null;
        pageStore = null;
        log = null;
        snpName = null;
        srcNodeId = null;
        tmpSnpWorkDir = null;
        snpSndr = null;

        err.set(e);
        startedFut.onDone(e);
        onDone(e);
        parts = null;
        ioFactory = null;
        locBuff = null;
    }

    /**
     * @param snpName Unique identifier of snapshot task.
     * @param ioFactory Factory to working with delta as file storage.
     * @param parts Map of cache groups and its partitions to include into snapshot, if set of partitions
     * is {@code null} than all OWNING partitions for given cache groups will be included into snapshot.
     */
    public SnapshotFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        String snpName,
        File tmpWorkDir,
        FileIOFactory ioFactory,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts,
        ThreadLocal<ByteBuffer> locBuff
    ) {
        assert snpName != null : "Snapshot name cannot be empty or null.";
        assert snpSndr != null : "Snapshot sender which handles execution tasks must be not null.";
        assert snpSndr.executor() != null : "Executor service must be not null.";
        assert cctx.pageStore() instanceof FilePageStoreManager : "Snapshot task can work only with physical files.";

        this.parts = parts;
        this.cctx = cctx;
        this.pageStore = (FilePageStoreManager)cctx.pageStore();
        this.log = cctx.logger(SnapshotFutureTask.class);
        this.snpName = snpName;
        this.srcNodeId = srcNodeId;
        this.tmpSnpWorkDir = new File(tmpWorkDir, snpName);
        this.snpSndr = snpSndr;
        this.ioFactory = ioFactory;
        this.locBuff = locBuff;
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return Node id which triggers this operation.
     */
    public UUID sourceNodeId() {
        return srcNodeId;
    }

    /**
     * @return Type of snapshot operation.
     */
    public Class<? extends SnapshotSender> type() {
        return snpSndr.getClass();
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
    public void acceptException(Throwable th) {
        if (th == null)
            return;

        if (err.compareAndSet(null, th))
            closeAsync();

        startedFut.onDone(th);

        if (!(th instanceof IgniteFutureCancelledCheckedException))
            U.error(log, "Snapshot task has accepted exception to stop", th);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
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
     * @throws IgniteCheckedException If fails.
     */
    public void awaitStarted() throws IgniteCheckedException {
        startedFut.get();
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
    public boolean start() {
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

                if (gctx.config().isEncryptionEnabled())
                    throw new IgniteCheckedException("Encrypted cache groups are not allowed to be snapshot: " + grpId);

                // Create cache group snapshot directory on start in a single thread.
                U.ensureDirectory(cacheWorkDir(tmpConsIdDir, cacheDirName(gctx.config())),
                    "directory for snapshotting cache group",
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
    @Override public void beforeCheckpointBegin(Context ctx) {
        if (stopping())
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
        // Write lock is hold. Partition pages counters has been collected under write lock.
        if (stopping())
            return;

        try {
            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                int grpId = e.getKey();
                Set<Integer> grpParts = e.getValue();

                GridDhtPartitionTopology top = cctx.cache().cacheGroup(grpId).topology();

                Iterator<GridDhtLocalPartition> iter;

                if (grpParts == null)
                    iter = top.currentLocalPartitions().iterator();
                else {
                    if (grpParts.contains(INDEX_PARTITION)) {
                        throw new IgniteCheckedException("Index partition cannot be included into snapshot if " +
                            " set of cache group partitions has been explicitly provided [grpId=" + grpId + ']');
                    }

                    iter = F.iterator(grpParts, top::localPartition, false);
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

                if (grpParts != null) {
                    // Partition has been provided for cache group, but some of them are not in OWNING state.
                    // Exit with an error.
                    if (!missed.isEmpty()) {
                        throw new IgniteCheckedException("Snapshot operation cancelled due to " +
                            "not all of requested partitions has OWNING state on local node [grpId=" + grpId +
                            ", missed" + missed + ']');
                    }
                }
                else {
                    // Partitions has not been provided for snapshot task and all partitions have
                    // OWNING state, so index partition must be included into snapshot.
                    if (!missed.isEmpty()) {
                        log.warning("All local cache group partitions in OWNING state have been included into a snapshot. " +
                            "Partitions which have different states skipped. Index partitions has also been skipped " +
                            "[snpName=" + snpName + ", grpId=" + grpId + ", missed=" + missed + ']');
                    }
                    else if (missed.isEmpty() && cctx.kernalContext().query().moduleEnabled())
                        owning.add(INDEX_PARTITION);
                }

                processed.put(grpId, owning);
            }

            List<CacheConfiguration<?, ?>> ccfgs = new ArrayList<>();

            for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
                int grpId = e.getKey();

                CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

                if (gctx == null) {
                    throw new IgniteCheckedException("Cache group context has not found " +
                        "due to the cache group is stopped: " + grpId);
                }

                for (int partId : e.getValue()) {
                    GroupPartitionId pair = new GroupPartitionId(grpId, partId);

                    PageStore store = pageStore.getStore(grpId, partId);

                    partDeltaWriters.put(pair,
                        new PageStoreSerialWriter(store,
                            partDeltaFile(cacheWorkDir(tmpConsIdDir, cacheDirName(gctx.config())), partId)));

                    partFileLengths.put(pair, store.size());
                }

                ccfgs.add(gctx.config());
            }

            pageStore.readConfigurationFiles(ccfgs,
                (ccfg, ccfgFile) -> ccfgSndrs.add(new CacheConfigurationSender(ccfg.getName(), cacheDirName(ccfg), ccfgFile)));
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

        if (log.isInfoEnabled())
            log.info("Submit partition processing tasks with partition allocated lengths: " + partFileLengths);

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

        for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
            int grpId = e.getKey();

            CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

            if (gctx == null) {
                acceptException(new IgniteCheckedException("Cache group context has not found " +
                    "due to the cache group is stopped: " + grpId));

                break;
            }

            // Process partitions for a particular cache group.
            for (int partId : e.getValue()) {
                GroupPartitionId pair = new GroupPartitionId(grpId, partId);

                CacheConfiguration<?, ?> ccfg = gctx.config();

                assert ccfg != null : "Cache configuration cannot be empty on snapshot creation: " + pair;

                String cacheDirName = cacheDirName(ccfg);
                Long partLen = partFileLengths.get(pair);

                CompletableFuture<Void> fut0 = CompletableFuture.runAsync(
                    wrapExceptionIfStarted(() -> {
                        snpSndr.sendPart(
                            getPartitionFile(pageStore.workDir(), cacheDirName, partId),
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
                            File delta = partDeltaWriters.get(pair).deltaFile;

                            try {
                                // Atomically creates a new, empty delta file if and only if
                                // a file with this name does not yet exist.
                                delta.createNewFile();
                            }
                            catch (IOException ex) {
                                throw new IgniteCheckedException(ex);
                            }

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
                assert t == null : "Exception must never be thrown since a wrapper is used " +
                    "for each snapshot task: " + t;

                closeAsync();
            });
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

            closeFut = CompletableFuture.runAsync(() -> onDone(true, err0),
                cctx.kernalContext().getSystemExecutorService());
        }

        return closeFut;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        acceptException(new IgniteFutureCancelledCheckedException("Snapshot operation has been cancelled " +
            "by external process [snpName=" + snpName + ']'));

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

            pageStore.addConfigurationChangeListener(this);
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
            pageStore.removeConfigurationChangeListener(this);

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
        private final PageStore store;

        /** Partition delta file to store delta pages into. */
        private final File deltaFile;

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
         */
        public PageStoreSerialWriter(PageStore store, File deltaFile) {
            assert store != null;
            assert cctx.database().checkpointLockIsHeldByThread();

            this.deltaFile = deltaFile;
            this.store = store;
            // It is important to init {@link AtomicBitSet} under the checkpoint write-lock.
            // This guarantee us that no pages will be modified and it's safe to init pages
            // list which needs to be processed.
            writtenPages = new AtomicBitSet(store.pages());

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
                        deltaFileIo = ioFactory.create(deltaFile);
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
        private void writePage0(long pageId, ByteBuffer pageBuf) throws IOException {
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
            deltaFileIo.writeFully(pageBuf);
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
