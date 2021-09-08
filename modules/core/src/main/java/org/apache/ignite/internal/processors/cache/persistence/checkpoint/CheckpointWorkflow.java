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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CheckpointWriteOrder;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridConcurrentMultiPairQueue;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.WorkProgressDispatcher;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_TAKEN;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.MARKER_STORED_TO_DISK;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.PAGE_SNAPSHOT_TAKEN;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC;
import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointReadWriteLock.CHECKPOINT_RUNNER_THREAD_PREFIX;

/**
 * This class responsibility is to complement {@link Checkpointer} class with side logic of checkpointing like
 * checkpoint listeners notifications, WAL records management and checkpoint markers management.
 *
 * It allows {@link Checkpointer} class is to focus on its main responsibility: synchronizing memory with disk.
 * Additional actions needed during checkpoint are implemented in this class.
 *
 * Two main blocks of logic this class is responsible for:
 * <p>{@link CheckpointWorkflow#markCheckpointBegin} - Initialization of next checkpoint. It collects all required
 * info</p>
 * <p>{@link CheckpointWorkflow#markCheckpointEnd} - Finalization of last checkpoint. </p>
 *
 * If also provides checkpointer with information about {@link #dataRegions} where to collect dirty pages
 * and {@link #cacheGroupsContexts} needed to perform checkpoint operation.
 */
public class CheckpointWorkflow {
    /** @see IgniteSystemProperties#CHECKPOINT_PARALLEL_SORT_THRESHOLD */
    public static final int DFLT_CHECKPOINT_PARALLEL_SORT_THRESHOLD = 512 * 1024;

    /****/
    private static final DataRegion NO_REGION = new DataRegion(null, null, null, null);

    /**
     * Starting from this number of dirty pages in checkpoint, array will be sorted with {@link
     * Arrays#parallelSort(Comparable[])} in case of {@link CheckpointWriteOrder#SEQUENTIAL}.
     */
    private final int parallelSortThreshold = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.CHECKPOINT_PARALLEL_SORT_THRESHOLD, DFLT_CHECKPOINT_PARALLEL_SORT_THRESHOLD);

    /** This number of threads will be created and used for parallel sorting. */
    private static final int PARALLEL_SORT_THREADS = Math.min(Runtime.getRuntime().availableProcessors(), 8);

    /** Skip sync. */
    private final boolean skipSync = getBoolean(IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);

    /** Logger. */
    private final IgniteLogger log;

    /** Write ahead log. */
    private final IgniteWriteAheadLogManager wal;

    /** Snapshot manager. */
    private final IgniteCacheSnapshotManager snapshotMgr;

    /** Checkpoint lock. */
    private final CheckpointReadWriteLock checkpointReadWriteLock;

    /** Data regions from which checkpoint will collect pages. */
    private final Supplier<Collection<DataRegion>> dataRegions;

    /** Cache groups from which partitions counter would be collected. */
    private final Supplier<Collection<CacheGroupContext>> cacheGroupsContexts;

    /** Checkpoint metadata directory ("cp"), contains files with checkpoint start and end markers. */
    private final CheckpointMarkersStorage checkpointMarkersStorage;

    /** Checkpoint write order configuration. */
    private final CheckpointWriteOrder checkpointWriteOrder;

    /** Collections of checkpoint listeners. */
    private final Map<CheckpointListener, DataRegion> lsnrs = new ConcurrentLinkedHashMap<>();

    /** Ignite instance name. */
    private final String igniteInstanceName;

    /** The number of CPU-bound threads which will be used for collections of info for checkpoint(ex. dirty pages). */
    private final int checkpointCollectInfoThreads;

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private volatile IgniteThreadPoolExecutor checkpointCollectPagesInfoPool;

    /** Pointer to a memory recovery record that should be included into the next checkpoint record. */
    private volatile WALPointer memoryRecoveryRecordPtr;

    /**
     * @param logger Logger.
     * @param wal WAL manager.
     * @param snapshotManager Snapshot manager.
     * @param checkpointMarkersStorage Checkpoint mark storage.
     * @param checkpointReadWriteLock Checkpoint read write lock.
     * @param checkpointWriteOrder Checkpoint write order.
     * @param dataRegions Regions for checkpointing.
     * @param cacheGroupContexts Cache group context for checkpoint.
     * @param checkpointCollectInfoThreads Number of threads which should collect info for checkpoint.
     * @param igniteInstanceName Ignite instance name.
     */
    CheckpointWorkflow(
        Function<Class<?>, IgniteLogger> logger,
        IgniteWriteAheadLogManager wal,
        IgniteCacheSnapshotManager snapshotManager,
        CheckpointMarkersStorage checkpointMarkersStorage,
        CheckpointReadWriteLock checkpointReadWriteLock,
        CheckpointWriteOrder checkpointWriteOrder,
        Supplier<Collection<DataRegion>> dataRegions,
        Supplier<Collection<CacheGroupContext>> cacheGroupContexts,
        int checkpointCollectInfoThreads,
        String igniteInstanceName
    ) {
        this.wal = wal;
        this.snapshotMgr = snapshotManager;
        this.checkpointReadWriteLock = checkpointReadWriteLock;
        this.dataRegions = dataRegions;
        this.cacheGroupsContexts = cacheGroupContexts;
        this.checkpointCollectInfoThreads = checkpointCollectInfoThreads;
        this.log = logger.apply(getClass());
        this.checkpointMarkersStorage = checkpointMarkersStorage;
        this.checkpointWriteOrder = checkpointWriteOrder;
        this.igniteInstanceName = igniteInstanceName;
        this.checkpointCollectPagesInfoPool = initializeCheckpointPool();
    }

    /**
     * @return Initialized checkpoint page write pool;
     */
    private IgniteThreadPoolExecutor initializeCheckpointPool() {
        if (checkpointCollectInfoThreads > 1)
            return new IgniteThreadPoolExecutor(
                CHECKPOINT_RUNNER_THREAD_PREFIX + "-cpu",
                igniteInstanceName,
                checkpointCollectInfoThreads,
                checkpointCollectInfoThreads,
                30_000,
                new LinkedBlockingQueue<Runnable>()
            );

        return null;
    }

    /**
     * First stage of checkpoint which collects demanded information(dirty pages mostly).
     *
     * @param cpTs Checkpoint start timestamp.
     * @param curr Current checkpoint event info.
     * @param tracker Checkpoint metrics tracker.
     * @param workProgressDispatcher Work progress dispatcher.
     * @return Checkpoint collected info.
     * @throws IgniteCheckedException if fail.
     */
    public Checkpoint markCheckpointBegin(
        long cpTs,
        CheckpointProgressImpl curr,
        CheckpointMetricsTracker tracker,
        WorkProgressDispatcher workProgressDispatcher
    ) throws IgniteCheckedException {
        Collection<DataRegion> checkpointedRegions = dataRegions.get();

        List<CheckpointListener> dbLsnrs = getRelevantCheckpointListeners(checkpointedRegions);

        CheckpointRecord cpRec = new CheckpointRecord(memoryRecoveryRecordPtr);

        memoryRecoveryRecordPtr = null;

        IgniteFuture snapFut = null;

        CheckpointPagesInfoHolder cpPagesHolder;

        int dirtyPagesCount;

        boolean hasPartitionsToDestroy;

        WALPointer cpPtr = null;

        CheckpointContextImpl ctx0 = new CheckpointContextImpl(
            curr, new PartitionAllocationMap(), checkpointCollectPagesInfoPool, workProgressDispatcher
        );

        checkpointReadWriteLock.readLock();

        try {
            for (CheckpointListener lsnr : dbLsnrs)
                lsnr.beforeCheckpointBegin(ctx0);

            ctx0.awaitPendingTasksFinished();
        }
        finally {
            checkpointReadWriteLock.readUnlock();
        }

        tracker.onLockWaitStart();

        checkpointReadWriteLock.writeLock();

        try {
            curr.transitTo(LOCK_TAKEN);

            tracker.onMarkStart();

            // Listeners must be invoked before we write checkpoint record to WAL.
            for (CheckpointListener lsnr : dbLsnrs)
                lsnr.onMarkCheckpointBegin(ctx0);

            ctx0.awaitPendingTasksFinished();

            tracker.onListenersExecuteEnd();

            if (curr.nextSnapshot())
                snapFut = snapshotMgr.onMarkCheckPointBegin(curr.snapshotOperation(), ctx0.partitionStatMap());

            fillCacheGroupState(cpRec);

            //There are allowable to replace pages only after checkpoint entry was stored to disk.
            cpPagesHolder = beginAllCheckpoints(checkpointedRegions, curr.futureFor(MARKER_STORED_TO_DISK));

            curr.currentCheckpointPagesCount(cpPagesHolder.pagesNum());

            dirtyPagesCount = cpPagesHolder.pagesNum();

            hasPartitionsToDestroy = !curr.getDestroyQueue().pendingReqs().isEmpty();

            if (dirtyPagesCount > 0 || curr.nextSnapshot() || hasPartitionsToDestroy) {
                // No page updates for this checkpoint are allowed from now on.
                if (wal != null)
                    cpPtr = wal.log(cpRec);

                if (cpPtr == null)
                    cpPtr = CheckpointStatus.NULL_PTR;
            }

            curr.transitTo(PAGE_SNAPSHOT_TAKEN);
        }
        finally {
            checkpointReadWriteLock.writeUnlock();

            tracker.onLockRelease();
        }

        curr.transitTo(LOCK_RELEASED);

        for (CheckpointListener lsnr : dbLsnrs)
            lsnr.onCheckpointBegin(ctx0);

        if (snapFut != null) {
            try {
                snapFut.get();
            }
            catch (IgniteException e) {
                U.error(log, "Failed to wait for snapshot operation initialization: " +
                    curr.snapshotOperation(), e);
            }
        }

        if (dirtyPagesCount > 0 || hasPartitionsToDestroy) {
            tracker.onWalCpRecordFsyncStart();

            // Sync log outside the checkpoint write lock.
            if (wal != null)
                wal.flush(cpPtr, true);

            tracker.onWalCpRecordFsyncEnd();

            CheckpointEntry checkpointEntry = null;

            if (checkpointMarkersStorage != null)
                checkpointEntry = checkpointMarkersStorage.writeCheckpointEntry(
                    cpTs,
                    cpRec.checkpointId(),
                    cpPtr,
                    cpRec,
                    CheckpointEntryType.START,
                    skipSync
                );

            curr.transitTo(MARKER_STORED_TO_DISK);

            tracker.onSplitAndSortCpPagesStart();

            GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> cpPages =
                splitAndSortCpPagesIfNeeded(cpPagesHolder);

            tracker.onSplitAndSortCpPagesEnd();

            return new Checkpoint(checkpointEntry, cpPages, curr);
        }
        else {
            if (curr.nextSnapshot() && wal != null)
                wal.flush(null, true);

            return new Checkpoint(null, GridConcurrentMultiPairQueue.EMPTY, curr);
        }
    }

    /**
     * Fill cache group state in checkpoint record.
     *
     * @param cpRec Checkpoint record for filling.
     * @throws IgniteCheckedException if fail.
     */
    private void fillCacheGroupState(CheckpointRecord cpRec) throws IgniteCheckedException {
        GridCompoundFuture grpHandleFut = checkpointCollectPagesInfoPool == null ? null : new GridCompoundFuture();

        for (CacheGroupContext grp : cacheGroupsContexts.get()) {
            if (grp.isLocal() || !grp.walEnabled())
                continue;

            Runnable r = () -> {
                ArrayList<GridDhtLocalPartition> parts = new ArrayList<>(grp.topology().localPartitions().size());

                for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
                    parts.add(part);

                CacheState state = new CacheState(parts.size());

                for (GridDhtLocalPartition part : parts) {
                    GridDhtPartitionState partState = part.state();

                    if (partState == LOST)
                        partState = OWNING;

                    state.addPartitionState(
                        part.id(),
                        part.dataStore().fullSize(),
                        part.updateCounter(),
                        (byte)partState.ordinal()
                    );
                }

                synchronized (cpRec) {
                    cpRec.addCacheGroupState(grp.groupId(), state);
                }
            };

            if (checkpointCollectPagesInfoPool == null)
                r.run();
            else
                try {
                    GridFutureAdapter<?> res = new GridFutureAdapter<>();

                    checkpointCollectPagesInfoPool.execute(U.wrapIgniteFuture(r, res));

                    grpHandleFut.add(res);
                }
                catch (RejectedExecutionException e) {
                    assert false : "Task should never be rejected by async runner";

                    throw new IgniteException(e); //to protect from disabled asserts and call to failure handler
                }
        }

        if (grpHandleFut != null) {
            grpHandleFut.markInitialized();

            grpHandleFut.get();
        }
    }

    /**
     * @param allowToReplace The sign which allows to replace pages from a checkpoint by page replacer.
     * @return holder of FullPageIds obtained from each PageMemory, overall number of dirty pages, and flag defines at
     * least one user page became a dirty since last checkpoint.
     */
    private CheckpointPagesInfoHolder beginAllCheckpoints(Collection<DataRegion> regions,
        IgniteInternalFuture<?> allowToReplace) {
        Collection<Map.Entry<PageMemoryEx, GridMultiCollectionWrapper<FullPageId>>> res =
            new ArrayList<>(regions.size());

        int pagesNum = 0;

        for (DataRegion reg : regions) {
            if (!reg.config().isPersistenceEnabled())
                continue;

            GridMultiCollectionWrapper<FullPageId> nextCpPages = ((PageMemoryEx)reg.pageMemory())
                .beginCheckpoint(allowToReplace);

            pagesNum += nextCpPages.size();

            res.add(new T2<>((PageMemoryEx)reg.pageMemory(), nextCpPages));
        }

        return new CheckpointPagesInfoHolder(res, pagesNum);
    }

    /**
     * Reorders list of checkpoint pages and splits them into appropriate number of sublists according to {@link
     * DataStorageConfiguration#getCheckpointThreads()} and {@link DataStorageConfiguration#getCheckpointWriteOrder()}.
     *
     * @param cpPages Checkpoint pages with overall count and user pages info.
     */
    private GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> splitAndSortCpPagesIfNeeded(
        CheckpointPagesInfoHolder cpPages
    ) throws IgniteCheckedException {
        Set<T2<PageMemoryEx, FullPageId[]>> cpPagesPerRegion = new HashSet<>();

        int realPagesArrSize = 0;

        int totalPagesCnt = cpPages.pagesNum();

        for (Map.Entry<PageMemoryEx, GridMultiCollectionWrapper<FullPageId>> regPages : cpPages.cpPages()) {
            FullPageId[] pages = new FullPageId[regPages.getValue().size()];

            int pagePos = 0;

            for (int i = 0; i < regPages.getValue().collectionsSize(); i++) {
                for (FullPageId page : regPages.getValue().innerCollection(i)) {
                    if (realPagesArrSize++ == totalPagesCnt)
                        throw new AssertionError("Incorrect estimated dirty pages number: " + totalPagesCnt);

                    pages[pagePos++] = page;
                }
            }

            // Some pages may have been already replaced.
            if (pagePos != pages.length)
                cpPagesPerRegion.add(new T2<>(regPages.getKey(), Arrays.copyOf(pages, pagePos)));
            else
                cpPagesPerRegion.add(new T2<>(regPages.getKey(), pages));
        }

        if (checkpointWriteOrder == CheckpointWriteOrder.SEQUENTIAL) {
            Comparator<FullPageId> cmp = Comparator.comparingInt(FullPageId::groupId)
                .thenComparingLong(FullPageId::effectivePageId);

            ForkJoinPool pool = null;

            for (T2<PageMemoryEx, FullPageId[]> pagesPerReg : cpPagesPerRegion) {
                if (pagesPerReg.getValue().length >= parallelSortThreshold)
                    pool = parallelSortInIsolatedPool(pagesPerReg.get2(), cmp, pool);
                else
                    Arrays.sort(pagesPerReg.get2(), cmp);
            }

            if (pool != null)
                pool.shutdown();
        }

        return new GridConcurrentMultiPairQueue<>(cpPagesPerRegion);
    }

    /**
     * Performs parallel sort in isolated fork join pool.
     *
     * @param pagesArr Pages array.
     * @param cmp Cmp.
     * @return ForkJoinPool instance, check {@link ForkJoinTask#fork()} realization.
     */
    private static ForkJoinPool parallelSortInIsolatedPool(
        FullPageId[] pagesArr,
        Comparator<FullPageId> cmp,
        ForkJoinPool pool
    ) throws IgniteCheckedException {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory = new ForkJoinPool.ForkJoinWorkerThreadFactory() {
            @Override public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
                ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);

                worker.setName("checkpoint-pages-sorter-" + worker.getPoolIndex());

                return worker;
            }
        };

        ForkJoinPool execPool = pool == null ?
            new ForkJoinPool(PARALLEL_SORT_THREADS + 1, factory, null, false) : pool;

        Future<?> sortTask = execPool.submit(() -> Arrays.parallelSort(pagesArr, cmp));

        try {
            sortTask.get();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedCheckedException(e);
        }
        catch (ExecutionException e) {
            throw new IgniteCheckedException("Failed to perform pages array parallel sort", e.getCause());
        }

        return execPool;
    }

    /**
     * Do some actions on checkpoint finish(After all pages were written to disk).
     *
     * @param chp Checkpoint snapshot.
     */
    public void markCheckpointEnd(Checkpoint chp) throws IgniteCheckedException {
        synchronized (this) {
            chp.progress.clearCounters();

            for (DataRegion memPlc : dataRegions.get()) {
                if (!memPlc.config().isPersistenceEnabled())
                    continue;

                ((PageMemoryEx)memPlc.pageMemory()).finishCheckpoint();
            }
        }

        if (chp.hasDelta()) {
            if (checkpointMarkersStorage != null)
                checkpointMarkersStorage.writeCheckpointEntry(
                    chp.cpEntry.timestamp(),
                    chp.cpEntry.checkpointId(),
                    chp.cpEntry.checkpointMark(),
                    null,
                    CheckpointEntryType.END,
                    skipSync
                );

            if (wal != null)
                wal.notchLastCheckpointPtr(chp.cpEntry.checkpointMark());
        }

        if (checkpointMarkersStorage != null)
            checkpointMarkersStorage.onCheckpointFinished(chp);

        CheckpointContextImpl emptyCtx = new CheckpointContextImpl(chp.progress, null, null, null);

        Collection<DataRegion> checkpointedRegions = dataRegions.get();

        List<CheckpointListener> dbLsnrs = getRelevantCheckpointListeners(checkpointedRegions);

        for (CheckpointListener lsnr : dbLsnrs)
            lsnr.afterCheckpointEnd(emptyCtx);

        chp.progress.transitTo(FINISHED);
    }

    /**
     * @param checkpointedRegions Regions which will be checkpointed.
     * @return Checkpoint listeners which should be handled.
     */
    public List<CheckpointListener> getRelevantCheckpointListeners(Collection<DataRegion> checkpointedRegions) {
        return lsnrs.entrySet().stream()
            .filter(entry -> entry.getValue() == NO_REGION || checkpointedRegions.contains(entry.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }

    /**
     * This method makes sense if node was stopped during the checkpoint(Start marker was written to disk while end
     * marker are not). It is able to write all pages to disk and create end marker.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void finalizeCheckpointOnRecovery(
        long cpTs,
        UUID cpId,
        WALPointer walPtr,
        StripedExecutor exec,
        CheckpointPagesWriterFactory checkpointPagesWriterFactory
    ) throws IgniteCheckedException {
        assert cpTs != 0;

        long start = System.currentTimeMillis();

        Collection<DataRegion> regions = dataRegions.get();

        CheckpointPagesInfoHolder cpPagesHolder = beginAllCheckpoints(regions, new GridFinishedFuture<>());

        // Sort and split all dirty pages set to several stripes.
        GridConcurrentMultiPairQueue<PageMemoryEx, FullPageId> pages =
            splitAndSortCpPagesIfNeeded(cpPagesHolder);

        // Identity stores set for future fsync.
        Collection<PageStore> updStores = new GridConcurrentHashSet<>();

        AtomicInteger cpPagesCnt = new AtomicInteger();

        // Shared refernce for tracking exception during write pages.
        AtomicReference<Throwable> writePagesError = new AtomicReference<>();

        for (int stripeIdx = 0; stripeIdx < exec.stripesCount(); stripeIdx++)
            exec.execute(
                stripeIdx,
                checkpointPagesWriterFactory.buildRecovery(pages, updStores, writePagesError, cpPagesCnt)
            );

        // Await completion all write tasks.
        awaitApplyComplete(exec, writePagesError);

        long written = U.currentTimeMillis();

        // Fsync all touched stores.
        for (PageStore updStore : updStores)
            updStore.sync();

        long fsync = U.currentTimeMillis();

        for (DataRegion memPlc : regions) {
            if (memPlc.config().isPersistenceEnabled())
                ((PageMemoryEx)memPlc.pageMemory()).finishCheckpoint();
        }

        checkpointMarkersStorage.writeCheckpointEntry(cpTs, cpId, walPtr, null, CheckpointEntryType.END, skipSync);

        if (log.isInfoEnabled())
            log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                    "pagesWrite=%dms, fsync=%dms, total=%dms]",
                cpId,
                cpPagesCnt.get(),
                walPtr,
                written - start,
                fsync - written,
                fsync - start));
    }

    /**
     * @param exec Striped executor.
     * @param applyError Check error reference.
     */
    private void awaitApplyComplete(
        StripedExecutor exec,
        AtomicReference<Throwable> applyError
    ) throws IgniteCheckedException {
        try {
            // Await completion apply tasks in all stripes.
            exec.awaitComplete();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }

        // Checking error after all task applied.
        Throwable error = applyError.get();

        if (error != null)
            throw error instanceof IgniteCheckedException
                ? (IgniteCheckedException)error : new IgniteCheckedException(error);
    }

    /**
     * @param memoryRecoveryRecordPtr Memory recovery record pointer.
     */
    public void memoryRecoveryRecordPtr(WALPointer memoryRecoveryRecordPtr) {
        this.memoryRecoveryRecordPtr = memoryRecoveryRecordPtr;
    }

    /**
     * Adding the listener which will be called only when given data region will be checkpointed.
     *
     * @param lsnr Listener.
     * @param dataRegion Data region for which listener is corresponded to.
     */
    public void addCheckpointListener(CheckpointListener lsnr, DataRegion dataRegion) {
        lsnrs.put(lsnr, dataRegion == null ? NO_REGION : dataRegion);
    }

    /**
     * @param lsnr Listener.
     */
    public void removeCheckpointListener(CheckpointListener lsnr) {
        lsnrs.remove(lsnr);
    }

    /**
     * Stop any activity.
     */
    @SuppressWarnings("unused")
    public void stop() {
        IgniteThreadPoolExecutor pool = checkpointCollectPagesInfoPool;

        if (pool != null) {
            pool.shutdownNow();

            try {
                pool.awaitTermination(2, TimeUnit.MINUTES);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }

            checkpointCollectPagesInfoPool = null;
        }

        for (CheckpointListener lsnr : lsnrs.keySet())
            lsnrs.remove(lsnr);
    }

    /**
     * Prepare all structure to further work. This object should be fully ready to work after call of this method.
     */
    public void start() {
        if (checkpointCollectPagesInfoPool == null)
            checkpointCollectPagesInfoPool = initializeCheckpointPool();
    }
}
