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

package org.apache.ignite.internal.processors.cache;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.RowStore;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * <p>
 *     This is the CacheDataStoreProxy implementation. The main purpose is hot switching between different
 *     modes of cache data storage (e.g. between <tt>FULL</tt> and <tt>LOG_ONLY</tt> mode) to guarantee the
 *     consistency for Checkpointer writes and async cache put operations.
 * </p>
 * <p>
 *     For instance, switching from <tt>FULL</tt> to <tt>LOG_ONLY</tt> mode:
 *     <ul>
 *         <li>there is no threads are holding the readLocks for <tt>LOG_ONLY</tt> mode;</li>
 *         <li>set current mode to the <tt>LOG_ONLY</tt>;</li>
 *         <li>each thread will update its local mode to the new one on the next storage usage;</li>
 *         <li>each thread which is currently using the storage holds the read lock;</li>
 *         <li>if thread finishes his action (put, invoke etc.) it releases the read lock;</li>
 *         <li>when there is no threads in the <tt>FULL</tt> mode left the switching is done;</li>
 *     </ul>
 * </p>
 * <p>
 *     <tt>Case 0:</tt> All async operations for the storage in LOG_ONLY mode must not touch pages in the durable
 *     PageMemory.
 * </p>
 * <p>
 *     <tt>Case 1:</tt> While the storage in transition state (threads are accessing both old and new modes) it must
 *     have all counters (e.g. partition counter, storage size, cache sizes counter) to be synchronized.
 * </p>
 * <p>
 *     <tt>Case 2:</tt> The switching storage mode and setting a new storage for the same mode simultaneously
 *     must be forbidden.
 * </p>
 * <p>
 *     <tt>Case 3:</tt> Thread switch its working storage mode on first access to it (if previously have no locks
 *     on it).
 * </p>
 *
 */
public class CacheDataStoreProxyImpl implements CacheDataStoreProxy {
    /** Shows that the usage counter cannot be used anymore. */
    private static final long USAGE_DENIED = -1;

    /** */
    private final IgniteLogger log;

    /** */
    private final CacheGroupContext grp;

    /** The map of all storages per each mode. */
    private final ConcurrentMap<StorageMode, IgniteCacheOffheapManager.CacheDataStore> storageMap =
        new ConcurrentHashMap<>(StorageMode.values().length);

    /** The map of lock per each mode. The particular lock will prevent changing status to already used storage. */
    private final EnumMap<StorageMode, ReentrantReadWriteLock> modeLockMap = new EnumMap<>(StorageMode.class);

    /** Currently used data storage state. <tt>FULL</tt> mode is used by default. */
    private volatile StorageState currState;

    /** The state for each thread which is locally using by itself. */
    private final ThreadLocal<StorageState> threadState = ThreadLocal.withInitial(() -> currState);

    /**
     * @param primary The main storage to perform full cache operations.
     * @param secondary The storage to handle only write operation in temporary mode.
     */
    public CacheDataStoreProxyImpl(
        CacheGroupContext grp,
        IgniteCacheOffheapManager.CacheDataStore primary,
        IgniteCacheOffheapManager.CacheDataStore secondary,
        IgniteLogger log
    ) {
        assert primary != null;
        assert secondary != null;

        this.grp = grp;
        this.log = log;

        storageMap.put(StorageMode.FULL, primary);
        storageMap.put(StorageMode.LOG_ONLY, secondary);

        for (StorageMode mode : StorageMode.values())
            modeLockMap.put(mode, new ReentrantReadWriteLock());

        currState = new StorageState(StorageMode.FULL,
            new TransitionStateFuture(StorageMode.FULL,
                StorageMode.FULL,
                new AtomicLong(),
                new ReentrantReadWriteLock()));

        // Immediately complete the initial future.
        currState.transitionFut.onDone(true);
    }

    /** {@inheritDoc} */
    @Override public boolean storage(StorageMode mode, IgniteCacheOffheapManager.CacheDataStore storage) {
        StorageState currState0 = currState;

        // The instance of currently active storage cannot be changed.
        if (mode == currState0.mode)
            return false;

        ReentrantReadWriteLock lock = modeLockMap.get(mode);

        lock.writeLock().lock();

        try {
            switch (mode) {
                case FULL:
                    if (storageMap.get(StorageMode.LOG_ONLY) != null && !storageMap.get(StorageMode.LOG_ONLY).isEmpty())
                        return false;

                    break;

                case LOG_ONLY:
                    break;

                default:
                    throw new IgniteException("Cache data storage cannot be set. The mode is unknown: " + mode);
            }

            if (activeStorage().updateCounter() != storage.updateCounter())
                return false;

            storageMap.put(mode, storage);

            U.log(log, "The new instance of storage have been successfully set [mode=" + mode +
                ", storage=" + storage + ']');

            return true;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> storageMode(StorageMode mode) {
        StorageState oldState = currState;

        if (mode == oldState.mode)
            return oldState.transitionFut;

        ReentrantReadWriteLock lock = modeLockMap.get(mode);

        lock.writeLock().lock();

        try {
            AtomicLong oldCnt = oldState.usageCnt;

            U.log(log, "The storage usages [mode=" + oldState.mode + ", cnt=" + oldCnt.get() +
                ", locks=" + modeLockMap.get(oldState.mode).getReadLockCount() + ']');

            currState = new StorageState(mode,
                new TransitionStateFuture(mode,
                    oldState.mode,
                    oldCnt,
                    modeLockMap.get(oldState.mode)));

            // The state have swithed. The old counter from now can be only decremented.
            // Can safely check and perform the future completion.
            if (oldCnt.get() == 0 && oldCnt.compareAndSet(0, USAGE_DENIED))
                currState.transitionFut.tryDone(oldState.mode);

            if (log.isDebugEnabled())
                log.debug("The partition cache data storage is sheduled to switch to the new mode: " + mode);

            return currState.transitionFut;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public StorageMode storageMode() {
        return currState.mode;
    }

    /** {@inheritDoc} */
    @Override public long usages(){
        return currState.usageCnt.get();
    }

    /** {@inheritDoc} */
    @Override public void activeStorageReadLock() {
        StorageState currState0 = currState;
        StorageState locState = threadState.get();

        ReentrantReadWriteLock lock = modeLockMap.get(locState.mode);

        if (lock.getReadHoldCount() == 0) {
            if (!locState.equals(currState0)) {
                threadState.set(currState0);

                locState = currState0;

                lock = modeLockMap.get(currState0.mode);
            }

            lock.readLock().lock();

            locState.usageCnt.updateAndGet(prev -> {
                assert prev > USAGE_DENIED : "The counter is marked as finished by transition future";

                return prev + 1;
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void activeStorageReadUnlock() {
        StorageState locState = threadState.get();

        ReentrantReadWriteLock lock = modeLockMap.get(locState.mode);

        lock.readLock().unlock();

        long next = locState.usageCnt.updateAndGet(prev -> {
            assert (prev - 1) > USAGE_DENIED : "The counter is marked as finished by transition future";

            return prev - 1;
        });

        if (next == 0)
            currState.transitionFut.tryDone(locState.mode);
    }

    /**
     * @return The currently active cache data storage.
     */
    private IgniteCacheOffheapManager.CacheDataStore activeStorageWithLock() {
        activeStorageReadLock();

        return activeStorage();
    }

    /**
     * @return The currently active cache data storage.
     */
    private IgniteCacheOffheapManager.CacheDataStore activeStorage() {
        return storageMap.get(threadState.get().mode);
    }

    /** {@inheritDoc} */
    @Override public int partId() {
        return activeStorage().partId();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return activeStorage().name();
    }

    /** {@inheritDoc} */
    @Override public void init(long size, long updCntr, @Nullable Map<Integer, Long> cacheSizes) {
        throw new UnsupportedOperationException("The init method of proxy storage must never be called.");
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow createRow(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException {
        return activeStorageWithLock().createRow(cctx, key, val, ver, expireTime, oldRow);
    }

    /** {@inheritDoc} */
    @Override public int cleanup(
        GridCacheContext cctx,
        @Nullable List<MvccLinkAwareSearchRow> cleanupRows
    ) throws IgniteCheckedException {
        return activeStorageWithLock().cleanup(cctx, cleanupRows);
    }

    /** {@inheritDoc} */
    @Override public void updateTxState(GridCacheContext cctx, CacheSearchRow row) throws IgniteCheckedException {
        activeStorageWithLock().updateTxState(cctx, row);
    }

    /** {@inheritDoc} */
    @Override public void update(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        @Nullable CacheDataRow oldRow
    ) throws IgniteCheckedException {
        activeStorageWithLock().update(cctx, key, val, ver, expireTime, oldRow);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccInitialValue(
        GridCacheContext cctx,
        KeyCacheObject key,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer
    ) throws IgniteCheckedException {
        return activeStorageWithLock().mvccInitialValue(cctx, key, val, ver, expireTime, mvccVer, newMvccVer);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccApplyHistoryIfAbsent(
        GridCacheContext cctx,
        KeyCacheObject key,
        List<GridCacheMvccEntryInfo> hist
    ) throws IgniteCheckedException {
        return activeStorageWithLock().mvccApplyHistoryIfAbsent(cctx, key, hist);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccUpdateRowWithPreloadInfo(
        GridCacheContext cctx,
        KeyCacheObject key,
        @Nullable CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer,
        MvccVersion newMvccVer,
        byte mvccTxState,
        byte newMvccTxState
    ) throws IgniteCheckedException {
        return activeStorageWithLock().mvccUpdateRowWithPreloadInfo(cctx, key, val, ver, expireTime, mvccVer, newMvccVer, mvccTxState,
            newMvccTxState);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccUpdate(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccSnapshot mvccSnapshot,
        @Nullable CacheEntryPredicate filter,
        EntryProcessor entryProc,
        Object[] invokeArgs,
        boolean primary,
        boolean needHist,
        boolean noCreate,
        boolean needOldVal,
        boolean retVal,
        boolean keepBinary
    ) throws IgniteCheckedException {
        return activeStorageWithLock().mvccUpdate(cctx, key, val, ver, expireTime, mvccSnapshot, filter, entryProc, invokeArgs, primary,
            needHist, noCreate, needOldVal, retVal, keepBinary);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccRemove(
        GridCacheContext cctx,
        KeyCacheObject key,
        MvccSnapshot mvccSnapshot,
        @Nullable CacheEntryPredicate filter,
        boolean primary,
        boolean needHistory,
        boolean needOldVal,
        boolean retVal
    ) throws IgniteCheckedException {
        return activeStorageWithLock().mvccRemove(cctx, key, mvccSnapshot, filter, primary, needHistory, needOldVal, retVal);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccLock(
        GridCacheContext cctx,
        KeyCacheObject key,
        MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException {
        return activeStorageWithLock().mvccLock(cctx, key, mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        activeStorageWithLock().mvccRemoveAll(cctx, key);
    }

    /** {@inheritDoc} */
    @Override public void invoke(
        GridCacheContext cctx,
        KeyCacheObject key,
        IgniteCacheOffheapManager.OffheapInvokeClosure c
    ) throws IgniteCheckedException {
        activeStorageWithLock().invoke(cctx, key, c);
    }

    /** {@inheritDoc} */
    @Override public void mvccApplyUpdate(
        GridCacheContext cctx,
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccVersion mvccVer
    ) throws IgniteCheckedException {
        activeStorageWithLock().mvccApplyUpdate(cctx, key, val, ver, expireTime, mvccVer);
    }

    /** {@inheritDoc} */
    @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId) throws IgniteCheckedException {
        activeStorageWithLock().remove(cctx, key, partId);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        return activeStorageWithLock().find(cctx, key);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(
        GridCacheContext cctx,
        KeyCacheObject key,
        Object x
    ) throws IgniteCheckedException {
        return activeStorageWithLock().mvccAllVersionsCursor(cctx, key, x);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow mvccFind(
        GridCacheContext cctx,
        KeyCacheObject key,
        MvccSnapshot snapshot
    ) throws IgniteCheckedException {
        return activeStorageWithLock().mvccFind(cctx, key, snapshot);
    }

    /** {@inheritDoc} */
    @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(
        GridCacheContext cctx,
        KeyCacheObject key
    ) throws IgniteCheckedException {
        return activeStorageWithLock().mvccFindAllVersions(cctx, key);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
        return activeStorageWithLock().cursor();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(Object x) throws IgniteCheckedException {
        return activeStorageWithLock().cursor(x);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException {
        return activeStorageWithLock().cursor(mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
        return activeStorageWithLock().cursor(cacheId);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        int cacheId,
        MvccSnapshot mvccSnapshot
    ) throws IgniteCheckedException {
        return activeStorageWithLock().cursor(cacheId, mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        int cacheId,
        KeyCacheObject lower,
        KeyCacheObject upper
    ) throws IgniteCheckedException {
        return activeStorageWithLock().cursor(cacheId, lower, upper);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        int cacheId,
        KeyCacheObject lower,
        KeyCacheObject upper,
        Object x
    ) throws IgniteCheckedException {
        return activeStorageWithLock().cursor(cacheId, lower, upper, x);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        int cacheId,
        KeyCacheObject lower,
        KeyCacheObject upper,
        Object x,
        MvccSnapshot snapshot
    ) throws IgniteCheckedException {
        return activeStorageWithLock().cursor(cacheId, lower, upper, x, snapshot);
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws IgniteCheckedException {
        activeStorageWithLock().destroy();
    }

    /** {@inheritDoc} */
    @Override public void clear(int cacheId) throws IgniteCheckedException {
        activeStorageWithLock().clear(cacheId);
    }

    /** {@inheritDoc} */
    @Override public RowStore rowStore() {
        // Checkpointer must always have assess to the original storage.
        return activeStorageWithLock().rowStore();
    }

    /** {@inheritDoc} */
    @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
        activeStorage().setRowCacheCleaner(rowCacheCleaner);
    }

    /** {@inheritDoc} */
    @Override public PendingEntriesTree pendingTree() {
        return activeStorageWithLock().pendingTree();
    }

    /** {@inheritDoc} */
    @Override public void preload() throws IgniteCheckedException {
        activeStorageWithLock().preload();
    }

    /** {@inheritDoc} */
    @Override public long cacheSize(int cacheId) {
        return activeStorage().cacheSize(cacheId);
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> cacheSizes() {
        return activeStorage().cacheSizes();
    }

    /** {@inheritDoc} */
    @Override public long fullSize() {
        return activeStorage().fullSize();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return activeStorage().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public void updateSize(int cacheId, long delta) {
        activeStorage().updateSize(cacheId, delta);
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return activeStorage().updateCounter();
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long val) {
        activeStorage().updateCounter(val);
    }

    /** {@inheritDoc} */
    @Override public void updateCounter(long start, long delta) {
        activeStorage().updateCounter(start, delta);
    }

    /** {@inheritDoc} */
    @Override public long nextUpdateCounter() {
        return activeStorage().nextUpdateCounter();
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrementUpdateCounter(long delta) {
        return activeStorage().getAndIncrementUpdateCounter(delta);
    }

    /** {@inheritDoc} */
    @Override public long initialUpdateCounter() {
        return activeStorage().initialUpdateCounter();
    }

    /** {@inheritDoc} */
    @Override public void updateInitialCounter(long cntr) {
        activeStorage().updateInitialCounter(cntr);
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return activeStorage().finalizeUpdateCounters();
    }

    /**
     *
     */
    private static class StorageState {
        /** */
        private final StorageMode mode;

        /** */
        private final AtomicLong usageCnt;

        /** */
        private final TransitionStateFuture transitionFut;

        /**
         * @param nextMode The mode of storage to be used.
         * @param fut The future will complete when the transition to the aproppriate mode finished.
         */
        public StorageState(StorageMode nextMode, TransitionStateFuture fut) {
            mode = nextMode;
            usageCnt = new AtomicLong();
            transitionFut = fut;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            StorageState state = (StorageState)o;

            return mode == state.mode;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(mode);
        }
    }

    /**
     *
     */
    private static class TransitionStateFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final StorageMode nextMode;

        /** */
        private final StorageMode prevMode;

        /** */
        private final AtomicLong prevStateCnt;

        /** */
        private final ReentrantReadWriteLock prevStateLock;

        /**
         * @param nextMode The mode to switch storage to.
         * @param prevMode The mode of storage to switch from.
         * @param cnt The counter used by previous nextMode.
         * @param lock The lock used by previous nextMode.
         */
        public TransitionStateFuture(
            StorageMode nextMode,
            StorageMode prevMode,
            AtomicLong cnt,
            ReentrantReadWriteLock lock
        ) {
            this.nextMode = nextMode;
            this.prevMode = prevMode;
            prevStateCnt = cnt;
            prevStateLock = lock;
        }

        /**
         * @param mode The mode to work with.
         */
        public void tryDone(StorageMode mode) {
            if (isDone() || mode == nextMode)
                return;

            assert mode == prevMode;
            assert prevStateCnt.get() == 0 || prevStateCnt.get() == USAGE_DENIED;
            assert prevStateLock.getReadLockCount() == 0;

            onDone(true);
        }
    }
}
