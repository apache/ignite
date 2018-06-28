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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionMetaStateRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMapImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.util.deque.FastSizeDeque;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED;
import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.RENTING;

/**
 * Key partition.
 */
public class GridDhtLocalPartition extends GridCacheConcurrentMapImpl implements Comparable<GridDhtLocalPartition>, GridReservable {
    /** */
    private static final GridCacheMapEntryFactory ENTRY_FACTORY = new GridCacheMapEntryFactory() {
        @Override public GridCacheMapEntry create(
            GridCacheContext ctx,
            AffinityTopologyVersion topVer,
            KeyCacheObject key
        ) {
            return new GridDhtCacheEntry(ctx, topVer, key);
        }
    };

    /** Maximum size for delete queue. */
    public static final int MAX_DELETE_QUEUE_SIZE = Integer.getInteger(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, 200_000);

    /** ONLY FOR TEST PURPOSES: force test checkpoint on partition eviction. */
    private static boolean forceTestCheckpointOnEviction = IgniteSystemProperties.getBoolean("TEST_CHECKPOINT_ON_EVICTION", false);

    /** ONLY FOR TEST PURPOSES: partition id where test checkpoint was enforced during eviction. */
    static volatile Integer partWhereTestCheckpointEnforced;

    /** Maximum size for {@link #rmvQueue}. */
    private final int rmvQueueMaxSize;

    /** Removed items TTL. */
    private final long rmvdEntryTtl;

    /** Static logger to avoid re-creation. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static volatile IgniteLogger log;

    /** Partition ID. */
    private final int id;

    /** State. 32 bits - size, 16 bits - reservations, 13 bits - reserved, 3 bits - GridDhtPartitionState. */
    @GridToStringExclude
    private final AtomicLong state = new AtomicLong((long)MOVING.ordinal() << 32);

    /** Evict guard. Must be CASed to -1 only when partition state is EVICTED. */
    @GridToStringExclude
    private final AtomicInteger evictGuard = new AtomicInteger();

    /** Rent future. */
    @GridToStringExclude
    private final GridFutureAdapter<?> rent;

    /** Clear future. */
    @GridToStringExclude
    private final ClearFuture clearFuture;

    /** */
    @GridToStringExclude
    private final GridCacheSharedContext ctx;

    /** */
    @GridToStringExclude
    private final CacheGroupContext grp;

    /** Create time. */
    @GridToStringExclude
    private final long createTime = U.currentTimeMillis();

    /** Lock. */
    @GridToStringExclude
    private final ReentrantLock lock = new ReentrantLock();

    /** */
    @GridToStringExclude
    private final ConcurrentMap<Integer, CacheMapHolder> cacheMaps;

    /** */
    @GridToStringExclude
    private final CacheMapHolder singleCacheEntryMap;

    /** Remove queue. */
    @GridToStringExclude
    private final FastSizeDeque<RemovedEntryHolder> rmvQueue = new FastSizeDeque<>(new ConcurrentLinkedDeque<>());

    /** Group reservations. */
    @GridToStringExclude
    private final CopyOnWriteArrayList<GridDhtPartitionsReservation> reservations = new CopyOnWriteArrayList<>();

    /** */
    @GridToStringExclude
    private volatile CacheDataStore store;

    /** Set if failed to move partition to RENTING state due to reservations, to be checked when
     * reservation is released. */
    private volatile boolean delayedRenting;

    /** Set if partition must be cleared in MOVING state. */
    private volatile boolean clear;

    /** Set if topology update sequence should be updated on partition destroy. */
    private boolean updateSeqOnDestroy;

    /**
     * @param ctx Context.
     * @param grp Cache group.
     * @param id Partition ID.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    GridDhtLocalPartition(GridCacheSharedContext ctx,
        CacheGroupContext grp,
        int id) {
        super(ENTRY_FACTORY);

        this.id = id;
        this.ctx = ctx;
        this.grp = grp;

        log = U.logger(ctx.kernalContext(), logRef, this);

        if (grp.sharedGroup()) {
            singleCacheEntryMap = null;
            cacheMaps = new ConcurrentHashMap<>();
        }
        else {
            singleCacheEntryMap = new CacheMapHolder(grp.singleCacheContext(), createEntriesMap());
            cacheMaps = null;
        }

        rent = new GridFutureAdapter<Object>() {
            @Override public String toString() {
                return "PartitionRentFuture [part=" + GridDhtLocalPartition.this + ']';
            }
        };

        clearFuture = new ClearFuture();

        int delQueueSize = grp.systemCache() ? 100 :
            Math.max(MAX_DELETE_QUEUE_SIZE / grp.affinity().partitions(), 20);

        rmvQueueMaxSize = U.ceilPow2(delQueueSize);

        rmvdEntryTtl = Long.getLong(IGNITE_CACHE_REMOVED_ENTRIES_TTL, 10_000);

        try {
            store = grp.offheap().createCacheDataStore(id);

            // Log partition creation for further crash recovery purposes.
            if (grp.walEnabled())
                ctx.wal().log(new PartitionMetaStateRecord(grp.groupId(), id, state(), updateCounter()));

            // Inject row cache cleaner on store creation
            // Used in case the cache with enabled SqlOnheapCache is single cache at the cache group
            if (ctx.kernalContext().query().moduleEnabled()) {
                GridQueryRowCacheCleaner cleaner = ctx.kernalContext().query().getIndexing()
                    .rowCacheCleaner(grp.groupId());

                if (store != null && cleaner != null)
                    store.setRowCacheCleaner(cleaner);
            }
        }
        catch (IgniteCheckedException e) {
            // TODO ignite-db
            throw new IgniteException(e);
        }
    }

    /**
     * @return Entries map.
     */
    private ConcurrentMap<KeyCacheObject, GridCacheMapEntry> createEntriesMap() {
        return new ConcurrentHashMap<>(Math.max(10, GridCacheAdapter.DFLT_START_CACHE_SIZE / grp.affinity().partitions()),
            0.75f,
            Runtime.getRuntime().availableProcessors() * 2);
    }

    /** {@inheritDoc} */
    @Override public int internalSize() {
        if (grp.sharedGroup()) {
            int size = 0;

            for (CacheMapHolder hld : cacheMaps.values())
                size += hld.map.size();

            return size;
        }

        return singleCacheEntryMap.map.size();
    }

    /** {@inheritDoc} */
    @Override protected CacheMapHolder entriesMap(GridCacheContext cctx) {
        if (grp.sharedGroup())
            return cacheMapHolder(cctx);

        return singleCacheEntryMap;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected CacheMapHolder entriesMapIfExists(Integer cacheId) {
        return grp.sharedGroup() ? cacheMaps.get(cacheId) : singleCacheEntryMap;
    }

    /**
     * @param cctx Cache context.
     * @return Map holder.
     */
    private CacheMapHolder cacheMapHolder(GridCacheContext cctx) {
        assert grp.sharedGroup();

        CacheMapHolder hld = cacheMaps.get(cctx.cacheIdBoxed());

        if (hld != null)
            return hld;

        CacheMapHolder  old = cacheMaps.putIfAbsent(cctx.cacheIdBoxed(), hld = new CacheMapHolder(cctx, createEntriesMap()));

        if (old != null)
            hld = old;

        return hld;
    }

    /**
     * @return Data store.
     */
    public CacheDataStore dataStore() {
        return store;
    }

    /**
     * Adds group reservation to this partition.
     *
     * @param r Reservation.
     * @return {@code false} If such reservation already added.
     */
    public boolean addReservation(GridDhtPartitionsReservation r) {
        assert (getPartState(state.get())) != EVICTED : "we can reserve only active partitions";
        assert (getReservations(state.get())) != 0 : "partition must be already reserved before adding group reservation";

        return reservations.addIfAbsent(r);
    }

    /**
     * @param r Reservation.
     */
    public void removeReservation(GridDhtPartitionsReservation r) {
        if (!reservations.remove(r))
            throw new IllegalStateException("Reservation was already removed.");
    }

    /**
     * @return Partition ID.
     */
    public int id() {
        return id;
    }

    /**
     * @return Create time.
     */
    long createTime() {
        return createTime;
    }

    /**
     * @return Partition state.
     */
    public GridDhtPartitionState state() {
        return getPartState(state.get());
    }

    /**
     * @return Reservations.
     */
    public int reservations() {
        return getReservations(state.get());
    }

    /**
     * @return {@code True} if partition is empty.
     */
    public boolean isEmpty() {
        return store.fullSize() == 0 && internalSize() == 0;
    }

    /**
     * @return If partition is moving or owning or renting.
     */
    public boolean valid() {
        GridDhtPartitionState state = state();

        return state == MOVING || state == OWNING || state == RENTING;
    }

    /**
     * @param entry Entry to remove.
     */
    void onRemoved(GridDhtCacheEntry entry) {
        assert entry.obsolete() : entry;

        // Make sure to remove exactly this entry.
        removeEntry(entry);

        // Attempt to evict.
        tryContinueClearing();
    }

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param ver Version.
     */
    private void removeVersionedEntry(int cacheId, KeyCacheObject key, GridCacheVersion ver) {
        CacheMapHolder hld = grp.sharedGroup() ? cacheMaps.get(cacheId) : singleCacheEntryMap;

        GridCacheMapEntry entry = hld != null ? hld.map.get(key) : null;

        if (entry != null && entry.markObsoleteVersion(ver))
            removeEntry(entry);
    }

    /**
     *
     */
    public void cleanupRemoveQueue() {
        while (rmvQueue.sizex() >= rmvQueueMaxSize) {
            RemovedEntryHolder item = rmvQueue.pollFirst();

            if (item != null)
                removeVersionedEntry(item.cacheId(), item.key(), item.version());
        }

        if (!grp.isDrEnabled()) {
            RemovedEntryHolder item = rmvQueue.peekFirst();

            while (item != null && item.expireTime() < U.currentTimeMillis()) {
                item = rmvQueue.pollFirst();

                if (item == null)
                    break;

                removeVersionedEntry(item.cacheId(), item.key(), item.version());

                item = rmvQueue.peekFirst();
            }
        }
    }

    /**
     * @param cacheId cacheId Cache ID.
     * @param key Removed key.
     * @param ver Removed version.
     */
    public void onDeferredDelete(int cacheId, KeyCacheObject key, GridCacheVersion ver) {
        cleanupRemoveQueue();

        rmvQueue.add(new RemovedEntryHolder(cacheId, key, ver, rmvdEntryTtl));
    }

    /**
     * Locks partition.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void lock() {
        lock.lock();
    }

    /**
     * Unlocks partition.
     */
    public void unlock() {
        lock.unlock();
    }

    /**
     * Reserves a partition so it won't be cleared or evicted.
     *
     * @return {@code True} if reserved.
     */
    @Override public boolean reserve() {
        while (true) {
            long state = this.state.get();

            if (getPartState(state) == EVICTED)
                return false;

            long newState = setReservations(state, getReservations(state) + 1);

            if (this.state.compareAndSet(state, newState))
                return true;
        }
    }

    /**
     * Releases previously reserved partition.
     */
    @Override public void release() {
        release0(0);
    }

    /** {@inheritDoc} */
    @Override protected void release(int sizeChange, CacheMapHolder hld, GridCacheEntryEx e) {
        if (grp.sharedGroup() && sizeChange != 0)
            hld.size.addAndGet(sizeChange);

        release0(sizeChange);
    }

    /**
     * @param sizeChange Size change delta.
     */
    private void release0(int sizeChange) {
        while (true) {
            long state = this.state.get();

            int reservations = getReservations(state);

            if (reservations == 0)
                return;

            assert getPartState(state) != EVICTED : getPartState(state);

            long newState = setReservations(state, --reservations);
            newState = setSize(newState, getSize(newState) + sizeChange);

            assert getSize(newState) == getSize(state) + sizeChange;

            // Decrement reservations.
            if (this.state.compareAndSet(state, newState)) {
                // If no more reservations try to continue delayed renting or clearing process.
                if (reservations == 0) {
                    if (delayedRenting)
                        rent(true);
                    else
                        tryContinueClearing();
                }

                break;
            }
        }
    }

    /**
     * @param stateToRestore State to restore.
     */
    public void restoreState(GridDhtPartitionState stateToRestore) {
        state.set(setPartState(state.get(),stateToRestore));
    }

    /**
     * @param state Current aggregated value.
     * @param toState State to switch to.
     * @return {@code true} if cas succeeds.
     */
    private boolean casState(long state, GridDhtPartitionState toState) {
        if (grp.persistenceEnabled() && grp.walEnabled()) {
            synchronized (this) {
                boolean update = this.state.compareAndSet(state, setPartState(state, toState));

                if (update)
                    try {
                        ctx.wal().log(new PartitionMetaStateRecord(grp.groupId(), id, toState, updateCounter()));
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Error while writing to log", e);
                    }

                return update;
            }
        }
        else
            return this.state.compareAndSet(state, setPartState(state, toState));
    }

    /**
     * @return {@code True} if transitioned to OWNING state.
     */
    boolean own() {
        while (true) {
            long state = this.state.get();

            GridDhtPartitionState partState = getPartState(state);

            if (partState == RENTING || partState == EVICTED)
                return false;

            if (partState == OWNING)
                return true;

            assert partState == MOVING || partState == LOST;

            if (casState(state, OWNING)) {
                if (log.isDebugEnabled())
                    log.debug("Owned partition: " + this);

                return true;
            }
        }
    }

    /**
     * Forcibly moves partition to a MOVING state.
     */
    public void moving() {
        while (true) {
            long state = this.state.get();

            GridDhtPartitionState partState = getPartState(state);

            assert partState == OWNING || partState == RENTING : "Only partitions in state OWNING or RENTING can be moved to MOVING state";

            if (casState(state, MOVING)) {
                if (log.isDebugEnabled())
                    log.debug("Forcibly moved partition to a MOVING state: " + this);

                break;
            }
        }
    }

    /**
     * @return {@code True} if partition state changed.
     */
    boolean markLost() {
        while (true) {
            long state = this.state.get();

            GridDhtPartitionState partState = getPartState(state);

            if (partState == LOST)
                return false;

            if (casState(state, LOST)) {
                if (log.isDebugEnabled())
                    log.debug("Marked partition as LOST: " + this);

                return true;
            }
        }
    }

    /**
     * Initiates partition eviction process.
     *
     * If partition has reservations, eviction will be delayed and continued after all reservations will be released.
     *
     * @param updateSeq If {@code true} topology update sequence will be updated after eviction is finished.
     * @return Future to signal that this node is no longer an owner or backup.
     */
    public IgniteInternalFuture<?> rent(boolean updateSeq) {
        long state0 = this.state.get();

        GridDhtPartitionState partState = getPartState(state0);

        if (partState == RENTING || partState == EVICTED)
            return rent;

        delayedRenting = true;

        if (getReservations(state0) == 0 && casState(state0, RENTING)) {
            delayedRenting = false;

            if (log.isDebugEnabled())
                log.debug("Moved partition to RENTING state: " + this);

            // Evict asynchronously, as the 'rent' method may be called
            // from within write locks on local partition.
            clearAsync0(updateSeq);
        }

        return rent;
    }

    /**
     * Starts clearing process asynchronously if it's requested and not running at the moment.
     * Method may finish clearing process ahead of time if partition is empty and doesn't have reservations.
     *
     * @param updateSeq Update sequence.
     */
    private void clearAsync0(boolean updateSeq) {
        long state = this.state.get();

        GridDhtPartitionState partState = getPartState(state);

        boolean evictionRequested = partState == RENTING || delayedRenting;
        boolean clearingRequested = partState == MOVING && clear;

        if (!evictionRequested && !clearingRequested)
            return;

        boolean reinitialized = clearFuture.initialize(updateSeq, evictionRequested);

        // Clearing process is already running at the moment. No needs to run it again.
        if (!reinitialized)
            return;

        // Try fast eviction
        if (isEmpty() && getSize(state) == 0 && !grp.queriesEnabled()
                && getReservations(state) == 0 && !groupReserved()) {

            if (partState == RENTING && casState(state, EVICTED) || clearingRequested) {
                clearFuture.finish();

                if (state() == EVICTED && markForDestroy()) {
                    updateSeqOnDestroy = updateSeq;

                    destroy();
                }

                return;
            }
        }

        grp.evictor().evictPartitionAsync(this);
    }

    /**
     * Initiates single clear process if partition is in MOVING state.
     * Method does nothing if clear process is already running.
     */
    public void clearAsync() {
        if (state() != MOVING)
            return;

        clear = true;
        clearAsync0(false);
    }

    /**
     * Continues delayed clearing of partition if possible.
     * Clearing may be delayed because of existing reservations.
     */
    void tryContinueClearing() {
        clearAsync0(true);
    }

    /**
     * @return {@code true} If there is a group reservation.
     */
    private boolean groupReserved() {
        for (GridDhtPartitionsReservation reservation : reservations) {
            if (!reservation.invalidate())
                return true; // Failed to invalidate reservation -> we are reserved.
        }

        return false;
    }

    /**
     * @return {@code true} if evicting thread was added.
     */
    private boolean addEvicting() {
        while (true) {
            int cnt = evictGuard.get();

            if (cnt != 0)
                return false;

            if (evictGuard.compareAndSet(cnt, cnt + 1)) {

                return true;
            }
        }
    }

    /**
     * @return {@code true} if no thread evicting partition at the moment.
     */
    private boolean clearEvicting() {
        boolean free;

        while (true) {
            int cnt = evictGuard.get();

            assert cnt > 0;

            if (evictGuard.compareAndSet(cnt, cnt - 1)) {
                free = cnt == 1;

                break;
            }
        }

        return free;
    }

    /**
     * @return {@code True} if partition is safe to destroy.
     */
    public boolean markForDestroy() {
        while (true) {
            int cnt = evictGuard.get();

            if (cnt != 0)
                return false;

            if (evictGuard.compareAndSet(0, -1))
                return true;
        }
    }

    /**
     * Moves partition state to {@code EVICTED} if possible.
     * and initiates partition destroy process after successful moving partition state to {@code EVICTED} state.
     *
     * @param updateSeq If {@code true} increment update sequence on cache group topology after successful eviction.
     */
    private void finishEviction(boolean updateSeq) {
        long state0 = this.state.get();

        GridDhtPartitionState state = getPartState(state0);

        if (state == EVICTED || (isEmpty() && getSize(state0) == 0 && getReservations(state0) == 0 && state == RENTING && casState(state0, EVICTED))) {
            if (log.isDebugEnabled())
                log.debug("Evicted partition: " + this);

            updateSeqOnDestroy = updateSeq;
        }
    }

    /**
     * Destroys partition data store and invokes appropriate callbacks.
     */
    public void destroy() {
        assert state() == EVICTED : this;
        assert evictGuard.get() == -1;

        grp.onPartitionEvicted(id);

        destroyCacheDataStore();

        rent.onDone();

        ((GridDhtPreloader)grp.preloader()).onPartitionEvicted(this, updateSeqOnDestroy);

        clearDeferredDeletes();
    }

    /**
     * Awaits completion of partition destroy process in case of {@code EVICTED} partition state.
     */
    public void awaitDestroy() {
        if (state() != EVICTED)
            return;

        final long timeout = 10_000;

        for (;;) {
            try {
                rent.get(timeout);

                break;
            }
            catch (IgniteFutureTimeoutCheckedException ignored) {
                U.warn(log, "Failed to await partition destroy within timeout " + this);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to await partition destroy " + this, e);
            }
        }
    }

    /**
     * Adds listener on {@link #clearFuture} finish.
     *
     * @param lsnr Listener.
     */
    public void onClearFinished(IgniteInClosure<? super IgniteInternalFuture<?>> lsnr) {
        clearFuture.listen(lsnr);
    }

    /**
     * @return {@code True} if clearing process is running at the moment on the partition.
     */
    public boolean isClearing() {
        return !clearFuture.isDone();
    }

    /**
     * Tries to start partition clear process {@link GridDhtLocalPartition#clearAll(EvictionContext)}).
     * Only one thread is allowed to do such process concurrently.
     * At the end of clearing method completes {@code clearFuture}.
     *
     * @return {@code false} if clearing is not started due to existing reservations.
     * @throws NodeStoppingException If node is stopping.
     */
    public boolean tryClear(EvictionContext evictionCtx) throws NodeStoppingException {
        if (clearFuture.isDone())
            return true;

        long state = this.state.get();

        if (getReservations(state) != 0 || groupReserved())
            return false;

        if (addEvicting()) {
            try {
                // Attempt to evict partition entries from cache.
                long clearedEntities = clearAll(evictionCtx);

                if (log.isDebugEnabled())
                    log.debug("Partition is cleared [clearedEntities=" + clearedEntities + ", part=" + this + "]");
            }
            catch (NodeStoppingException e) {
                clearFuture.finish(e);

                throw e;
            }
            finally {
                boolean free = clearEvicting();

                if (free)
                    clearFuture.finish();
            }
        }

        return true;
    }

    /**
     * Release created data store for this partition.
     */
    private void destroyCacheDataStore() {
        try {
            grp.offheap().destroyCacheDataStore(dataStore());
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to destroy cache data store on partition eviction [id=" + id + "]", e);
        }
    }

    /**
     * On partition unlock callback.
     * Tries to continue delayed partition clearing.
     */
    void onUnlock() {
        tryContinueClearing();
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if local node is primary for this partition.
     */
    public boolean primary(AffinityTopologyVersion topVer) {
        List<ClusterNode> nodes = grp.affinity().cachedAffinity(topVer).get(id);

        return !nodes.isEmpty() && ctx.localNode().equals(nodes.get(0));
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if local node is backup for this partition.
     */
    public boolean backup(AffinityTopologyVersion topVer) {
        List<ClusterNode> nodes = grp.affinity().cachedAffinity(topVer).get(id);

        return nodes.indexOf(ctx.localNode()) > 0;
    }

    /**
     * @param cacheId ID of cache initiated counter update.
     * @param topVer Topology version for current operation.
     * @return Next update index.
     */
    long nextUpdateCounter(int cacheId, AffinityTopologyVersion topVer, boolean primary, @Nullable Long primaryCntr) {
        long nextCntr = store.nextUpdateCounter();

        if (grp.sharedGroup())
            grp.onPartitionCounterUpdate(cacheId, id, primaryCntr != null ? primaryCntr : nextCntr, topVer, primary);

        return nextCntr;
    }

    /**
     * @return Current update index.
     */
    public long updateCounter() {
        return store.updateCounter();
    }

    /**
     * @return Initial update counter.
     */
    public long initialUpdateCounter() {
        return store.initialUpdateCounter();
    }

    /**
     * @param val Update index value.
     */
    public void updateCounter(long val) {
        store.updateCounter(val);
    }

    /**
     * @param val Initial update index value.
     */
    public void initialUpdateCounter(long val) {
        store.updateInitialCounter(val);
    }

    /**
     * @return Total size of all caches.
     */
    public long fullSize() {
        return store.fullSize();
    }

    /**
     * Removes all entries and rows from this partition.
     *
     * @return Number of rows cleared from page memory.
     * @throws NodeStoppingException If node stopping.
     */
    private long clearAll(EvictionContext evictionCtx) throws NodeStoppingException {
        GridCacheVersion clearVer = ctx.versions().next();

        GridCacheObsoleteEntryExtras extras = new GridCacheObsoleteEntryExtras(clearVer);

        boolean rec = grp.eventRecordable(EVT_CACHE_REBALANCE_OBJECT_UNLOADED);

        if (grp.sharedGroup()) {
            for (CacheMapHolder hld : cacheMaps.values())
                clear(hld.map, extras, rec);
        }
        else
            clear(singleCacheEntryMap.map, extras, rec);

        long cleared = 0;

        final int stopCheckingFreq = 1000;

        CacheMapHolder hld = grp.sharedGroup() ? null : singleCacheEntryMap;

        try {
            GridIterator<CacheDataRow> it0 = grp.offheap().partitionIterator(id);

            while (it0.hasNext()) {
                ctx.database().checkpointReadLock();

                try {
                    CacheDataRow row = it0.next();

                    // Do not clear fresh rows in case of single partition clearing.
                    if (row.version().compareTo(clearVer) >= 0 && (state() == MOVING && clear))
                        continue;

                    if (grp.sharedGroup() && (hld == null || hld.cctx.cacheId() != row.cacheId()))
                        hld = cacheMapHolder(ctx.cacheContext(row.cacheId()));

                    assert hld != null;

                    GridCacheMapEntry cached = putEntryIfObsoleteOrAbsent(
                        hld,
                        hld.cctx,
                        grp.affinity().lastVersion(),
                        row.key(),
                        true,
                        false);

                    if (cached instanceof GridDhtCacheEntry && ((GridDhtCacheEntry)cached).clearInternal(clearVer, extras)) {
                        removeEntry(cached);

                        if (rec && !hld.cctx.config().isEventsDisabled()) {
                            hld.cctx.events().addEvent(cached.partition(),
                                cached.key(),
                                ctx.localNodeId(),
                                (IgniteUuid)null,
                                null,
                                EVT_CACHE_REBALANCE_OBJECT_UNLOADED,
                                null,
                                false,
                                cached.rawGet(),
                                cached.hasValue(),
                                null,
                                null,
                                null,
                                false);
                        }

                        cleared++;
                    }

                    // For each 'stopCheckingFreq' cleared entities check clearing process to stop.
                    if (cleared % stopCheckingFreq == 0 && evictionCtx.shouldStop())
                        return cleared;
                }
                catch (GridDhtInvalidPartitionException e) {
                    assert isEmpty() && state() == EVICTED : "Invalid error [e=" + e + ", part=" + this + ']';

                    break; // Partition is already concurrently cleared and evicted.
                }
                finally {
                    ctx.database().checkpointReadUnlock();
                }
            }

            if (forceTestCheckpointOnEviction) {
                if (partWhereTestCheckpointEnforced == null && cleared >= fullSize()) {
                    ctx.database().forceCheckpoint("test").finishFuture().get();

                    log.warning("Forced checkpoint by test reasons for partition: " + this);

                    partWhereTestCheckpointEnforced = id;
                }
            }
        }
        catch (NodeStoppingException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get iterator for evicted partition: " + id);

            throw e;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to get iterator for evicted partition: " + id, e);
        }

        return cleared;
    }

    /**
     * Removes all cache entries from specified {@code map}.
     *
     * @param map Map to clear.
     * @param extras Obsolete extras.
     * @param evt Unload event flag.
     * @throws NodeStoppingException If current node is stopping.
     */
    private void clear(ConcurrentMap<KeyCacheObject, GridCacheMapEntry> map,
        GridCacheObsoleteEntryExtras extras,
        boolean evt) throws NodeStoppingException {
        Iterator<GridCacheMapEntry> it = map.values().iterator();

        while (it.hasNext()) {
            GridCacheMapEntry cached = null;

            ctx.database().checkpointReadLock();

            try {
                cached = it.next();

                if (cached instanceof GridDhtCacheEntry && ((GridDhtCacheEntry)cached).clearInternal(extras.obsoleteVersion(), extras)) {
                    removeEntry(cached);

                    if (!cached.isInternal()) {
                        if (evt) {
                            grp.addCacheEvent(cached.partition(),
                                cached.key(),
                                ctx.localNodeId(),
                                EVT_CACHE_REBALANCE_OBJECT_UNLOADED,
                                null,
                                false,
                                cached.rawGet(),
                                cached.hasValue(),
                                false);
                        }
                    }
                }
            }
            catch (GridDhtInvalidPartitionException e) {
                assert isEmpty() && state() == EVICTED : "Invalid error [e=" + e + ", part=" + this + ']';

                break; // Partition is already concurrently cleared and evicted.
            }
            catch (NodeStoppingException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to clear cache entry for evicted partition: " + cached.partition());

                throw e;
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to clear cache entry for evicted partition: " + cached, e);
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
    }

    /**
     * Removes all deferred delete requests from {@code rmvQueue}.
     */
    private void clearDeferredDeletes() {
        for (RemovedEntryHolder e : rmvQueue)
            removeVersionedEntry(e.cacheId(), e.key(), e.version());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"OverlyStrongTypeCast"})
    @Override public boolean equals(Object obj) {
        return obj instanceof GridDhtLocalPartition && (obj == this || ((GridDhtLocalPartition)obj).id() == id);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull GridDhtLocalPartition part) {
        if (part == null)
            return 1;

        return Integer.compare(id, part.id());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLocalPartition.class, this,
            "grp", grp.cacheOrGroupName(),
            "state", state(),
            "reservations", reservations(),
            "empty", isEmpty(),
            "createTime", U.format(createTime));
    }

    /** {@inheritDoc} */
    @Override public int publicSize(int cacheId) {
        if (grp.sharedGroup()) {
            CacheMapHolder hld = cacheMaps.get(cacheId);

            return hld != null ? hld.size.get() : 0;
        }

        return getSize(state.get());
    }

    /** {@inheritDoc} */
    @Override public void incrementPublicSize(@Nullable CacheMapHolder hld, GridCacheEntryEx e) {
        if (grp.sharedGroup()) {
            if (hld == null)
                hld = cacheMapHolder(e.context());

            hld.size.incrementAndGet();
        }

        while (true) {
            long state = this.state.get();

            if (this.state.compareAndSet(state, setSize(state, getSize(state) + 1)))
                return;
        }
    }

    /** {@inheritDoc} */
    @Override public void decrementPublicSize(@Nullable CacheMapHolder hld, GridCacheEntryEx e) {
        if (grp.sharedGroup()) {
            if (hld == null)
                hld = cacheMapHolder(e.context());

            hld.size.decrementAndGet();
        }

        while (true) {
            long state = this.state.get();

            assert getPartState(state) != EVICTED;

            if (this.state.compareAndSet(state, setSize(state, getSize(state) - 1)))
                return;
        }
    }

    /**
     * @param cacheId Cache ID.
     */
    void onCacheStopped(int cacheId) {
        assert grp.sharedGroup() : grp.cacheOrGroupName();

        for (Iterator<RemovedEntryHolder> it = rmvQueue.iterator(); it.hasNext();) {
            RemovedEntryHolder e = it.next();

            if (e.cacheId() == cacheId)
                it.remove();
        }

        cacheMaps.remove(cacheId);
    }

    /**
     * @param state Composite state.
     * @return Partition state.
     */
    private static GridDhtPartitionState getPartState(long state) {
        return GridDhtPartitionState.fromOrdinal((int)(state & (0x0000000000000007L)));
    }

    /**
     * @param state Composite state to update.
     * @param partState Partition state.
     * @return Updated composite state.
     */
    private static long setPartState(long state, GridDhtPartitionState partState) {
        return (state & (~0x0000000000000007L)) | partState.ordinal();
    }

    /**
     * @param state Composite state.
     * @return Reservations.
     */
    private static int getReservations(long state) {
        return (int)((state & 0x00000000FFFF0000L) >> 16);
    }

    /**
     * @param state Composite state to update.
     * @param reservations Reservations to set.
     * @return Updated composite state.
     */
    private static long setReservations(long state, int reservations) {
        return (state & (~0x00000000FFFF0000L)) | (reservations << 16);
    }

    /**
     * @param state Composite state.
     * @return Size.
     */
    private static int getSize(long state) {
        return (int)((state & 0xFFFFFFFF00000000L) >> 32);
    }

    /**
     * @param state Composite state to update.
     * @param size Size to set.
     * @return Updated composite state.
     */
    private static long setSize(long state, int size) {
        return (state & (~0xFFFFFFFF00000000L)) | ((long)size << 32);
    }

    /**
     * Removed entry holder.
     */
    private static class RemovedEntryHolder {
        /** */
        private final int cacheId;

        /** Cache key */
        private final KeyCacheObject key;

        /** Entry version */
        private final GridCacheVersion ver;

        /** Entry expire time. */
        private final long expireTime;

        /**
         * @param cacheId Cache ID.
         * @param key Key.
         * @param ver Entry version.
         * @param ttl TTL.
         */
        private RemovedEntryHolder(int cacheId, KeyCacheObject key, GridCacheVersion ver, long ttl) {
            this.cacheId = cacheId;
            this.key = key;
            this.ver = ver;

            expireTime = U.currentTimeMillis() + ttl;
        }

        /**
         * @return Cache ID.
         */
        int cacheId() {
            return cacheId;
        }

        /**
         * @return Key.
         */
        KeyCacheObject key() {
            return key;
        }

        /**
         * @return Version.
         */
        GridCacheVersion version() {
            return ver;
        }

        /**
         * @return item expired time
         */
        long expireTime() {
            return expireTime;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemovedEntryHolder.class, this);
        }
    }

    /**
     * Future is needed to control partition clearing process.
     * Future can be used both for single clearing or eviction processes.
     */
    class ClearFuture extends GridFutureAdapter<Boolean> {
        /** Flag indicates that eviction callback was registered on the current future. */
        private volatile boolean evictionCbRegistered;

        /** Flag indicates that clearing callback was registered on the current future. */
        private volatile boolean clearingCbRegistered;

        /** Flag indicates that future with all callbacks was finished. */
        private volatile boolean finished;

        /**
         * Constructor.
         */
        ClearFuture() {
            onDone();
            finished = true;
        }

        /**
         * Registers finish eviction callback on the future.
         *
         * @param updateSeq If {@code true} update topology sequence after successful eviction.
         */
        private void registerEvictionCallback(boolean updateSeq) {
            if (evictionCbRegistered)
                return;

            synchronized (this) {
                // Double check
                if (evictionCbRegistered)
                    return;

                evictionCbRegistered = true;

                // Initiates partition eviction and destroy.
                listen(f -> {
                    try {
                        // Check for errors.
                        f.get();

                        finishEviction(updateSeq);
                    }
                    catch (Exception e) {
                        rent.onDone(e);
                    }

                    evictionCbRegistered = false;
                });
            }
        }

        /**
         * Registers clearing callback on the future.
         */
        private void registerClearingCallback() {
            if (clearingCbRegistered)
                return;

            synchronized (this) {
                // Double check
                if (clearingCbRegistered)
                    return;

                clearingCbRegistered = true;

                // Recreate cache data store in case of allowed fast eviction, and reset clear flag.
                listen(f -> {
                    clear = false;

                    clearingCbRegistered = false;
                });
            }
        }

        /**
         * Successfully finishes the future.
         */
        public void finish() {
            synchronized (this) {
                onDone();
                finished = true;
            }
        }

        /**
         * Finishes the future with error.
         *
         * @param t Error.
         */
        public void finish(Throwable t) {
            synchronized (this) {
                onDone(t);
                finished = true;
            }
        }

        /**
         * Reuses future if it's done.
         * Adds appropriate callbacks to the future in case of eviction or single clearing.
         *
         * @param updateSeq Update sequence.
         * @param evictionRequested If {@code true} adds eviction callback, in other case adds single clearing callback.
         * @return {@code true} if future has been reinitialized.
         */
        public boolean initialize(boolean updateSeq, boolean evictionRequested) {
            // In case of running clearing just try to add missing callbacks to avoid extra synchronization.
            if (!finished) {
                if (evictionRequested)
                    registerEvictionCallback(updateSeq);
                else
                    registerClearingCallback();

                return false;
            }

            synchronized (this) {
                boolean done = isDone();

                if (done) {
                    reset();

                    finished = false;
                    evictionCbRegistered = false;
                    clearingCbRegistered = false;
                }

                if (evictionRequested)
                    registerEvictionCallback(updateSeq);
                else
                    registerClearingCallback();

                return done;
            }
        }
    }
}
