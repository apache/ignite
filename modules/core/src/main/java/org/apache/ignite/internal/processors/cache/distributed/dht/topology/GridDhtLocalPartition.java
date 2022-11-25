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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.pagemem.wal.record.PartitionClearingStartRecord;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TxCounters;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.util.deque.FastSizeDeque;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED;
import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;

/**
 * Key partition.
 */
public class GridDhtLocalPartition extends GridCacheConcurrentMapImpl implements Comparable<GridDhtLocalPartition>, GridReservable {
    /** */
    private static final GridCacheMapEntryFactory ENTRY_FACTORY = GridDhtCacheEntry::new;

    /** @see IgniteSystemProperties#IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE */
    public static final int DFLT_ATOMIC_CACHE_DELETE_HISTORY_SIZE = 200_000;

    /** Maximum size for delete queue. */
    public static final int MAX_DELETE_QUEUE_SIZE =
        Integer.getInteger(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, DFLT_ATOMIC_CACHE_DELETE_HISTORY_SIZE);

    /** @see IgniteSystemProperties#IGNITE_CACHE_REMOVED_ENTRIES_TTL */
    public static final int DFLT_CACHE_REMOVE_ENTRIES_TTL = 10_000;

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

    /** Rent future. */
    @GridToStringExclude
    private final GridFutureAdapter<?> rent;

    /** */
    @GridToStringExclude
    private final GridCacheSharedContext ctx;

    /** */
    @GridToStringExclude
    private final CacheGroupContext grp;

    /** Create time. */
    @GridToStringExclude
    private final long createTime = U.currentTimeMillis();

    /** */
    @GridToStringExclude
    private final IntMap<CacheMapHolder> cacheMaps;

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

    /** */
    private final AtomicReference<GridFutureAdapter<?>> finishFutRef = new AtomicReference<>();

    /** */
    private volatile long clearVer;

    /**
     * @param ctx Context.
     * @param grp Cache group.
     * @param id Partition ID.
     * @param recovery Flag indicates that partition is created during recovery phase.
     */
    public GridDhtLocalPartition(
            GridCacheSharedContext ctx,
            CacheGroupContext grp,
            int id,
            boolean recovery
    ) {
        super(ENTRY_FACTORY);

        this.id = id;
        this.ctx = ctx;
        this.grp = grp;

        log = U.logger(ctx.kernalContext(), logRef, this);

        if (grp.sharedGroup()) {
            singleCacheEntryMap = null;
            cacheMaps = new IntRWHashMap<>();
        }
        else {
            GridCacheContext cctx = grp.singleCacheContext();

            if (cctx.isNear())
                cctx = cctx.near().dht().context();

            singleCacheEntryMap = ctx.kernalContext().resource().resolve(
                new CacheMapHolder(cctx, createEntriesMap()));

            cacheMaps = null;
        }

        rent = new GridFutureAdapter<Object>() {
            @Override public String toString() {
                return "PartitionRentFuture [part=" + GridDhtLocalPartition.this + ']';
            }
        };

        int delQueueSize = grp.systemCache() ? 100 :
            Math.max(MAX_DELETE_QUEUE_SIZE / grp.affinity().partitions(), 20);

        rmvQueueMaxSize = U.ceilPow2(delQueueSize);

        rmvdEntryTtl = Long.getLong(IGNITE_CACHE_REMOVED_ENTRIES_TTL, DFLT_CACHE_REMOVE_ENTRIES_TTL);

        try {
            store = grp.offheap().createCacheDataStore(id);

            // Log partition creation for further crash recovery purposes.
            if (grp.persistenceEnabled() && grp.walEnabled() && !recovery)
                ctx.wal().log(new PartitionMetaStateRecord(grp.groupId(), id, state(), 0));

            // Inject row cache cleaner on store creation.
            // Used in case the cache with enabled SqlOnheapCache is single cache at the cache group.
            if (ctx.kernalContext().query().moduleEnabled())
                store.setRowCacheCleaner(ctx.kernalContext().indexProcessor().rowCacheCleaner(grp.groupId()));
        }
        catch (IgniteCheckedException e) {
            // TODO ignite-db
            throw new IgniteException(e);
        }

        if (log.isDebugEnabled())
            log.debug("Partition has been created [grp=" + grp.cacheOrGroupName()
                + ", p=" + id + ", state=" + state() + "]");

        clearVer = ctx.versions().localOrder();
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
            final AtomicInteger size = new AtomicInteger(0);

            cacheMaps.forEach((key, hld) -> size.addAndGet(hld.map.size()));

            return size.get();
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

        if (cctx.isNear())
            cctx = cctx.near().dht().context();

        CacheMapHolder old = cacheMaps.putIfAbsent(cctx.cacheIdBoxed(), hld = ctx.kernalContext().resource().resolve(
            new CacheMapHolder(cctx, createEntriesMap())));

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
    public long createTime() {
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
        return store.isEmpty() && internalSize() == 0;
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
    public void onRemoved(GridDhtCacheEntry entry) {
        assert entry.obsolete() : entry;

        // Make sure to remove exactly this entry.
        removeEntry(entry);
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
     * TODO FIXME Get rid of deferred delete queue https://issues.apache.org/jira/browse/IGNITE-11704
     */
    void cleanupRemoveQueue() {
        if (state() == MOVING) {
            if (rmvQueue.sizex() >= rmvQueueMaxSize) {
                LT.warn(log, "Deletion queue cleanup for moving partition was delayed until rebalance is finished. " +
                    "[grpId=" + this.grp.groupId() +
                    ", partId=" + id() +
                    ", grpParts=" + this.grp.affinity().partitions() +
                    ", maxRmvQueueSize=" + rmvQueueMaxSize + ']');
            }

            return;
        }

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
     * Reserves the partition so it won't be cleared or evicted.
     * Only MOVING, OWNING and LOST partitions can be reserved.
     *
     * @return {@code True} if reserved.
     */
    @Override public boolean reserve() {
        while (true) {
            long state = this.state.get();

            int ordinal = ordinal(state);

            if (ordinal == RENTING.ordinal() || ordinal == EVICTED.ordinal())
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

            assert reservations > 0;

            assert getPartState(state) != EVICTED : this;

            long newState = setReservations(state, --reservations);
            newState = setSize(newState, getSize(newState) + sizeChange);

            assert getSize(newState) == getSize(state) + sizeChange;

            // Decrement reservations.
            if (this.state.compareAndSet(state, newState)) {
                // If no more reservations try to continue delayed renting.
                if (reservations == 0)
                    tryContinueClearing();

                return;
            }
        }
    }

    /**
     * @param stateToRestore State to restore.
     */
    public void restoreState(GridDhtPartitionState stateToRestore) {
        state.set(setPartState(state.get(), stateToRestore));
    }

    /**
     * For testing purposes only.
     * @param toState State to set.
     */
    public void setState(GridDhtPartitionState toState) {
        if (grp.persistenceEnabled() && grp.walEnabled()) {
            synchronized (this) {
                long state0 = state.get();

                this.state.compareAndSet(state0, setPartState(state0, toState));

                try {
                    ctx.wal().log(new PartitionMetaStateRecord(grp.groupId(), id, toState, 0));
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Error while writing to log", e);
                }
            }
        }
        else
            restoreState(toState);
    }

    /**
     * @param state Current aggregated value.
     * @param toState State to switch to.
     * @return {@code true} if cas succeeds.
     */
    private boolean casState(long state, GridDhtPartitionState toState) {
        if (grp.persistenceEnabled() && grp.walEnabled()) {
            synchronized (this) {
                GridDhtPartitionState prevState = state();

                boolean updated = this.state.compareAndSet(state, setPartState(state, toState));

                if (updated) {
                    assert toState != EVICTED || reservations() == 0 : this;

                    try {
                        // Optimization: do not log OWNING -> OWNING.
                        if (prevState == OWNING && toState == LOST)
                            return true;

                        // Log LOST partitions as OWNING.
                        ctx.wal().log(
                            new PartitionMetaStateRecord(grp.groupId(), id, toState == LOST ? OWNING : toState, 0));
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to log partition state change to WAL.", e);

                        ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
                    }

                    if (log.isDebugEnabled())
                        log.debug("Partition changed state [grp=" + grp.cacheOrGroupName()
                            + ", p=" + id + ", prev=" + prevState + ", to=" + toState + "]");
                }

                return updated;
            }
        }
        else {
            GridDhtPartitionState prevState = state();

            boolean updated = this.state.compareAndSet(state, setPartState(state, toState));

            if (updated) {
                assert toState != EVICTED || reservations() == 0 : this;

                if (log.isDebugEnabled())
                    log.debug("Partition changed state [grp=" + grp.cacheOrGroupName()
                        + ", p=" + id + ", prev=" + prevState + ", to=" + toState + "]");
            }

            return updated;
        }
    }

    /**
     * @return {@code True} if transitioned to OWNING state.
     */
    public boolean own() {
        while (true) {
            long state = this.state.get();

            GridDhtPartitionState partState = getPartState(state);
            if (partState == RENTING || partState == EVICTED)
                return false;

            if (partState == OWNING)
                return true;

            assert partState == MOVING || partState == LOST;

            if (casState(state, OWNING))
                return true;
        }
    }

    /**
     * Forcibly moves partition to a MOVING state.
     *
     * @return {@code True} if a partition was switched to MOVING state.
     */
    public boolean moving() {
        while (true) {
            long state = this.state.get();

            GridDhtPartitionState partState = getPartState(state);

            if (partState == EVICTED)
                return false;

            assert partState == OWNING || partState == RENTING :
                "Only partitions in state OWNING or RENTING can be moved to MOVING state " + partState + " " + id;

            if (casState(state, MOVING)) {
                // The state is switched under global topology lock, safe to record version here.
                updateClearVersion();

                return true;
            }
        }
    }

    /**
     * Records a version for row clearing. Must be called when a partition is marked for full rebalancing.
     * @see #clearAll(EvictionContext)
     */
    public void updateClearVersion() {
        clearVer = ctx.versions().localOrder();
    }

    /**
     * Used to set a version from {@link PartitionClearingStartRecord} when need to repeat a clearing after node restart.
     * @param clearVer Clear version.
     */
    public void updateClearVersion(long clearVer) {
        this.clearVer = clearVer;
    }

    /**
     * @return {@code True} if partition state changed.
     */
    public boolean markLost() {
        while (true) {
            long state = this.state.get();

            GridDhtPartitionState partState = getPartState(state);

            if (partState == LOST)
                return false;

            if (casState(state, LOST))
                return true;
        }
    }

    /**
     * Initiates partition eviction process and returns an eviction future.
     * Future will be completed when a partition is moved to EVICTED state (possibly not yet physically deleted).
     *
     * If partition has reservations, eviction will be delayed and continued after all reservations will be released.
     *
     * @return Future to signal that this node is no longer an owner or backup or null if corresponding partition
     * state is {@code RENTING} or {@code EVICTED}.
     */
    public IgniteInternalFuture<?> rent() {
        long state0 = this.state.get();

        GridDhtPartitionState partState = getPartState(state0);

        if (partState == EVICTED)
            return rent;

        if (partState == RENTING) {
            // If for some reason a partition has stuck in renting state try restart clearing.
            if (finishFutRef.get() == null)
                clearAsync();

            return rent;
        }

        if (tryInvalidateGroupReservations() && getReservations(state0) == 0 && casState(state0, RENTING)) {
            // Evict asynchronously, as the 'rent' method may be called from within write locks on local partition.
            clearAsync();
        }
        else
            delayedRenting = true;

        return rent;
    }

    /**
     * Continue clearing if it was delayed before due to reservation and topology version not changed.
     */
    public void tryContinueClearing() {
        if (delayedRenting)
            group().topology().rent(id);
    }

    /**
     * Initiates a partition clearing attempt.
     *
     * @return A future what will be finished then a current clearing attempt is done.
     */
    public IgniteInternalFuture<?> clearAsync() {
        long state = this.state.get();

        GridDhtPartitionState partState = getPartState(state);

        boolean evictionRequested = partState == RENTING;
        boolean clearingRequested = partState == MOVING;

        if (!evictionRequested && !clearingRequested)
            return new GridFinishedFuture<>();

        GridFutureAdapter<?> finishFut = new GridFutureAdapter<>();

        do {
            GridFutureAdapter<?> curFut = finishFutRef.get();

            if (curFut != null)
                return curFut;
        }
        while (!finishFutRef.compareAndSet(null, finishFut));

        finishFut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> fut) {
                // A partition cannot be reused after the eviction, it's not necessary to reset a clearing state.
                if (state() == EVICTED)
                    rent.onDone(fut.error());
                else
                    finishFutRef.set(null);
            }
        });

        // Evict partition asynchronously to avoid deadlocks.
        ctx.evict().evictPartitionAsync(grp, this, finishFut);

        return finishFut;
    }

    /**
     * Invalidates all partition group reservations, so they can't be reserved again any more.
     *
     * @return {@code true} If all group reservations are invalidated (or no such reservations).
     */
    private boolean tryInvalidateGroupReservations() {
        for (GridDhtPartitionsReservation reservation : reservations) {
            if (!reservation.invalidate())
                return false; // Failed to invalidate reservation -> we are reserved.
        }

        return true;
    }

    /**
     * @param state State.
     * @return {@code True} if partition has no reservations and empty.
     */
    private boolean freeAndEmpty(long state) {
        return isEmpty() && getSize(state) == 0 && getReservations(state) == 0;
    }

    /**
     * Moves partition state to {@code EVICTED} if possible.
     */
    public void finishEviction() {
        long state0 = this.state.get();

        GridDhtPartitionState state = getPartState(state0);

        // Some entries still might be present in partition cache maps due to concurrent updates on backup nodes,
        // but it's safe to finish eviction because no physical updates are possible.
        // A partition is promoted to EVICTED state if it is not reserved and empty.
        if (store.isEmpty() && getReservations(state0) == 0 && state == RENTING)
            casState(state0, EVICTED);
    }

    /**
     * @return {@code True} if clearing process is running at the moment on the partition.
     */
    public boolean isClearing() {
        return finishFutRef.get() != null;
    }

    /**
     * On partition unlock callback.
     * Tries to continue delayed partition clearing.
     */
    public void onUnlock() {
        // No-op.
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
     * Returns new update counter for primary node or passed counter for backup node.
     * <p>
     * Used for non-tx cases.
     * <p>
     * Counter generation/update logic is delegated to counter implementation.
     *
     * @param cacheId ID of cache initiated counter update.
     * @param topVer Topology version for current operation.
     * @param init {@code True} if initial update.
     * @return Next update index.
     */
    public long nextUpdateCounter(int cacheId, AffinityTopologyVersion topVer, boolean primary, boolean init,
        @Nullable Long primaryCntr) {
        long nextCntr;

        if (primaryCntr == null) // Primary node.
            nextCntr = store.nextUpdateCounter();
        else {
            assert !init : "Initial update must generate a counter for partition " + this;

            // Backup.
            assert primaryCntr != 0;

            store.updateCounter(nextCntr = primaryCntr);
        }

        if (grp.sharedGroup())
            grp.onPartitionCounterUpdate(cacheId, id, nextCntr, topVer, primary);

        return nextCntr;
    }

    /**
     * Used for transactions.
     *
     * @param cacheId Cache id.
     * @param tx Tx.
     * @param primaryCntr Primary counter.
     */
    public long nextUpdateCounter(int cacheId, IgniteInternalTx tx, @Nullable Long primaryCntr) {
        Long nextCntr;

        if (primaryCntr != null)
            nextCntr = primaryCntr;
        else {
            TxCounters txCounters = tx.txCounters(false);

            assert txCounters != null : "Must have counters for tx [nearXidVer=" + tx.nearXidVersion() + ']';

            // Null must never be returned on primary node.
            nextCntr = txCounters.generateNextCounter(cacheId, id());

            assert nextCntr != null : this;
        }

        if (grp.sharedGroup())
            grp.onPartitionCounterUpdate(cacheId, id, nextCntr, tx.topologyVersion(), tx.local());

        return nextCntr;
    }

    /**
     * @return Current update counter (LWM).
     */
    public long updateCounter() {
        return store.updateCounter();
    }

    /**
     * @return Current reserved counter (HWM).
     */
    public long reservedCounter() {
        return store.reservedCounter();
    }

    /**
     * @param val Update counter value.
     */
    public void updateCounter(long val) {
        store.updateCounter(val);
    }

    /**
     * @return Initial update counter.
     */
    public long initialUpdateCounter() {
        return store.initialUpdateCounter();
    }

    /**
     * Increments cache update counter on primary node.
     *
     * @param delta Value to be added to update counter.
     * @return Update counter value before update.
     */
    public long getAndIncrementUpdateCounter(long delta) {
        return store.getAndIncrementUpdateCounter(delta);
    }

    /**
     * Updates MVCC cache update counter on backup node.
     *
     * @param start Start position
     * @param delta Delta.
     */
    public boolean updateCounter(long start, long delta) {
        return store.updateCounter(start, delta);
    }

    /**
     * Reset partition update counter.
     */
    public void resetUpdateCounter() {
        store.resetUpdateCounter();
    }

    /**
     * Reset partition initial update counter.
     */
    public void resetInitialUpdateCounter() {
        store.resetInitialUpdateCounter();
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
    protected long clearAll(EvictionContext evictionCtx) throws NodeStoppingException {
        long order = clearVer;

        GridCacheVersion clearVer = ctx.versions().startVersion();

        GridCacheObsoleteEntryExtras extras = new GridCacheObsoleteEntryExtras(clearVer);

        boolean rec = grp.eventRecordable(EVT_CACHE_REBALANCE_OBJECT_UNLOADED);

        long cleared = 0;
        int stopCntr = 0;

        CacheMapHolder hld = grp.sharedGroup() ? null : singleCacheEntryMap;

        boolean recoveryMode = ctx.kernalContext().recoveryMode();

        try {
            // If a partition was not checkpointed after clearing on a rebalance and a node was stopped,
            // then it's need to repeat clearing on node start. So need to write a partition clearing start record
            // and repeat clearing on applying updates from WAL if the record was read.
            // It's need for atomic cache only. Transactional cache start a rebalance due to outdated counter in this case,
            // because atomic and transactional caches use different partition counters implementation.
            if (state() == MOVING && !recoveryMode && grp.persistenceEnabled() && grp.walEnabled() &&
                grp.config().getAtomicityMode() == ATOMIC)
                ctx.wal().log(new PartitionClearingStartRecord(id, grp.groupId(), order));

            GridIterator<CacheDataRow> it0 = grp.offheap().partitionIterator(id);

            while (it0.hasNext()) {
                if ((stopCntr = (stopCntr + 1) & 1023) == 0 && evictionCtx.shouldStop())
                    return cleared;

                ctx.database().checkpointReadLock();

                try {
                    CacheDataRow row = it0.next();

                    // Do not clear fresh rows in case of partition reloading.
                    // This is required because normal updates are possible to moving partition which is currently cleared.
                    // We can clean OWNING partition if a partition has been reset from lost state.
                    // In this case new updates must be preserved.
                    // Partition state can be switched from RENTING to MOVING and vice versa during clearing.
                    long order0 = row.version().order();

                    if ((state() == MOVING || recoveryMode) && (order0 == 0 /** Inserted by isolated updater. */ || order0 > order))
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
                        true);

                    assert cached != null : "Expecting the reservation " + this;

                    if (cached.deleted())
                        continue;

                    if (cached instanceof GridDhtCacheEntry && ((GridDhtCacheEntry)cached).clearInternal(clearVer, extras)) {
                        removeEntry(cached);

                        if (rec && !hld.cctx.config().isEventsDisabled()) {
                            hld.cctx.events().addEvent(cached.partition(),
                                cached.key(),
                                ctx.localNodeId(),
                                null,
                                null,
                                null,
                                EVT_CACHE_REBALANCE_OBJECT_UNLOADED,
                                null,
                                false,
                                cached.rawGet(),
                                cached.hasValue(),
                                null,
                                null,
                                false);
                        }

                        cleared++;
                    }
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
                    ctx.database().forceCheckpoint("test").futureFor(FINISHED).get();

                    log.warning("Forced checkpoint by test reasons for partition: " + this);

                    partWhereTestCheckpointEnforced = id;
                }
            }

            // Attempt to destroy.
            if (!recoveryMode)
                ((GridDhtPreloader)grp.preloader()).tryFinishEviction(this);
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
     * Clears all data for this partition from indexes.
     * @return Set of cleared indexes.
     */
    public Set<Index> removeAllFromIndexes(EvictionContext grpEvictionCtx) {
        if (!QueryUtils.isEnabled(grp.config()))
            return Collections.emptySet();

        GridCacheContext<?, ?> gctx = ctx.cacheContext(grp.groupId());

        Set<Index> res = new HashSet<>();

        for (GridCacheContext<?, ?> cctx : gctx.group().caches())
            res.addAll(ctx.kernalContext().query().removeAllForPartition(cctx, id, grpEvictionCtx));

        return res;
    }

    /**
     * Removes all deferred delete requests from {@code rmvQueue}.
     */
    public void clearDeferredDeletes() {
        for (RemovedEntryHolder e : rmvQueue)
            removeVersionedEntry(e.cacheId(), e.key(), e.version());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * id + grp.groupId();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridDhtLocalPartition part = (GridDhtLocalPartition)o;

        return id == part.id && grp.groupId() == part.group().groupId();
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
            "createTime", U.format(createTime),
            "fullSize", fullSize(),
            "cntr", dataStore().partUpdateCounter());
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
     * Returns group context.
     *
     * @return Group context.
     */
    public CacheGroupContext group() {
        return grp;
    }

    /**
     * @param cacheId Cache ID.
     */
    public void onCacheStopped(int cacheId) {
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
     * @param state State.
     */
    private static int ordinal(long state) {
        return (int)(state & (0x0000000000000007L));
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
     * Flushes pending update counters closing all possible gaps.
     *
     * @return Even-length array of pairs [start, end] for each gap.
     */
    public GridLongList finalizeUpdateCounters() {
        return store.finalizeUpdateCounters();
    }

    /**
     * Called before next batch is about to be applied during rebalance. Currently used for tests.
     *
     * @param last {@code True} if last batch for partition.
     */
    public void beforeApplyBatch(boolean last) {
        // No-op.
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
     * Collects detailed info about the partition.
     *
     * @param buf Buffer.
     */
    public void dumpDebugInfo(SB buf) {
        GridDhtPartitionTopology top = grp.topology();
        AffinityTopologyVersion topVer = top.readyTopologyVersion();

        if (!topVer.initialized()) {
            buf.a(toString());

            return;
        }

        final int limit = 3;

        buf.a("[topVer=").a(topVer);
        buf.a(", lastChangeTopVer=").a(top.lastTopologyChangeVersion());
        buf.a(", waitRebalance=").a(ctx.kernalContext().cache().context().affinity().waitRebalance(grp.groupId(), id));
        buf.a(", nodes=").a(F.nodeIds(top.nodes(id, topVer)).stream().limit(limit).collect(Collectors.toList()));
        buf.a(", locPart=").a(toString());

        NavigableSet<AffinityTopologyVersion> versions = grp.affinity().cachedVersions();

        int i = 5;

        Iterator<AffinityTopologyVersion> iter = versions.descendingIterator();

        while (--i >= 0 && iter.hasNext()) {
            AffinityTopologyVersion topVer0 = iter.next();
            buf.a(", ver").a(i).a('=').a(topVer0);

            Collection<UUID> nodeIds = F.nodeIds(grp.affinity().cachedAffinity(topVer0).get(id));
            buf.a(", affOwners").a(i).a('=').a(nodeIds.stream().limit(limit).collect(Collectors.toList()));
        }

        buf.a(']');
    }
}
