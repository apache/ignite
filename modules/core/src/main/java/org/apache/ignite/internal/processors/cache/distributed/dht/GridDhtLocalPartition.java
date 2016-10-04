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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMapImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.CacheDataRow;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridCircularBuffer;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
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
public class GridDhtLocalPartition implements Comparable<GridDhtLocalPartition>, GridReservable, GridCacheConcurrentMap {
    /** Maximum size for delete queue. */
    public static final int MAX_DELETE_QUEUE_SIZE = Integer.getInteger(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE,
        200_000);

    /** Static logger to avoid re-creation. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static volatile IgniteLogger log;

    /** Partition ID. */
    private final int id;

    /** State. */
    @GridToStringExclude
    private final AtomicLong state = new AtomicLong((long)MOVING.ordinal() << 32);

    /** Evict guard. Must be CASed to -1 only when partition state is EVICTED. */
    @GridToStringExclude
    private final AtomicInteger evictGuard = new AtomicInteger();

    /** Rent future. */
    @GridToStringExclude
    private final GridFutureAdapter<?> rent;

    /** Entries map. */
    private final GridCacheConcurrentMap map;

    /** Context. */
    private final GridCacheContext cctx;

    /** Create time. */
    @GridToStringExclude
    private final long createTime = U.currentTimeMillis();

    /** Eviction history. */
    private volatile Map<KeyCacheObject, GridCacheVersion> evictHist = new HashMap<>();

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Remove queue. */
    private final GridCircularBuffer<T2<KeyCacheObject, GridCacheVersion>> rmvQueue;

    /** Group reservations. */
    private final CopyOnWriteArrayList<GridDhtPartitionsReservation> reservations = new CopyOnWriteArrayList<>();

    /** */
    private final CacheDataStore store;

    /** Partition updates. */
    private ConcurrentNavigableMap<Long, Boolean> updates = new ConcurrentSkipListMap<>();

    /** Last applied update. */
    private AtomicLong lastApplied = new AtomicLong(0);

    /** Set if failed to move partition to RENTING state due to reservations, to be checked when
     * reservation is released. */
    private volatile boolean shouldBeRenting;

    /**
     * @param cctx Context.
     * @param id Partition ID.
     * @param entryFactory Entry factory.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    GridDhtLocalPartition(GridCacheContext cctx, int id, GridCacheMapEntryFactory entryFactory) {
        assert cctx != null;

        this.id = id;
        this.cctx = cctx;

        log = U.logger(cctx.kernalContext(), logRef, this);

        rent = new GridFutureAdapter<Object>() {
            @Override public String toString() {
                return "PartitionRentFuture [part=" + GridDhtLocalPartition.this + ", map=" + map + ']';
            }
        };

        map = new GridCacheConcurrentMapImpl(cctx, entryFactory, cctx.config().getStartSize() / cctx.affinity().partitions());

        int delQueueSize = CU.isSystemCache(cctx.name()) ? 100 :
            Math.max(MAX_DELETE_QUEUE_SIZE / cctx.affinity().partitions(), 20);

        rmvQueue = new GridCircularBuffer<>(U.ceilPow2(delQueueSize));

        try {
            store = cctx.offheap().createCacheDataStore(id);
        }
        catch (IgniteCheckedException e) {
            // TODO ignite-db
            throw new IgniteException(e);
        }
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
        assert GridDhtPartitionState.fromOrdinal((int)(state.get() >> 32)) != EVICTED :
            "we can reserve only active partitions";
        assert (state.get() & 0xFFFF) != 0 : "partition must be already reserved before adding group reservation";

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
        return GridDhtPartitionState.fromOrdinal((int)(state.get() >> 32));
    }

    /**
     * @return Reservations.
     */
    public int reservations() {
        return (int)(state.get() & 0xFFFF);
    }

    /**
     * @return Keys belonging to partition.
     */
    public Set<KeyCacheObject> keySet() {
        return map.keySet();
    }

    /**
     * @return {@code True} if partition is empty.
     */
    public boolean isEmpty() {
        return size() == 0 && map.size() == 0;
    }

    /**
     * @return Last applied update.
     */
    public long lastAppliedUpdate() {
        return lastApplied.get();
    }

    /**
     * @return Last received update.
     */
    private long lastReceivedUpdate() {
        if (updates.isEmpty())
            return lastApplied.get();

        return updates.lastKey();
    }

    /**
     * @param cntr Received counter.
     */
    public void onUpdateReceived(long cntr) {
        boolean changed = updates.putIfAbsent(cntr, true) == null;

        if (!changed)
            return;

        while (true) {
            Map.Entry<Long, Boolean> entry = updates.firstEntry();

            if (entry == null)
                return;

            long first = entry.getKey();

            long cntr0 = lastApplied.get();

            if (first <= cntr0)
                updates.remove(first);
            else if (first == cntr0 + 1)
                if (lastApplied.compareAndSet(cntr0, first))
                    updates.remove(first);
                else
                    break;
            else
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return (int)store.size();
    }

    /** {@inheritDoc} */
    @Override public int publicSize() {
        return (int)store.size();
    }

    /** {@inheritDoc} */
    @Override public void incrementPublicSize(GridCacheEntryEx e) {
        map.incrementPublicSize(e);
    }

    /** {@inheritDoc} */
    @Override public void decrementPublicSize(GridCacheEntryEx e) {
        map.decrementPublicSize(e);
    }

    /**
     * @return If partition is moving or owning or renting.
     */
    public boolean valid() {
        GridDhtPartitionState state = state();

        return state == MOVING || state == OWNING || state == RENTING;
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridCacheMapEntry getEntry(KeyCacheObject key) {
        return map.getEntry(key);
    }

    /** {@inheritDoc} */
    @Override public boolean removeEntry(GridCacheEntryEx entry) {
        return map.removeEntry(entry);
    }

    /** {@inheritDoc} */
    @Override public Iterable<GridCacheMapEntry> entries(
        CacheEntryPredicate... filter) {
        return map.entries(filter);
    }

    /** {@inheritDoc} */
    @Override public Iterable<GridCacheMapEntry> allEntries(CacheEntryPredicate... filter) {
        return map.allEntries(filter);
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheMapEntry> entrySet(CacheEntryPredicate... filter) {
        return map.entrySet(filter);
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridCacheMapEntry randomEntry() {
        return map.randomEntry();
    }

    /** {@inheritDoc} */
    @Override public GridCacheMapEntry putEntryIfObsoleteOrAbsent(
        AffinityTopologyVersion topVer, KeyCacheObject key,
        @Nullable CacheObject val, boolean create, boolean touch) {
        return map.putEntryIfObsoleteOrAbsent(topVer, key, val, create, touch);
    }

    /** {@inheritDoc} */
    @Override public Set<KeyCacheObject> keySet(CacheEntryPredicate... filter) {
        return map.keySet(filter);
    }

    /**
     * @param entry Entry to remove.
     */
    void onRemoved(GridDhtCacheEntry entry) {
        assert entry.obsolete() : entry;

        // Make sure to remove exactly this entry.
        map.removeEntry(entry);

        // Attempt to evict.
        tryEvict();
    }

    /**
     * @param key Removed key.
     * @param ver Removed version.
     * @throws IgniteCheckedException If failed.
     */
    public void onDeferredDelete(KeyCacheObject key, GridCacheVersion ver) throws IgniteCheckedException {
        try {
            T2<KeyCacheObject, GridCacheVersion> evicted = rmvQueue.add(new T2<>(key, ver));

            if (evicted != null)
                cctx.dht().removeVersionedEntry(evicted.get1(), evicted.get2());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
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
     * @param key Key.
     * @param ver Version.
     */
    public void onEntryEvicted(KeyCacheObject key, GridCacheVersion ver) {
        assert key != null;
        assert ver != null;
        assert lock.isHeldByCurrentThread(); // Only one thread can enter this method at a time.

        if (state() != MOVING)
            return;

        Map<KeyCacheObject, GridCacheVersion> evictHist0 = evictHist;

        if (evictHist0 != null) {
            GridCacheVersion ver0 = evictHist0.get(key);

            if (ver0 == null || ver0.isLess(ver)) {
                GridCacheVersion ver1 = evictHist0.put(key, ver);

                assert ver1 == ver0;
            }
        }
    }

    /**
     * Cache preloader should call this method within partition lock.
     *
     * @param key Key.
     * @param ver Version.
     * @return {@code True} if preloading is permitted.
     */
    public boolean preloadingPermitted(KeyCacheObject key, GridCacheVersion ver) {
        assert key != null;
        assert ver != null;
        assert lock.isHeldByCurrentThread(); // Only one thread can enter this method at a time.

        if (state() != MOVING)
            return false;

        Map<KeyCacheObject, GridCacheVersion> evictHist0 = evictHist;

        if (evictHist0 != null) {
            GridCacheVersion ver0 = evictHist0.get(key);

            // Permit preloading if version in history
            // is missing or less than passed in.
            return ver0 == null || ver0.isLess(ver);
        }

        return false;
    }

    /**
     * Reserves a partition so it won't be cleared.
     *
     * @return {@code True} if reserved.
     */
    @Override public boolean reserve() {
        while (true) {
            long reservations = state.get();

            if ((int)(reservations >> 32) == EVICTED.ordinal())
                return false;

            if (state.compareAndSet(reservations, reservations + 1))
                return true;
        }
    }

    /**
     * Releases previously reserved partition.
     */
    @Override public void release() {
        while (true) {
            long reservations = state.get();

            if ((int)(reservations & 0xFFFF) == 0)
                return;

            assert (int)(reservations >> 32) != EVICTED.ordinal();

            // Decrement reservations.
            if (state.compareAndSet(reservations, --reservations)) {
                if ((reservations & 0xFFFF) == 0 && shouldBeRenting)
                    rent(true);

                tryEvict();

                break;
            }
        }
    }

    /**
     * @param reservations Current aggregated value.
     * @param toState State to switch to.
     * @return {@code true} if cas succeeds.
     */
    private boolean casState(long reservations, GridDhtPartitionState toState) {
        return state.compareAndSet(reservations, (reservations & 0xFFFF) | ((long)toState.ordinal() << 32));
    }

    /**
     * @return {@code True} if transitioned to OWNING state.
     */
    boolean own() {
        while (true) {
            long reservations = state.get();

            int ord = (int)(reservations >> 32);

            if (ord == RENTING.ordinal() || ord == EVICTED.ordinal())
                return false;

            if (ord == OWNING.ordinal())
                return true;

            assert ord == MOVING.ordinal() || ord == LOST.ordinal();

            if (casState(reservations, OWNING)) {
                if (log.isDebugEnabled())
                    log.debug("Owned partition: " + this);

                // No need to keep history any more.
                evictHist = null;

                return true;
            }
        }
    }

    /**
     * @return {@code True} if partition state changed.
     */
    boolean markLost() {
        while (true) {
            long reservations = state.get();

            int ord = (int)(reservations >> 32);

            if (ord == LOST.ordinal())
                return false;

            if (casState(reservations, LOST)) {
                if (log.isDebugEnabled())
                    log.debug("Marked partition as LOST: " + this);

                // No need to keep history any more.
                evictHist = null;

                return true;
            }
        }
    }

    /**
     * @param updateSeq Update sequence.
     * @return Future to signal that this node is no longer an owner or backup.
     */
    IgniteInternalFuture<?> rent(boolean updateSeq) {
        long reservations = state.get();

        int ord = (int)(reservations >> 32);

        if (ord == RENTING.ordinal() || ord == EVICTED.ordinal())
            return rent;

        shouldBeRenting = true;

        if ((reservations & 0xFFFF) == 0 && casState(reservations, RENTING)) {
                shouldBeRenting = false;

            if (log.isDebugEnabled())
                log.debug("Moved partition to RENTING state: " + this);

            // Evict asynchronously, as the 'rent' method may be called
            // from within write locks on local partition.
            tryEvictAsync(updateSeq);
        }

        return rent;
    }

    /**
     * @param updateSeq Update sequence.
     */
    void tryEvictAsync(boolean updateSeq) {
        long reservations = state.get();

        int ord = (int)(reservations >> 32);

        if (isEmpty() &&
            ord == RENTING.ordinal() && (reservations & 0xFFFF) == 0 && !groupReserved() &&
            casState(reservations, EVICTED)) {
            if (log.isDebugEnabled())
                log.debug("Evicted partition: " + this);

            if (markForDestroy())
                finishDestroy(updateSeq);
        }
        else
            cctx.preloader().evictPartitionAsync(this);
    }

    /**
     * @return {@code true} If there is a group reservation.
     */
    boolean groupReserved() {
        for (GridDhtPartitionsReservation reservation : reservations) {
            if (!reservation.invalidate())
                return true; // Failed to invalidate reservation -> we are reserved.
        }

        return false;
    }

    /**
     * @return {@code True} if evicting thread was added.
     */
    private boolean addEvicting() {
        while (true) {
            int cnt = evictGuard.get();

            if (cnt < 0)
                return false;

            if (evictGuard.compareAndSet(cnt, cnt + 1))
                return true;
        }
    }

    /**
     *
     */
    private void clearEvicting() {
       boolean free = false;

        while (true) {
            int cnt = evictGuard.get();

            assert cnt > 0;

            if (evictGuard.compareAndSet(cnt, cnt - 1)) {
                free = cnt == 1;

                break;
            }
        }

        if (free && state() == EVICTED) {
            if (markForDestroy())
                finishDestroy(true);
        }
    }

    /**
     * @return {@code True} if partition is safe to destroy
     */
    private boolean markForDestroy() {
        while (true) {
            int cnt = evictGuard.get();

            if (cnt != 0)
                return false;

            if (evictGuard.compareAndSet(0, -1))
                return true;
        }
    }

    /**
     * @param updateSeq Update sequence request.
     */
    private void finishDestroy(boolean updateSeq) {
        assert state() == EVICTED : this;
        assert evictGuard.get() == -1;

        if (cctx.isDrEnabled())
            cctx.dr().partitionEvicted(id);

        cctx.continuousQueries().onPartitionEvicted(id);

        cctx.dataStructures().onPartitionEvicted(id);

        destroyCacheDataStore();

        rent.onDone();

        ((GridDhtPreloader)cctx.preloader()).onPartitionEvicted(this, updateSeq);

        clearDeferredDeletes();
    }

    /**
     *
     */
    public void tryEvict() {
        long reservations = state.get();

        int ord = (int)(reservations >> 32);

        if (ord != RENTING.ordinal() || (reservations & 0xFFFF) != 0 || groupReserved())
            return;

        if (addEvicting()) {
            try {
                // Attempt to evict partition entries from cache.
                clearAll();

                if (isEmpty() && casState(reservations, EVICTED)) {
                    if (log.isDebugEnabled())
                        log.debug("Evicted partition: " + this);
                    // finishDestroy() will be initiated by clearEvicting().
                }
            }
            finally {
                clearEvicting();
            }
        }
    }

    /**
     * Release created data store for this partition.
     */
    private void destroyCacheDataStore() {
        try {
            CacheDataStore store = dataStore();

            cctx.offheap().destroyCacheDataStore(id, store);
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to destroy cache data store on partition eviction [id=" + id + "]", e);
        }
    }

    /**
     *
     */
    void onUnlock() {
        tryEvict();
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if local node is primary for this partition.
     */
    public boolean primary(AffinityTopologyVersion topVer) {
        return cctx.affinity().primary(cctx.localNode(), id, topVer);
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if local node is backup for this partition.
     */
    public boolean backup(AffinityTopologyVersion topVer) {
        return cctx.affinity().backup(cctx.localNode(), id, topVer);
    }

    /**
     * @return Next update index.
     */
    public long nextUpdateCounter() {
        return store.nextUpdateCounter();
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
     * Clears values for this partition.
     */
    public void clearAll() {
        GridCacheVersion clearVer = cctx.versions().next();

        boolean rec = cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_UNLOADED);

        Iterator<GridDhtCacheEntry> it = (Iterator)map.allEntries().iterator();

        GridCacheObsoleteEntryExtras extras = new GridCacheObsoleteEntryExtras(clearVer);

        while (it.hasNext()) {
            GridDhtCacheEntry cached = null;

            cctx.shared().database().checkpointReadLock();

            try {
                cached = it.next();

                if (cached.clearInternal(clearVer, extras)) {
                    map.removeEntry(cached);

                    if (!cached.isInternal()) {
                        if (rec) {
                            cctx.events().addEvent(cached.partition(),
                                cached.key(),
                                cctx.localNodeId(),
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
                    }
                }
            }
            catch (GridDhtInvalidPartitionException e) {
                assert isEmpty() && state() == EVICTED : "Invalid error [e=" + e + ", part=" + this + ']';

                break; // Partition is already concurrently cleared and evicted.
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to clear cache entry for evicted partition: " + cached, e);
            }
            finally {
                cctx.shared().database().checkpointReadUnlock();
            }
        }

        try {
            GridIterator<CacheDataRow> it0 = cctx.offheap().iterator(id);

            while (it0.hasNext()) {
                cctx.shared().database().checkpointReadLock();

                try {
                    CacheDataRow row = it0.next();

                    GridDhtCacheEntry cached = (GridDhtCacheEntry)cctx.cache().entryEx(row.key());

                    if (cached.clearInternal(clearVer, extras)) {
                        if (rec) {
                            cctx.events().addEvent(cached.partition(),
                                cached.key(),
                                cctx.localNodeId(),
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

                        map.removeEntry(cached);
                    }
                }
                catch (GridDhtInvalidPartitionException e) {
                    assert isEmpty() && state() == EVICTED : "Invalid error [e=" + e + ", part=" + this + ']';

                    break; // Partition is already concurrently cleared and evicted.
                }
                finally {
                    cctx.shared().database().checkpointReadUnlock();
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to get iterator for evicted partition: " + id, e);
        }
    }

    /**
     *
     */
    private void clearDeferredDeletes() {
        rmvQueue.forEach(new CI1<T2<KeyCacheObject, GridCacheVersion>>() {
            @Override public void apply(T2<KeyCacheObject, GridCacheVersion> t) {
                cctx.dht().removeVersionedEntry(t.get1(), t.get2());
            }
        });
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
            "state", state(),
            "reservations", reservations(),
            "empty", isEmpty(),
            "createTime", U.format(createTime));
    }
}
