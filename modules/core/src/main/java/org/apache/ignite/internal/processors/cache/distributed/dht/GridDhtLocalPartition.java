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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSwapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridCircularBuffer;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.LongAdder8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.RENTING;

/**
 * Key partition.
 */
public class GridDhtLocalPartition implements Comparable<GridDhtLocalPartition>, GridReservable {
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
    private final AtomicStampedReference<GridDhtPartitionState> state =
        new AtomicStampedReference<>(MOVING, 0);

    /** Rent future. */
    @GridToStringExclude
    private final GridFutureAdapter<?> rent;

    /** Entries map. */
    private final ConcurrentMap<KeyCacheObject, GridDhtCacheEntry> map;

    /** Context. */
    private final GridCacheContext cctx;

    /** Create time. */
    @GridToStringExclude
    private final long createTime = U.currentTimeMillis();

    /** Eviction history. */
    private volatile Map<KeyCacheObject, GridCacheVersion> evictHist = new HashMap<>();

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Public size counter. */
    private final LongAdder8 mapPubSize = new LongAdder8();

    /** Remove queue. */
    private final GridCircularBuffer<T2<KeyCacheObject, GridCacheVersion>> rmvQueue;

    /** Group reservations. */
    private final CopyOnWriteArrayList<GridDhtPartitionsReservation> reservations = new CopyOnWriteArrayList<>();

    /**
     * @param cctx Context.
     * @param id Partition ID.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    GridDhtLocalPartition(GridCacheContext cctx, int id) {
        assert cctx != null;

        this.id = id;
        this.cctx = cctx;

        log = U.logger(cctx.kernalContext(), logRef, this);

        rent = new GridFutureAdapter<Object>() {
            @Override public String toString() {
                return "PartitionRentFuture [part=" + GridDhtLocalPartition.this + ", map=" + map + ']';
            }
        };

        map = new ConcurrentHashMap8<>(cctx.config().getStartSize() /
            cctx.affinity().partitions());

        int delQueueSize = CU.isSystemCache(cctx.name()) ? 100 :
            Math.max(MAX_DELETE_QUEUE_SIZE / cctx.affinity().partitions(), 20);

        rmvQueue = new GridCircularBuffer<>(U.ceilPow2(delQueueSize));
    }

    /**
     * Adds group reservation to this partition.
     *
     * @param r Reservation.
     * @return {@code false} If such reservation already added.
     */
    public boolean addReservation(GridDhtPartitionsReservation r) {
        assert state.getReference() != EVICTED : "we can reserve only active partitions";
        assert state.getStamp() != 0 : "partition must be already reserved before adding group reservation";

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
        return state.getReference();
    }

    /**
     * @return Reservations.
     */
    public int reservations() {
        return state.getStamp();
    }

    /**
     * @return Keys belonging to partition.
     */
    public Set<KeyCacheObject> keySet() {
        return map.keySet();
    }

    /**
     * @return Entries belonging to partition.
     */
    public Collection<GridDhtCacheEntry> entries() {
        return map.values();
    }

    /**
     * @return {@code True} if partition is empty.
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * @return Number of entries in this partition (constant-time method).
     */
    public int size() {
        return map.size();
    }

    /**
     * Increments public size of the map.
     */
    public void incrementPublicSize() {
        mapPubSize.increment();
    }

    /**
     * Decrements public size of the map.
     */
    public void decrementPublicSize() {
        mapPubSize.decrement();
    }

    /**
     * @return Number of public (non-internal) entries in this partition.
     */
    public int publicSize() {
        return mapPubSize.intValue();
    }

    /**
     * @return If partition is moving or owning or renting.
     */
    public boolean valid() {
        GridDhtPartitionState state = state();

        return state == MOVING || state == OWNING || state == RENTING;
    }

    /**
     * @param entry Entry to add.
     */
    void onAdded(GridDhtCacheEntry entry) {
        GridDhtPartitionState state = state();

        if (state == EVICTED)
            throw new GridDhtInvalidPartitionException(id, "Adding entry to invalid partition [part=" + id + ']');

        map.put(entry.key(), entry);

        if (!entry.isInternal())
            mapPubSize.increment();
    }

    /**
     * @param entry Entry to remove.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    void onRemoved(GridDhtCacheEntry entry) {
        assert entry.obsolete();

        // Make sure to remove exactly this entry.
        synchronized (entry) {
            map.remove(entry.key(), entry);

            if (!entry.isInternal() && !entry.deleted())
                mapPubSize.decrement();
        }

        // Attempt to evict.
        tryEvict(true);
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
    @SuppressWarnings( {"LockAcquiredButNotSafelyReleased"})
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

        if (evictHist0 != null ) {
            GridCacheVersion ver0 = evictHist0.get(key);

            if (ver0 == null || ver0.isLess(ver)) {
                GridCacheVersion ver1  = evictHist0.put(key, ver);

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

        if (evictHist0 != null)  {
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
            int reservations = state.getStamp();

            GridDhtPartitionState s = state.getReference();

            if (s == EVICTED)
                return false;

            if (state.compareAndSet(s, s, reservations, reservations + 1))
                return true;
        }
    }

    /**
     * Releases previously reserved partition.
     */
    @Override public void release() {
        while (true) {
            int reservations = state.getStamp();

            if (reservations == 0)
                return;

            GridDhtPartitionState s = state.getReference();

            assert s != EVICTED;

            // Decrement reservations.
            if (state.compareAndSet(s, s, reservations, --reservations)) {
                tryEvict(true);

                break;
            }
        }
    }

    /**
     * @return {@code True} if transitioned to OWNING state.
     */
    boolean own() {
        while (true) {
            int reservations = state.getStamp();

            GridDhtPartitionState s = state.getReference();

            if (s == RENTING || s == EVICTED)
                return false;

            if (s == OWNING)
                return true;

            assert s == MOVING;

            if (state.compareAndSet(MOVING, OWNING, reservations, reservations)) {
                if (log.isDebugEnabled())
                    log.debug("Owned partition: " + this);

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
        while (true) {
            int reservations = state.getStamp();

            GridDhtPartitionState s = state.getReference();

            if (s == RENTING || s == EVICTED)
                return rent;

            if (state.compareAndSet(s, RENTING, reservations, reservations)) {
                if (log.isDebugEnabled())
                    log.debug("Moved partition to RENTING state: " + this);

                // Evict asynchronously, as the 'rent' method may be called
                // from within write locks on local partition.
                tryEvictAsync(updateSeq);

                break;
            }
        }

        return rent;
    }

    /**
     * @param updateSeq Update sequence.
     * @return Future for evict attempt.
     */
    IgniteInternalFuture<Boolean> tryEvictAsync(boolean updateSeq) {
        if (map.isEmpty() && !GridQueryProcessor.isEnabled(cctx.config()) &&
            state.compareAndSet(RENTING, EVICTED, 0, 0)) {
            if (log.isDebugEnabled())
                log.debug("Evicted partition: " + this);

            clearSwap();

            if (cctx.isDrEnabled())
                cctx.dr().partitionEvicted(id);

            cctx.dataStructures().onPartitionEvicted(id);

            rent.onDone();

            ((GridDhtPreloader)cctx.preloader()).onPartitionEvicted(this, updateSeq);

            clearDeferredDeletes();

            return new GridFinishedFuture<>(true);
        }

        return cctx.closures().callLocalSafe(new GPC<Boolean>() {
            @Override public Boolean call() {
                return tryEvict(true);
            }
        }, /*system pool*/ true);
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
     * @param updateSeq Update sequence.
     * @return {@code True} if entry has been transitioned to state EVICTED.
     */
    boolean tryEvict(boolean updateSeq) {
        if (state.getReference() != RENTING || state.getStamp() != 0 || groupReserved())
            return false;

        // Attempt to evict partition entries from cache.
        clearAll();

        if (map.isEmpty() && state.compareAndSet(RENTING, EVICTED, 0, 0)) {
            if (log.isDebugEnabled())
                log.debug("Evicted partition: " + this);

            if (!GridQueryProcessor.isEnabled(cctx.config()))
                clearSwap();

            if (cctx.isDrEnabled())
                cctx.dr().partitionEvicted(id);

            cctx.dataStructures().onPartitionEvicted(id);

            rent.onDone();

            ((GridDhtPreloader)cctx.preloader()).onPartitionEvicted(this, updateSeq);

            clearDeferredDeletes();

            return true;
        }

        return false;
    }

    /**
     * Clears swap entries for evicted partition.
     */
    private void clearSwap() {
        assert state() == EVICTED;
        assert !GridQueryProcessor.isEnabled(cctx.config()) : "Indexing needs to have unswapped values.";

        try {
            GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry>> it = cctx.swap().iterator(id);

            boolean isLocStore = cctx.store().isLocal();

            if (it != null) {
                // We can safely remove these values because no entries will be created for evicted partition.
                while (it.hasNext()) {
                    Map.Entry<byte[], GridCacheSwapEntry> entry = it.next();

                    byte[] keyBytes = entry.getKey();

                    KeyCacheObject key = cctx.toCacheKeyObject(keyBytes);

                    cctx.swap().remove(key);

                    if (isLocStore)
                        cctx.store().remove(null, key.value(cctx.cacheObjectContext(), false));
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to clear swap for evicted partition: " + this, e);
        }
    }

    /**
     *
     */
    void onUnlock() {
        tryEvict(true);
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if local node is primary for this partition.
     */
    public boolean primary(AffinityTopologyVersion topVer) {
        return cctx.affinity().primary(cctx.localNode(), id, topVer);
    }

    /**
     * Clears values for this partition.
     */
    private void clearAll() {
        GridCacheVersion clearVer = cctx.versions().next();

        boolean swap = cctx.isSwapOrOffheapEnabled();

        boolean rec = cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_UNLOADED);

        Iterator<GridDhtCacheEntry> it = map.values().iterator();

        GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry>> swapIt = null;

        if (swap && GridQueryProcessor.isEnabled(cctx.config())) { // Indexing needs to unswap cache values.
            Iterator<GridDhtCacheEntry> unswapIt = null;

            try {
                swapIt = cctx.swap().iterator(id);
                unswapIt = unswapIterator(swapIt);
            }
            catch (Exception e) {
                U.error(log, "Failed to clear swap for evicted partition: " + this, e);
            }

            if (unswapIt != null)
                it = F.concat(it, unswapIt);
        }

        try {
            while (it.hasNext()) {
                GridDhtCacheEntry cached = it.next();

                try {
                    if (cached.clearInternal(clearVer, swap)) {
                        map.remove(cached.key(), cached);

                        if (!cached.isInternal()) {
                            mapPubSize.decrement();

                            if (rec)
                                cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(),
                                    (IgniteUuid)null, null, EVT_CACHE_REBALANCE_OBJECT_UNLOADED, null, false,
                                    cached.rawGet(), cached.hasValue(), null, null, null);
                        }
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to clear cache entry for evicted partition: " + cached, e);
                }
            }
        }
        finally {
            U.close(swapIt, log);
        }
    }

    /**
     * @param it Swap iterator.
     * @return Unswapping iterator over swapped entries.
     */
    private Iterator<GridDhtCacheEntry> unswapIterator(
        final GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry>> it) {
        if (it == null)
            return null;

        return new Iterator<GridDhtCacheEntry>() {
            /** */
            GridDhtCacheEntry lastEntry;

            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public GridDhtCacheEntry next() {
                Map.Entry<byte[], GridCacheSwapEntry> entry = it.next();

                byte[] keyBytes = entry.getKey();

                try {
                    KeyCacheObject key = cctx.toCacheKeyObject(keyBytes);

                    lastEntry = (GridDhtCacheEntry)cctx.cache().entryEx(key, false);

                    lastEntry.unswap(true);

                    return lastEntry;
                }
                catch (IgniteCheckedException e) {
                    throw new CacheException(e);
                }
            }

            @Override public void remove() {
                map.remove(lastEntry.key(), lastEntry);
            }
        };
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
    @SuppressWarnings( {"OverlyStrongTypeCast"})
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
            "empty", map.isEmpty(),
            "createTime", U.format(createTime),
            "mapPubSize", mapPubSize);
    }
}