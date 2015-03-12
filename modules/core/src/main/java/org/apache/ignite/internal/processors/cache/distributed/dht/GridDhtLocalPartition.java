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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.*;

/**
 * Key partition.
 */
public class GridDhtLocalPartition implements Comparable<GridDhtLocalPartition> {
    /** Maximum size for delete queue. */
    private static final int MAX_DELETE_QUEUE_SIZE = Integer.getInteger(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE,
        200_000);

    /** Static logger to avoid re-creation. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static volatile IgniteLogger log;

    /** Partition ID. */
    private final int id;

    /** State. */
    @GridToStringExclude
    private AtomicStampedReference<GridDhtPartitionState> state =
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
    private final LongAdder mapPubSize = new LongAdder();

    /** Remove queue. */
    private GridCircularBuffer<T2<KeyCacheObject, GridCacheVersion>> rmvQueue;

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

        assert state != EVICTED : "Adding entry to invalid partition: " + this;

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
    public boolean reserve() {
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
    public void release() {
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
    private IgniteInternalFuture<Boolean> tryEvictAsync(boolean updateSeq) {
        if (map.isEmpty() && state.compareAndSet(RENTING, EVICTED, 0, 0)) {
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
     * @param updateSeq Update sequence.
     * @return {@code True} if entry has been transitioned to state EVICTED.
     */
    private boolean tryEvict(boolean updateSeq) {
        // Attempt to evict partition entries from cache.
        if (state.getReference() == RENTING && state.getStamp() == 0)
            clearAll();

        if (map.isEmpty() && state.compareAndSet(RENTING, EVICTED, 0, 0)) {
            if (log.isDebugEnabled())
                log.debug("Evicted partition: " + this);

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

        try {
            GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry>> it = cctx.swap().iterator(id);

            boolean isLocStore = cctx.store().isLocalStore();

            if (it != null) {
                // We can safely remove these values because no entries will be created for evicted partition.
                while (it.hasNext()) {
                    Map.Entry<byte[], GridCacheSwapEntry> entry = it.next();

                    byte[] keyBytes = entry.getKey();

                    KeyCacheObject key = cctx.toCacheKeyObject(keyBytes);

                    cctx.swap().remove(key);

                    if (isLocStore)
                        cctx.store().removeFromStore(null, key.value(cctx.cacheObjectContext(), false));
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
    public boolean primary(long topVer) {
        return cctx.affinity().primary(cctx.localNode(), id, topVer);
    }

    /**
     * Clears values for this partition.
     */
    private void clearAll() {
        GridCacheVersion clearVer = cctx.versions().next();

        boolean swap = cctx.isSwapOrOffheapEnabled();

        boolean rec = cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_UNLOADED);

        for (Iterator<GridDhtCacheEntry> it = map.values().iterator(); it.hasNext();) {
            GridDhtCacheEntry cached = it.next();

            try {
                if (cached.clearInternal(clearVer, swap)) {
                    it.remove();

                    if (!cached.isInternal()) {
                        mapPubSize.decrement();

                        if (rec)
                            cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(), (IgniteUuid)null,
                                null, EVT_CACHE_REBALANCE_OBJECT_UNLOADED, null, false, cached.rawGet(),
                                cached.hasValue(), null, null, null);
                    }
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to clear cache entry for evicted partition: " + cached, e);
            }
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
    @SuppressWarnings( {"OverlyStrongTypeCast"})
    @Override public boolean equals(Object obj) {
        return obj instanceof GridDhtLocalPartition && (obj == this || ((GridDhtLocalPartition)obj).id() == id);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull GridDhtLocalPartition part) {
        if (part == null)
            return 1;

        return id == part.id() ? 0 : id > part.id() ? 1 : -1;
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
