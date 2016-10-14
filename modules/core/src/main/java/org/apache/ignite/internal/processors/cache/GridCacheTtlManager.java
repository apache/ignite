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

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSmartPointer;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSmartPointerFactory;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeMap;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jsr166.LongAdder8;

/**
 * Eagerly removes expired m from cache when
 * {@link CacheConfiguration#isEagerTtl()} flag is set.
 */
public class GridCacheTtlManager extends GridCacheManagerAdapter {
    /** Entries pending removal. */
    private PendingEntriesQueue pendingEntries;

    /** */
    private GridUnsafeMemory unsafeMemory;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {

        if (cctx.isSwapOrOffheapEnabled()) {
            unsafeMemory = new GridUnsafeMemory(0);

            pendingEntries = new OffHeapPendingEntriesSet(unsafeMemory, new GridUnsafeGuard());

            if (log.isDebugEnabled())
                log.trace("Use OffHeap structure for ttl entries for cache: " + cctx.namex());
        }
        else {
            pendingEntries = new OnHeapPendingEntriesQueue();

            if (log.isDebugEnabled())
                log.trace("Use OnHeap structure for ttl entries for cache: " + cctx.namex());
        }

        boolean cleanupDisabled = cctx.kernalContext().isDaemon() ||
            !cctx.config().isEagerTtl() ||
            CU.isAtomicsCache(cctx.name()) ||
            CU.isMarshallerCache(cctx.name()) ||
            CU.isUtilityCache(cctx.name()) ||
            (cctx.kernalContext().clientNode() && cctx.config().getNearConfiguration() == null);

        if (cleanupDisabled)
            return;

        cctx.shared().ttl().register(this);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (pendingEntries != null)
            pendingEntries.clear();

        cctx.shared().ttl().unregister(this);
    }

    /**
     * Adds tracked entry to ttl processor.
     *
     * @param entry Entry to add.
     */
    public void addTrackedEntry(GridCacheMapEntry entry) {
        assert Thread.holdsLock(entry);

        assert pendingEntries != null;

        PendingEntry e = new PendingEntry(entry);

        pendingEntries.add(e);
    }

    /**
     * @param entry Entry to remove.
     */
    public void removeTrackedEntry(GridCacheMapEntry entry) {
        assert Thread.holdsLock(entry);

        assert pendingEntries != null;

        PendingEntry e = new PendingEntry(entry);

        pendingEntries.remove(e);
    }

    /**
     * @return The size of pending m.
     */
    public int pendingSize() {
        return pendingEntries.size();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> TTL processor memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
        X.println(">>>   pendingEntriesSize: " + pendingEntries.size());
        if (unsafeMemory != null)
            X.println(">>>   OffHeap memory allocated size: " + unsafeMemory.allocatedSize());
    }

    /**
     * Expires m by TTL.
     */
    public void expire() {
        expire(-1);
    }

    /**
     * Processes specified amount of expired m.
     *
     * @param amount Limit of processed m by single call, {@code -1} for no limit.
     * @return {@code True} if unprocessed expired m remains.
     */
    public boolean expire(int amount) {
        if (pendingEntries == null)
            return false;

        long now = U.currentTimeMillis();

        GridCacheVersion obsoleteVer = null;

        int limit = (-1 != amount) ? amount : pendingEntries.size();

        for (int cnt = limit; cnt > 0; cnt--) {
            PendingEntry pendingEntry = pendingEntries.pollExpiredFor(now);

            if (pendingEntry == null)
                return false;

            if (obsoleteVer == null)
                obsoleteVer = cctx.versions().next();

            // Offheap pendingEntry is deserialized already
            GridCacheEntryEx entry = unwrapEntry(pendingEntry);

            if (entry != null) {
                if (log.isTraceEnabled())
                    log.trace("Trying to remove expired entry from cache: " + entry);

                boolean touch = entry.context().isSwapOrOffheapEnabled();

                while (true) {
                    try {
                        if (entry.onTtlExpired(obsoleteVer))
                            touch = false;

                        break;
                    }
                    catch (GridCacheEntryRemovedException e0) {
                        entry = entry.context().cache().entryEx(entry.key());

                        touch = true;
                    }
                }

                if (touch)
                    entry.context().evicts().touch(entry, null);
            }
        }

        return amount != -1 && pendingEntries.hasExpiredFor(now);

    }

    /**
     * @param arr1 first array
     * @param arr2 second array
     * @return Comparison result.
     */
    private static int compareArrays(byte[] arr1, byte[] arr2) {
        // Must not do fair array comparison.
        int res = Integer.compare(arr1.length, arr2.length);

        if (res == 0) {
            for (int i = 0; i < arr1.length; i++) {
                res = Byte.compare(arr1[i], arr2[i]);

                if (res != 0)
                    break;
            }
        }
        return res;
    }

    /**
     * @return GridCacheEntry
     */
    private GridCacheEntryEx unwrapEntry(PendingEntry e) {
        GridCacheAdapter cache = cctx.cache();

        //Here we need to assign appropriate context to entry
        if (e.isNear)
            cache = cache.isDht() ? ((GridDhtCacheAdapter)cache).near() : cache;
        else
            cache = cache.isNear() ? ((GridNearCacheAdapter)cache).dht() : cache;

        KeyCacheObject key;
        try {
            key = cache.context().toCacheKeyObject(e.keyBytes);
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException(ex);
        }

        return cache.ctx.isSwapOrOffheapEnabled() ? cache.entryEx(key) : cache.peekEx(key);
    }

    /**
     * Pending entry.
     */
    private static final class PendingEntry implements Comparable<PendingEntry> {
        /** Entry expire time. */
        private final long expireTime;

        /** Cache Object Serialized Key */
        private final byte[] keyBytes;

        /** Cached hash code */
        private final int hashCode;

        /** Is near cache entry flag */
        private final boolean isNear;

        /**
         * Constructor
         */
        PendingEntry(long expireTime, int hashCode, boolean isNear, byte[] keyBytes) {
            this.expireTime = expireTime;
            this.keyBytes = keyBytes;
            this.hashCode = hashCode;
            this.isNear = isNear;
        }

        /**
         * @param entry Cache entry to create wrapper for.
         */
        PendingEntry(GridCacheEntryEx entry) {
            expireTime = entry.expireTimeUnlocked();

            assert expireTime != 0;

            GridCacheContext ctx = entry.context();

            isNear = entry.isNear();

            CacheObject key = entry.key();

            hashCode = key.hashCode();

            key = (CacheObject)ctx.unwrapTemporary(key);

            assert key != null;

            try {
                keyBytes = key.valueBytes(ctx.cacheObjectContext());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull PendingEntry o) {
            int res = Long.compare(expireTime, o.expireTime);

            if (res == 0)
                res = Integer.compare(hashCode, o.hashCode);

            if (res == 0)
                res = Boolean.compare(isNear, o.isNear);

            if (res == 0)
                res = compareArrays(keyBytes, o.keyBytes);

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof PendingEntry))
                return false;

            PendingEntry that = (PendingEntry)o;

            return compareTo(that) == 0;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hashCode;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PendingEntry.class, this);
        }
    }

    /**
     * Key SmartPointer factory
     */
    private static class PendingEntrySmartPointerFactory
        implements GridOffHeapSmartPointerFactory<PendingEntrySmartPointer> {
        /** */
        private final GridUnsafeMemory mem;

        /**
         * @param mem Unsafe Memory.
         */
        PendingEntrySmartPointerFactory(GridUnsafeMemory mem) {
            this.mem = mem;
        }

        /** {@inheritDoc} */
        @Override public PendingEntrySmartPointer createPointer(final long ptr) {
            return new PendingEntrySmartPointer(mem, ptr);
        }

        /**  */
        PendingEntrySmartPointer createPointer(PendingEntry entry) {
            return new PendingEntrySmartPointer(mem, entry);
        }
    }

    /** */
    private static class PendingEntrySmartPointer implements GridOffHeapSmartPointer,
        Comparable<PendingEntrySmartPointer> {
        /** empty value */
        private static final byte[] EMPTY_BYTES = new byte[0];

        /** */
        private static final int OFFSET_REF_COUNT = 4;

        /** */
        private static final int OFFSET_EXPIRE_TIME = OFFSET_REF_COUNT + 4;

        /** */
        private static final int OFFSET_HASHCODE = OFFSET_EXPIRE_TIME + 8;

        /** */
        private static final int OFFSET_IS_NEAR = OFFSET_HASHCODE + 4;

        /** */
        private static final int OFFSET_KEY_BYTES = OFFSET_IS_NEAR + 1;

        /** */
        private final GridUnsafeMemory mem;

        /** */
        private long ptr;

        /** Hot field, actively accessed by comparator in tree operations */
        private long expiretime = 0L;

        /** */
        private PendingEntry entry;

        /** Constructor */
        PendingEntrySmartPointer(GridUnsafeMemory mem, PendingEntry entry) {
            this.mem = mem;
            this.entry = entry;
        }

        /** Constructor */
        PendingEntrySmartPointer(GridUnsafeMemory mem, long ptr) {
            this.mem = mem;
            this.ptr = ptr;
        }

        /** */
        public long expireTime() {
            if (entry != null)
                return entry.expireTime;

            if (expiretime != 0L)
                return expiretime;

            if (ptr > 0) {
                expiretime = mem.readLong(ptr + OFFSET_EXPIRE_TIME);
                return expiretime;
            }

            throw new IllegalStateException();
        }

        /** */
        public byte[] keyBytes() {
            if (entry != null)
                return entry.keyBytes;

            if (ptr > 0) {
                int recordSize = mem.readInt(ptr);

                return recordSize > OFFSET_KEY_BYTES ?
                    mem.readBytes(ptr + OFFSET_KEY_BYTES, recordSize - OFFSET_KEY_BYTES) : EMPTY_BYTES;
            }

            throw new IllegalStateException();
        }

        /** */
        public boolean isNear() {
            if (entry != null)
                return entry.isNear;

            if (ptr > 0)
                return mem.readByte(ptr + OFFSET_IS_NEAR) == 1;

            throw new IllegalStateException();
        }

        /** Deserialize full entry */
        public PendingEntry entry() {
            if (entry != null)
                return entry;

            if (ptr > 0) {
                int recordSize = mem.readIntVolatile(ptr);

                long expireTime = mem.readLong(ptr + OFFSET_EXPIRE_TIME);

                int hash = mem.readInt(ptr + OFFSET_HASHCODE);

                boolean isNear = mem.readByte(ptr + OFFSET_IS_NEAR) == 1;

                byte[] bytes = recordSize > OFFSET_KEY_BYTES ?
                    mem.readBytes(ptr + OFFSET_KEY_BYTES, recordSize - OFFSET_KEY_BYTES) : EMPTY_BYTES;

                entry = new PendingEntry(expireTime, hash, isNear, bytes);
            }
            return entry;
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(@NotNull PendingEntrySmartPointer o) {
            assert o != null;

            if (ptr > 0 && ptr == o.ptr)
                return 0;

            int res = Long.compare(expireTime(), o.expireTime());

            if (res == 0)
                res = Integer.compare(hashCode(), o.hashCode());

            if (res == 0)
                res = Boolean.compare(isNear(), o.isNear());

            if (res == 0)
                res = compareArrays(keyBytes(), o.keyBytes());

            return res;
        }

        /** {@inheritDoc} */
        @Override public long pointer() {
            return ptr;
        }

        /** {@inheritDoc} */
        @Override public void incrementRefCount() {
            final long p = ptr;

            if (p == 0) {
                int recordSize = OFFSET_KEY_BYTES + entry.keyBytes.length;

                assert recordSize > OFFSET_KEY_BYTES;

                final long res = mem.allocate(recordSize, true);

                mem.writeInt(res + OFFSET_REF_COUNT, 1); // Initial ref count

                mem.writeLong(res + OFFSET_EXPIRE_TIME, entry.expireTime);

                mem.writeInt(res + OFFSET_HASHCODE, entry.hashCode);

                mem.writeByte(res + OFFSET_IS_NEAR, (byte)(entry.isNear ? 1 : 0));

                mem.writeBytes(res + OFFSET_KEY_BYTES, entry.keyBytes);

                mem.writeIntVolatile(res, recordSize);

                ptr = res;
            }
            else {
                final long refCountPtr = this.ptr + OFFSET_REF_COUNT;

                int refCount;

                do {
                    refCount = mem.readIntVolatile(refCountPtr);

                    assert refCount > 0;
                }
                while (!mem.casInt(refCountPtr, refCount, refCount + 1));
            }
        }

        /** {@inheritDoc} */
        @Override public void decrementRefCount() {
            assert ptr > 0;

            final long refCountPtr = ptr + OFFSET_REF_COUNT;

            int refCount;

            for (; ; ) {
                refCount = mem.readIntVolatile(refCountPtr);

                assert refCount > 0;

                if (refCount == 1) // no other pointers exists
                    break;

                if (mem.casInt(refCountPtr, refCount, refCount - 1))
                    return;
            }

            int size = mem.readInt(ptr);

            mem.release(ptr, size);

            ptr = 0;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            if (entry != null)
                return entry.hashCode;

            if (ptr > 0)
                return mem.readInt(ptr + OFFSET_HASHCODE);

            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PendingEntrySmartPointer [ptr=" + ptr + ']';
        }
    }

    /** Pending m collection facade */
    private interface PendingEntriesQueue {
        /**
         * Add entry to queue
         *
         * @param e pending entry
         */
        void add(PendingEntry e);

        /**
         * Remove entry from queue
         *
         * @param e pending entry
         */
        void remove(PendingEntry e);

        /** Clear queue */
        void clear();

        /**
         * @return Size based on performed operations.
         */
        int size();

        /**
         * @return @{true} if there is expired entry in collection, otherwise @{false}.
         */
        boolean hasExpiredFor(long now);

        /**
         * @return first expired entry end remove it from collection.
         */
        PendingEntry pollExpiredFor(long now);
    }

    /** */
    private static class OnHeapPendingEntriesQueue implements PendingEntriesQueue {
        /** */
        private final ConcurrentSkipListMap<PendingEntry, Boolean> m;

        /** Size. */
        private final LongAdder8 size = new LongAdder8();

        OnHeapPendingEntriesQueue() {
            this.m = new ConcurrentSkipListMap<>();
        }

        /** {@inheritDoc} */
        @Override
        public int size() {
            return size.intValue();
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            m.clear();
            size.reset();
        }

        /** {@inheritDoc} */
        @Override public void add(PendingEntry e) {
            boolean res = m.put(e, Boolean.TRUE) == null;

            if (res)
                size.increment();
        }

        /** {@inheritDoc} */
        @Override public void remove(PendingEntry o) {
            boolean res = m.remove(o) != null;

            if (res)
                size.decrement();
        }

        /** {@inheritDoc} */
        @Override public boolean hasExpiredFor(long now) {
            Map.Entry<PendingEntry, Boolean> entry = m.firstEntry();

            return entry != null && now >= entry.getKey().expireTime;
        }

        /** {@inheritDoc} */
        @Override public PendingEntry pollExpiredFor(long now) {
            Map.Entry<PendingEntry, Boolean> firstEntry;

            while ((firstEntry = m.firstEntry()) != null) {
                if (firstEntry.getValue() == null)
                    continue; // Entry is removing by another thread

                PendingEntry pendingEntry = firstEntry.getKey();

                if (pendingEntry.expireTime > now)
                    return null; // entry is not expired

                if (m.remove(pendingEntry) == null)
                    continue; // Entry was polled by another thread

                size.decrement();

                return pendingEntry;
            }

            return null;
        }
    }

    /** */
    private static class OffHeapPendingEntriesSet implements PendingEntriesQueue {
        /** */
        private static final DummySmartPointer DUMMY_SMART_POINTER = new DummySmartPointer(Long.MAX_VALUE);

        /** */
        private final GridOffHeapSnapTreeMap<PendingEntrySmartPointer, DummySmartPointer> m;

        /** */
        private PendingEntrySmartPointerFactory pointerFactory;

        /** */
        private GridUnsafeGuard guard;

        /**
         * Default constructor.
         */
        OffHeapPendingEntriesSet(GridUnsafeMemory mem, GridUnsafeGuard guard) {
            this.pointerFactory = new PendingEntrySmartPointerFactory(mem);

            this.guard = guard;

            m = new GridOffHeapSnapTreeMap<>(this.pointerFactory, new ValueSmartPointerFactory(), mem, this.guard);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return m.size();
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            guard.begin();

            try {
                /* GridOffHeapSnapTreeMap does not support clear operation */
                for (PendingEntrySmartPointer ptr : m.keySet())
                    m.remove(ptr);
            }
            finally {
                guard.end();
            }
        }

        /** {@inheritDoc} */
        @Override public void add(PendingEntry e) {
            guard.begin();

            try {
                m.put(pointerFactory.createPointer(e), DUMMY_SMART_POINTER);
            }
            finally {
                guard.end();
            }
        }

        /** {@inheritDoc} */
        @Override public void remove(PendingEntry e) {
            guard.begin();

            try {
                m.remove(pointerFactory.createPointer(e));
            }
            finally {
                guard.end();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean hasExpiredFor(long now) {
            Map.Entry<PendingEntrySmartPointer, DummySmartPointer> e = m.firstEntry();

            return e != null && now >= e.getKey().expireTime();
        }

        /** {@inheritDoc} */
        @Override public PendingEntry pollExpiredFor(long now) {
            guard.begin();

            try {
                Map.Entry<PendingEntrySmartPointer, DummySmartPointer> entry;

                while ((entry = m.firstEntry()) != null) {

                    if (entry.getValue() == null)
                        continue; // Entry is removing by another thread

                    PendingEntrySmartPointer firstKey = entry.getKey();

                    if (firstKey.expireTime() > now)
                        return null; // entry is not expired

                    if (m.remove(firstKey) == null)
                        continue; // Entry was polled by another thread

                    // deserialize removed entry, it's possible under guard
                    return firstKey.entry();
                }
            }
            finally {
                guard.end();
            }

            return null;
        }

        /** Value SmartPointer factory */
        private static class ValueSmartPointerFactory implements GridOffHeapSmartPointerFactory {
            /** {@inheritDoc} */
            @Override public GridOffHeapSmartPointer createPointer(final long ptr) {
                assert ptr == Long.MAX_VALUE;
                return DUMMY_SMART_POINTER;
            }
        }

        /** Dummy smart pointer */
        private static class DummySmartPointer implements GridOffHeapSmartPointer {
            /** */
            private final long ptr;

            /**
             * @param ptr Unsafe memory pointer.
             */
            DummySmartPointer(long ptr) {
                this.ptr = ptr;
            }

            /** {@inheritDoc} */
            @Override public long pointer() {
                return ptr;
            }

            /** {@inheritDoc} */
            @Override public void incrementRefCount() {
            }

            /** {@inheritDoc} */
            @Override public void decrementRefCount() {
            }
        }
    }

}