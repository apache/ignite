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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSmartPointer;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSmartPointerFactory;
import org.apache.ignite.internal.util.offheap.unsafe.GridOffHeapSnapTreeSet;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeGuard;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Eagerly removes expired entries from cache when
 * {@link CacheConfiguration#isEagerTtl()} flag is set.
 */
@SuppressWarnings("NakedNotify")
public class GridCacheTtlManager extends GridCacheManagerAdapter {
    /** Pending entries pointer factory */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private PendingEntrySmartPointerFactory pointerFactory;

    /** Entries pending removal. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private GridOffHeapSnapTreeSet<EntryGridOffHeapSmartPointer> pendingPointers;

    /** Unsafe memory object for direct memory allocation. */
    private GridUnsafeMemory unsafeMemory;

    /** */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private GridUnsafeGuard guard;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        unsafeMemory = new GridUnsafeMemory(0);

        guard = new GridUnsafeGuard();

        pointerFactory = new PendingEntrySmartPointerFactory(unsafeMemory);

        pendingPointers = new GridOffHeapSnapTreeSet<EntryGridOffHeapSmartPointer>(pointerFactory, unsafeMemory, guard);

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
        if (pendingPointers != null) {
            guard.begin();

            try {
                pendingPointers.clear();
            }
            finally {
                guard.end();
            }
        }

        cctx.shared().ttl().unregister(this);
    }

    /**
     * Adds tracked entry to ttl processor.
     *
     * @param entry Entry to add.
     */
    public void addTrackedEntry(GridCacheMapEntry entry) {
        assert Thread.holdsLock(entry);

        PendingEntry e = new PendingEntry(entry);

        guard.begin();

        try {
            pendingPointers.add(pointerFactory.createPointer(e));
        }
        finally {
            guard.end();
        }
    }

    /**
     * @param entry Entry to remove.
     */
    public void removeTrackedEntry(GridCacheMapEntry entry) {
        assert Thread.holdsLock(entry);

        PendingEntry e = new PendingEntry(entry);

        guard.begin();

        try {
            pendingPointers.remove(pointerFactory.createPointer(e));
        }
        finally {
            guard.end();
        }
    }

    /**
     * @return The size of pending entries.
     */
    public int pendingSize() {
        return pendingPointers.size();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> TTL processor memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
        X.println(">>>   pendingEntriesSize: " + pendingPointers.size());
        X.println(">>>   OffHeap memory allocated size: " + unsafeMemory.allocatedSize());
    }

    /**
     * Expires entries by TTL.
     */
    public void expire() {
        expire(-1);
    }

    /**
     * Processes specified amount of expired entries.
     *
     * @param amount Limit of processed entries by single call, {@code -1} for no limit.
     * @return {@code True} if unprocessed expired entries remains.
     */
    public boolean expire(int amount) {
        if (pendingPointers == null)
            return false;

        long now = U.currentTimeMillis();

        GridCacheVersion obsoleteVer = null;

        int limit = (-1 != amount) ? amount : pendingPointers.size();

        for (int cnt = limit; cnt > 0; cnt--) {
            PendingEntry pendingEntry;
            boolean entryRemoved;

            guard.begin();
            try {
                EntryGridOffHeapSmartPointer firstKey = pendingPointers.firstx();

                if (firstKey == null)
                    return false; //Nothing to do

                pendingEntry = firstKey.entry();

                if (pendingEntry != null && pendingEntry.expireTime > now)
                    return false; // entry is not expired

                entryRemoved = pendingPointers.remove(firstKey);
            }
            finally {
                guard.end();
            }

            if (entryRemoved) {
                if (obsoleteVer == null)
                    obsoleteVer = cctx.versions().next();

                boolean touch = cctx.isSwapOrOffheapEnabled();

                GridCacheEntryEx entry = unwrapEntry(pendingEntry, touch);

                if (log.isTraceEnabled())
                    log.trace("Trying to remove expired entry from cache: " + entry);

                if (entry != null) {
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
        }

        if (amount != -1) {
            EntryGridOffHeapSmartPointer pointer = pendingPointers.firstx();

            return pointer != null && pointer.entry() != null && pointer.entry().expireTime <= now;
        }

        return false;
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
    private GridCacheEntryEx unwrapEntry(PendingEntry e, boolean touch) {
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

        return touch ? cctx.cache().entryEx(key) : cctx.cache().peekEx(key);
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

        /** */
        private final boolean isNear;

        /**
         * Constructor
         */
        private PendingEntry(long expireTime, int hashCode, boolean isNear, byte[] keyBytes) {
            this.expireTime = expireTime;
            this.keyBytes = keyBytes;
            this.hashCode = hashCode;
            this.isNear = isNear;
        }

        /**
         * @param entry Cache entry to create wrapper for.
         */
        private PendingEntry(GridCacheEntryEx entry) {
            expireTime = entry.expireTimeUnlocked();

            assert expireTime != 0;

            GridCacheContext ctx = entry.context();

            isNear = ctx.isNear();

            CacheObject key = entry.key();

            hashCode = hashCode0(key.hashCode());

            key = (CacheObject)ctx.unwrapTemporary(key);

            assert key != null;

            try {
                keyBytes = key.valueBytes(ctx.cacheObjectContext());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * Pre-compute hashcode
         *
         * @param keyHashCode key hashcode
         * @return entry hashcode
         */
        private int hashCode0(int keyHashCode) {
            int res = (int)(expireTime ^ (expireTime >>> 32));

            res = 31 * res + keyHashCode;
            return res;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull PendingEntry o) {
            int res = Long.compare(expireTime, o.expireTime);

            if (res == 0)
                res = Integer.compare(hashCode, o.hashCode);

            if (res == 0)
                res = compareArrays(keyBytes, o.keyBytes);

            if (res == 0)
                res = Boolean.compare(isNear, o.isNear);

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
     * SmartPointer for Entry
     */
    private interface EntryGridOffHeapSmartPointer extends GridOffHeapSmartPointer {
        /** */
        PendingEntry entry();
    }

    /**
     * Key SmartPointer factory
     */
    private static class PendingEntrySmartPointerFactory
        implements GridOffHeapSmartPointerFactory<EntryGridOffHeapSmartPointer> {

        /** */
        private final GridUnsafeMemory mem;

        /**
         * @param mem Unsafe Memory.
         */
        PendingEntrySmartPointerFactory(GridUnsafeMemory mem) {
            this.mem = mem;
        }

        /** {@inheritDoc} */
        @Override public EntryGridOffHeapSmartPointer createPointer(final long ptr) {
            return new PendingEntrySmartPointer(ptr);
        }

        /**  */
        EntryGridOffHeapSmartPointer createPointer(PendingEntry entry) {
            return new PendingEntrySmartPointer(entry);
        }

        /** */
        private class PendingEntrySmartPointer implements EntryGridOffHeapSmartPointer,
            Comparable<PendingEntrySmartPointer> {
            /** */
            private long ptr;

            /** */
            private PendingEntry entry;

            /** */
            @Override public PendingEntry entry() {
                if (entry != null)
                    return entry;

                long p = ptr;

                if (p > 0) {
                    int recordSize = mem.readIntVolatile(p);
                    p += 4;

                    long expireTime = mem.readLong(p);
                    p += 8;

                    int hash = mem.readInt(p);
                    p += 4;

                    boolean isNear = mem.readByte(p) == 1;
                    p += 1;

                    byte[] bytes = recordSize > 17 ? mem.readBytes(p, recordSize - 17) : new byte[0];

                    entry = new PendingEntry(expireTime, hash, isNear, bytes);
                }
                return entry;
            }

            /** */
            PendingEntrySmartPointer(PendingEntry entry) {
                this.entry = entry;
            }

            /** */
            PendingEntrySmartPointer(long ptr) {
                this.ptr = ptr;
            }

            /** {@inheritDoc} */
            @Override
            public int compareTo(@NotNull PendingEntrySmartPointer o) {
                assert o != null;

                if (ptr > 0 && ptr == o.ptr)
                    return 0;

                PendingEntry e1 = entry();
                PendingEntry e2 = o.entry();

                return e1.compareTo(e2);
            }

            /** {@inheritDoc} */
            @Override public long pointer() {
                return ptr;
            }

            /** {@inheritDoc} */
            @Override public void incrementRefCount() {
                final long p = ptr;

                if (p == 0) {
                    int recordSize = 17 + entry.keyBytes.length;

                    assert recordSize > 17;

                    final long res = mem.allocate(recordSize, true);

                    long p0 = res + 4;

                    mem.writeLong(p0, entry.expireTime);
                    p0 += 8;

                    mem.writeInt(p0, entry.hashCode);
                    p0 += 4;

                    mem.writeByte(p0, (byte)(entry.isNear ? 1 : 0));
                    p0 += 1;

                    mem.writeBytes(p0, entry.keyBytes);

                    mem.writeIntVolatile(res, recordSize);

                    ptr = res;
                }
            }

            /** {@inheritDoc} */
            @Override public void decrementRefCount() {
                final long p = ptr;

                if (p > 0) {
                    int size = mem.readIntVolatile(p);
                    mem.release(p, size);
                }
            }

            /** {@inheritDoc} */
            @Override public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;

                PendingEntrySmartPointer other = (PendingEntrySmartPointer)o;

                long ptr = this.ptr;

                if (ptr > 0 && ptr == other.ptr)
                    return true;

                PendingEntry entry0 = entry();
                PendingEntry entry1 = other.entry();

                return entry0 != null && entry0.equals(entry1);

            }

            /** {@inheritDoc} */
            @Override public int hashCode() {
                return entry().hashCode();
            }

            /** {@inheritDoc} */
            @Override public String toString() {
                return "PendingEntrySmartPointer{ptr=" + ptr + '}';
            }
        }
    }
}