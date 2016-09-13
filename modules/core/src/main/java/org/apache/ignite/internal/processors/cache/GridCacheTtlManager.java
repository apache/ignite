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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
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
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentHashMap8;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Eagerly removes expired entries from cache when
 * {@link CacheConfiguration#isEagerTtl()} flag is set.
 */
@SuppressWarnings("NakedNotify")
public class GridCacheTtlManager extends GridCacheManagerAdapter {

    /** Pending entries pointer factory */
    private MyGridOffHeapSmartPointerFactory pointerFactory;

    /** Entries pending removal. */
    private GridOffHeapSnapTreeSet<EntryGridOffHeapSmartPointer> pendingPointers;

    /** Cleanup worker. */
    private CleanupWorker cleanupWorker;

    /** Mutex. */
    private final Object mux = new Object();

    /** Next expire time. */
    private volatile long nextExpireTime;

    /** Next expire time updater. */
    private static final AtomicLongFieldUpdater<GridCacheTtlManager> nextExpireTimeUpdater =
        AtomicLongFieldUpdater.newUpdater(GridCacheTtlManager.class, "nextExpireTime");

    /** Unsafe memory object for direct memory allocation. */
    private GridUnsafeMemory unsafeMemory;

    /** */
    private GridUnsafeGuard guard;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        unsafeMemory = new GridUnsafeMemory(0);

        guard = new GridUnsafeGuard();

        Marshaller marshaller = cctx.marshaller();

        assert marshaller != null;

        pointerFactory = new GridCacheTtlManager.MyGridOffHeapSmartPointerFactory(unsafeMemory, marshaller);

        pendingPointers = new GridOffHeapSnapTreeSet<>(pointerFactory, unsafeMemory, guard);

        boolean cleanupDisabled = cctx.kernalContext().isDaemon() ||
            !cctx.config().isEagerTtl() ||
            CU.isAtomicsCache(cctx.name()) ||
            CU.isMarshallerCache(cctx.name()) ||
            CU.isUtilityCache(cctx.name()) ||
            (cctx.kernalContext().clientNode() && cctx.config().getNearConfiguration() == null);

        if (cleanupDisabled)
            return;

        cleanupWorker = new CleanupWorker();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        if (cleanupWorker != null)
            new IgniteThread(cleanupWorker).start();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        U.cancel(cleanupWorker);
        U.join(cleanupWorker, log);
    }

    /**
     * Adds tracked entry to ttl processor.
     *
     * @param entry Entry to add.
     */
    public void addTrackedEntry(GridCacheMapEntry entry) {
        assert Thread.holdsLock(entry);
        assert cleanupWorker != null;

        PendingEntry e = new PendingEntry(entry);

        guard.begin();

        pendingPointers.add(pointerFactory.createPointer(e));

        guard.end();

        while (true) {
            long nextExpireTime = this.nextExpireTime;

            if (e.expireTime < nextExpireTime) {
                if (nextExpireTimeUpdater.compareAndSet(this, nextExpireTime, e.expireTime)) {
                    synchronized (mux) {
                        mux.notifyAll();
                    }

                    break;
                }
            }
            else
                break;
        }
    }

    /**
     * @param entry Entry to remove.
     */
    public void removeTrackedEntry(GridCacheMapEntry entry) {
        assert Thread.holdsLock(entry);
        assert cleanupWorker != null;

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
        long now = U.currentTimeMillis();

        GridCacheVersion obsoleteVer = null;

        for (int size = pendingPointers.size(); size > 0; size--) {
            PendingEntry pendingEntry;
            boolean entryRemoved;

            guard.begin();
            try {
                EntryGridOffHeapSmartPointer firstKey = pendingPointers.firstx();

                if (firstKey == null)
                    return; //Nothing to do

                pendingEntry = firstKey.entry();

                assert pendingEntry != null;

                if (pendingEntry.expireTime > now)
                    return; // entry is not expired

                entryRemoved = pendingPointers.remove(firstKey);
            }
            finally {
                guard.end();
            }

            if (entryRemoved) {
                if (obsoleteVer == null)
                    obsoleteVer = cctx.versions().next();

                GridCacheEntryEx entry = unwrapEntry(pendingEntry);

                assert entry != null;

                if (log.isTraceEnabled())
                    log.trace("Trying to remove expired entry from cache: " + entry);

                boolean touch = false;

                while (true) {
                    try {
                        if (entry.onTtlExpired(obsoleteVer))
                            touch = false;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        entry = entry.context().cache().entryEx(entry.key());

                        touch = true;
                    }
                }

                if (touch)
                    entry.context().evicts().touch(entry, null);
            }
        }
    }

    /**
     * Entry cleanup worker.
     */
    private class CleanupWorker extends GridWorker {
        /**
         * Creates cleanup worker.
         */
        CleanupWorker() {
            super(cctx.gridName(), "ttl-cleanup-worker-" + cctx.name(), cctx.logger(GridCacheTtlManager.class));
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                expire();

                while (true) {
                    long curTime = U.currentTimeMillis();

                    EntryGridOffHeapSmartPointer key0;
                    PendingEntry pendingEntry;

                    guard.begin();
                    try {
                        key0 = pendingPointers.firstx();

                        pendingEntry = (key0 == null) ? null : key0.entry();
                    }
                    finally {
                        guard.end();
                    }

                    long waitTime;
                    if (key0 == null) {
                        waitTime = 500;
                        nextExpireTime = curTime + 500;
                    }
                    else {
                        long expireTime = pendingEntry.expireTime;
                        waitTime = expireTime - curTime;
                        nextExpireTime = expireTime;
                    }

                    synchronized (mux) {
                        guard.begin();
                        try {
                            EntryGridOffHeapSmartPointer key1 = pendingPointers.firstx();

                            boolean firstEntryChanged = (key0 != key1) &&
                                (key0 == null || key0.pointer() != key1.pointer());

                            if (firstEntryChanged)
                                continue;
                        }
                        finally {
                            guard.end();
                        }

                        if (waitTime > 0)
                            mux.wait(waitTime);

                        break;
                    }
                }
            }
        }
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
        KeyCacheObject key;
        try {
            key = cctx.toCacheKeyObject(e.keyBytes);
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException(ex);
        }

        return cctx.cache().entryEx(key);
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
    private static class MyGridOffHeapSmartPointerFactory
        implements GridOffHeapSmartPointerFactory<EntryGridOffHeapSmartPointer> {

        /** */
        private final GridUnsafeMemory mem;

        /** */
        private final Marshaller marshaller;

        /**
         * @param mem Unsafe Memory.
         * @param marshaller Binary marshaller
         */
        MyGridOffHeapSmartPointerFactory(GridUnsafeMemory mem, Marshaller marshaller) {
            this.mem = mem;
            this.marshaller = marshaller;
        }

        /** {@inheritDoc} */
        @Override public EntryGridOffHeapSmartPointer createPointer(final long ptr) {
            return new MyGridOffHeapSmartPointer(ptr);
        }

        /**  */
        EntryGridOffHeapSmartPointer createPointer(PendingEntry entry) {
            return new MyGridOffHeapSmartPointer(entry);
        }

        /** */
        private class MyGridOffHeapSmartPointer implements EntryGridOffHeapSmartPointer,
            Comparable<MyGridOffHeapSmartPointer> {
            /** */
            private long ptr;

            /** */
            private PendingEntry entry;

            /** */
            @Override public PendingEntry entry() {
                long p = ptr;

                if (entry == null && p > 0) {
                    IgniteBiTuple<byte[], Byte> biTuple = mem.get(p);

                    try {
                        //TODO: replace with custom serialization to save some more memory
                        entry = marshaller.unmarshal(biTuple.get1(), null);
                    }
                    catch (IgniteCheckedException ex) {
                        throw new IgniteException(ex);
                    }
                }
                return entry;
            }

            /** */
            MyGridOffHeapSmartPointer(PendingEntry entry) {
                this.entry = entry;
            }

            /** */
            MyGridOffHeapSmartPointer(long ptr) {
                this.ptr = ptr;
            }

            /** {@inheritDoc} */
            @Override
            public int compareTo(@NotNull MyGridOffHeapSmartPointer o) {
                if (o == null)
                    return -1;

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
                long p = ptr;

                if (p == 0) {
                    try {
                        byte[] bytes = marshaller.marshal(entry);
                        p = mem.putOffHeap(0, bytes, GridBinaryMarshaller.BYTE_ARR);

                        ptr = p;
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            }

            /** {@inheritDoc} */
            @Override public void decrementRefCount() {
                final long p = ptr;

                if (p > 0) {
                    ptr = 0;

                    mem.removeOffHeap(p);
                }
            }

            /** {@inheritDoc} */
            @Override public boolean equals(Object o) {
                if (this == o)
                    return true;
                if (o == null || getClass() != o.getClass())
                    return false;

                MyGridOffHeapSmartPointer other = (MyGridOffHeapSmartPointer)o;

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
        }
    }
}