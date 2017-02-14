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
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

/**
 * Eagerly removes expired entries from cache when
 * {@link CacheConfiguration#isEagerTtl()} flag is set.
 */
@SuppressWarnings("NakedNotify")
public class GridCacheTtlManager extends GridCacheManagerAdapter {
    /** Entries pending removal. */
    private final GridConcurrentSkipListSetEx pendingEntries = new GridConcurrentSkipListSetEx();

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
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

        EntryWrapper e = new EntryWrapper(entry);

        pendingEntries.add(e);
    }

    /**
     * @param entry Entry to remove.
     */
    public void removeTrackedEntry(GridCacheMapEntry entry) {
        assert Thread.holdsLock(entry);

        pendingEntries.remove(new EntryWrapper(entry));
    }

    /**
     * @return The size of pending entries.
     */
    public int pendingSize() {
        return pendingEntries.sizex();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> TTL processor memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
        X.println(">>>   pendingEntriesSize: " + pendingEntries.size());
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
        long now = U.currentTimeMillis();

        GridCacheVersion obsoleteVer = null;

        int limit = (-1 != amount) ? amount : pendingEntries.sizex();

        for (int cnt = limit; cnt > 0; cnt--) {
            EntryWrapper e = pendingEntries.firstx();

            if (e == null || e.expireTime > now)
                return false; // All expired entries are processed.

            if (pendingEntries.remove(e)) {
                if (obsoleteVer == null)
                    obsoleteVer = cctx.versions().next();

                if (log.isTraceEnabled())
                    log.trace("Trying to remove expired entry from cache: " + e);

                boolean touch = e.ctx.isSwapOrOffheapEnabled();

                GridCacheEntryEx entry = touch ? e.ctx.cache().entryEx(e.key) : e.ctx.cache().peekEx(e.key);

                if (entry != null) {
                    while (true) {
                        try {
                            if (entry.onTtlExpired(obsoleteVer))
                                touch = false;

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignored) {
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
            EntryWrapper e = pendingEntries.firstx();

            return e != null && e.expireTime <= now;
        }

        return false;
    }

    /**
     * @param cctx1 First cache context.
     * @param key1 Left key to compare.
     * @param cctx2 Second cache context.
     * @param key2 Right key to compare.
     * @return Comparison result.
     */
    private static int compareKeys(GridCacheContext cctx1, CacheObject key1, GridCacheContext cctx2, CacheObject key2) {
        int key1Hash = key1.hashCode();
        int key2Hash = key2.hashCode();

        int res = Integer.compare(key1Hash, key2Hash);

        if (res == 0) {
            key1 = (CacheObject)cctx1.unwrapTemporary(key1);
            key2 = (CacheObject)cctx2.unwrapTemporary(key2);

            try {
                byte[] key1ValBytes = key1.valueBytes(cctx1.cacheObjectContext());
                byte[] key2ValBytes = key2.valueBytes(cctx2.cacheObjectContext());

                // Must not do fair array comparison.
                res = Integer.compare(key1ValBytes.length, key2ValBytes.length);

                if (res == 0) {
                    for (int i = 0; i < key1ValBytes.length; i++) {
                        res = Byte.compare(key1ValBytes[i], key2ValBytes[i]);

                        if (res != 0)
                            break;
                    }
                }

                if (res == 0)
                    res = Boolean.compare(cctx1.isNear(), cctx2.isNear());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        return res;
    }

    /**
     * Entry wrapper.
     */
    private static class EntryWrapper implements Comparable<EntryWrapper> {
        /** Entry expire time. */
        private final long expireTime;

        /** Cache Object Context */
        private final GridCacheContext ctx;

        /** Cache Object Key */
        private final KeyCacheObject key;

        /**
         * @param entry Cache entry to create wrapper for.
         */
        private EntryWrapper(GridCacheMapEntry entry) {
            expireTime = entry.expireTimeUnlocked();

            assert expireTime != 0;

            this.ctx = entry.context();
            this.key = entry.key();
        }

        /** {@inheritDoc} */
        @Override public int compareTo(EntryWrapper o) {
            int res = Long.compare(expireTime, o.expireTime);

            if (res == 0)
                res = compareKeys(ctx, key, o.ctx, o.key);

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof EntryWrapper))
                return false;

            EntryWrapper that = (EntryWrapper)o;

            return expireTime == that.expireTime && compareKeys(ctx, key, that.ctx, that.key) == 0;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = (int)(expireTime ^ (expireTime >>> 32));

            res = 31 * res + key.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EntryWrapper.class, this);
        }
    }

    /**
     * Provides additional method {@code #sizex()}. NOTE: Only the following methods supports this addition:
     * <ul>
     *     <li>{@code #add()}</li>
     *     <li>{@code #remove()}</li>
     *     <li>{@code #pollFirst()}</li>
     * <ul/>
     */
    private static class GridConcurrentSkipListSetEx extends GridConcurrentSkipListSet<EntryWrapper> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Size. */
        private final LongAdder8 size = new LongAdder8();

        /**
         * @return Size based on performed operations.
         */
        public int sizex() {
            return size.intValue();
        }

        /** {@inheritDoc} */
        @Override public boolean add(EntryWrapper e) {
            boolean res = super.add(e);

            if (res)
                size.increment();

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            boolean res = super.remove(o);

            if (res)
                size.decrement();

            return res;
        }

        /** {@inheritDoc} */
        @Nullable @Override public EntryWrapper pollFirst() {
            EntryWrapper e = super.pollFirst();

            if (e != null)
                size.decrement();

            return e;
        }
    }
}
