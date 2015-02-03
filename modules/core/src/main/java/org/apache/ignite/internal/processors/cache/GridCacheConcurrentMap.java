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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.ref.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.internal.processors.cache.CacheFlag.*;

/**
 * Concurrent implementation of cache map.
 */
public class GridCacheConcurrentMap<K, V> {
    /** Debug flag. */
    private static final boolean DEBUG = false;

    /** Random. */
    private static final Random RAND = new Random();

    /** The default load factor for this map. */
    private static final float DFLT_LOAD_FACTOR = 0.75f;

    /** The default concurrency level for this map. */
    private static final int DFLT_CONCUR_LEVEL = 2048;

    /**
     * The maximum capacity, used if a higher value is implicitly specified by either
     * of the constructors with arguments. Must be a power of two <= 1<<30 to ensure
     * that entries are indexable using integers.
     */
    private static final int MAX_CAP = 1 << 30;

    /** The maximum number of segments to allow. */
    private static final int MAX_SEGS = 1 << 16; // slightly conservative

    /**
     * Mask value for indexing into segments. The upper bits of a
     * key's hash code are used to choose the segment.
     */
    private final int segMask;

    /** Shift value for indexing within segments. */
    private final int segShift;

    /** The segments, each of which is a specialized hash table. */
    private final Segment[] segs;

    /** */
    private GridCacheMapEntryFactory<K, V> factory;

    /** Cache context. */
    protected final GridCacheContext<K, V> ctx;

    /** */
    private final LongAdder mapPubSize = new LongAdder();

    /** */
    private final LongAdder mapSize = new LongAdder();

    /** Filters cache internal entry. */
    private static final P1<CacheEntry<?, ?>> NON_INTERNAL =
        new P1<CacheEntry<?, ?>>() {
            @Override public boolean apply(CacheEntry<?, ?> entry) {
                return !(entry.getKey() instanceof GridCacheInternal);
            }
        };

    /** Non-internal predicate array. */
    public static final IgnitePredicate[] NON_INTERNAL_ARR = new P1[] {NON_INTERNAL};

    /** Filters obsolete cache map entry. */
    private final IgnitePredicate<GridCacheMapEntry<K, V>> obsolete =
        new P1<GridCacheMapEntry<K, V>>() {
            @Override public boolean apply(GridCacheMapEntry<K, V> entry) {
                return entry.obsolete();
            }
        };

    /** Soft iterator queue. */
    private final ReferenceQueue<Iterator0<K, V>> itQ = new ReferenceQueue<>();

    /** Soft iterator set. */
    private final Map<WeakIterator<K, V>, SegmentHeader<K, V>> itMap =
        new ConcurrentHashMap8<>();

    /**
     * Checks phantom queue.
     */
    private void checkWeakQueue() {
        // If queue is empty, then it is a lock-free volatile read which should happen
        // in most cases. Otherwise queue uses synchronization to poll elements.
        for (Reference<? extends Iterator0<K, V>> itRef = itQ.poll(); itRef != null; itRef = itQ.poll()) {
            assert itRef instanceof WeakIterator;

            if (DEBUG)
                X.println("Checking weak queue [itSetSize=" + itMap.size() + ']');

            SegmentHeader<K, V> lastSeg = removeWeakIterator(itRef);

            // Segment may be null if iterator is empty at creation time.
            if (lastSeg != null)
                lastSeg.onReadEnd();
        }
    }

    /**
     * @param itRef Iterator reference.
     * @return Last segment.
     */
    private SegmentHeader<K, V> removeWeakIterator(Reference<? extends Iterator0<K, V>> itRef) {
        assert itRef instanceof WeakIterator;

        SegmentHeader<K, V> hdr = itMap.remove(itRef);

        if (DEBUG) {
            if (hdr == null)
                X.println("Removed non-existent iterator: " + itRef);
            else
                X.println("Removed iterator [hdrId=" + hdr.id() + ", it=" + itRef + ", mapSize=" + itMap.size() + ']');
        }

        return hdr;
    }

    /**
     * @param itRef Iterator reference.
     * @param hdr Segment header.
     */
    private void addWeakIterator(WeakIterator<K, V> itRef, SegmentHeader<K, V> hdr) {
        SegmentHeader<K, V> prev = itMap.put(itRef, hdr);

        if (DEBUG)
            if (prev == null)
                X.println("Added weak reference: " + itMap.size());
    }



    /**
     * @return Iterator set size.
     */
    int iteratorMapSize() {
        return itMap.size();
    }

    /**
     * @return Reference queue for iterators.
     */
    private ReferenceQueue<Iterator0<K, V>> iteratorQueue() {
        return itQ;
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     * <p>
     * This function has been taken from Java 8 ConcurrentHashMap with
     * slightly modifications.
     *
     * @param h Value to hash.
     * @return Hash value.
     */
    protected static int hash(int h) {
        return U.hash(h);
    }

    /**
     * Returns the segment that should be used for key with given hash
     *
     * @param hash The hash code for the key.
     * @return The segment.
     */
    private Segment segmentFor(int hash) {
        return segs[(hash >>> segShift) & segMask];
    }

    /**
     * Creates a new, empty map with the specified initial
     * capacity, load factor and concurrency level.
     *
     * @param ctx Cache context.
     * @param initCap the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @param concurrencyLevel the estimated number of concurrently
     *      updating threads. The implementation performs internal sizing
     *      to try to accommodate this many threads.
     * @throws IllegalArgumentException if the initial capacity is
     *      negative or the load factor or concurrencyLevel are
     *      non-positive.
     */
    @SuppressWarnings({"unchecked"})
    protected GridCacheConcurrentMap(GridCacheContext<K, V> ctx, int initCap, float loadFactor,
        int concurrencyLevel) {
        this.ctx = ctx;

        if (!(loadFactor > 0) || initCap < 0 || concurrencyLevel <= 0)
            throw new IllegalArgumentException();

        if (concurrencyLevel > MAX_SEGS)
            concurrencyLevel = MAX_SEGS;

        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;

        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }

        segShift = 32 - sshift;
        segMask = ssize - 1;
        segs = (Segment[])Array.newInstance(Segment.class, ssize);

        if (initCap > MAX_CAP)
            initCap = MAX_CAP;

        int c = initCap / ssize;

        if (c * ssize < initCap)
            ++c;

        int cap = 1;

        while (cap < c)
            cap <<= 1;

        if (cap < 16)
            cap = 16;

        for (int i = 0; i < segs.length; ++i)
            segs[i] = new Segment(cap, loadFactor);
    }

    /**
     * Creates a new, empty map with the specified initial capacity
     * and load factor and with the default concurrencyLevel (16).
     *
     * @param ctx Cache context.
     * @param initCap The implementation performs internal
     *      sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative or the load factor is non-positive.
     */
    public GridCacheConcurrentMap(GridCacheContext<K, V> ctx, int initCap, float loadFactor) {
        this(ctx, initCap, loadFactor, DFLT_CONCUR_LEVEL);
    }

    /**
     * Creates a new, empty map with the specified initial capacity,
     * and with default load factor (0.75) and concurrencyLevel (16).
     *
     * @param ctx Cache context.
     * @param initCap the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative.
     */
    public GridCacheConcurrentMap(GridCacheContext<K, V> ctx, int initCap) {
        this(ctx, initCap, DFLT_LOAD_FACTOR, DFLT_CONCUR_LEVEL);
    }

    /**
     * Sets factory for entries.
     *
     * @param factory Entry factory.
     */
    public void setEntryFactory(GridCacheMapEntryFactory<K, V> factory) {
        assert factory != null;

        this.factory = factory;
    }

    /**
     * @return Non-internal predicate.
     */
    private static <K, V> IgnitePredicate<CacheEntry<K, V>>[] nonInternal() {
        return (IgnitePredicate<CacheEntry<K,V>>[])NON_INTERNAL_ARR;
    }

    /**
     * @param filter Filter to add to non-internal-key filter.
     * @return Non-internal predicate.
     */
    private static <K, V> IgnitePredicate<CacheEntry<K, V>>[] nonInternal(
        IgnitePredicate<CacheEntry<K, V>>[] filter) {
        return F.asArray(F0.and((IgnitePredicate<CacheEntry<K, V>>[]) NON_INTERNAL_ARR, filter));
    }

    /**
     * @return {@code True} if this map is empty.
     */
    public boolean isEmpty() {
        return mapSize.sum() == 0;
    }

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map.
     */
    public int size() {
        return mapSize.intValue();
    }

    /**
     * @return Public size.
     */
    public int publicSize() {
        return mapPubSize.intValue();
    }

    /**
     * @param e Cache map entry.
     */
    public void decrementSize(GridCacheMapEntry<K, V> e) {
        assert !e.isInternal();
        assert Thread.holdsLock(e);
        assert e.deletedUnlocked();
        assert ctx.deferredDelete();

        mapPubSize.decrement();

        segmentFor(e.hash()).decrementPublicSize();
    }

    /**
     * @param e Cache map entry.
     */
    public void incrementSize(GridCacheMapEntry<K, V> e) {
        assert !e.isInternal();
        assert Thread.holdsLock(e);
        assert !e.deletedUnlocked();
        assert ctx.deferredDelete();

        mapPubSize.increment();

        segmentFor(e.hash()).incrementPublicSize();
    }

    /**
     * @param key Key.
     * @return {@code True} if map contains mapping for provided key.
     */
    public boolean containsKey(Object key) {
        checkWeakQueue();

        int hash = hash(key.hashCode());

        return segmentFor(hash).containsKey(key, hash);
    }

    /**
     * Collection of all (possibly {@code null}) values.
     *
     * @param filter Filter.
     * @return a collection view of the values contained in this map.
     */
    public Collection<V> allValues(IgnitePredicate<CacheEntry<K, V>>[] filter) {
        checkWeakQueue();

        return new Values<>(this, filter);
    }

    /**
     * @return Random entry out of hash map.
     */
    @Nullable public GridCacheMapEntry<K, V> randomEntry() {
        checkWeakQueue();

        while (true) {
            if (mapPubSize.sum() == 0)
                return null;

            // Desired and current indexes.
            int segIdx = RAND.nextInt(segs.length);

            Segment seg = null;

            for (int i = segIdx; i < segs.length + segIdx; i++) {
                Segment s = segs[i % segs.length];

                if (s.publicSize() > 0)
                    seg = s;
            }

            if (seg == null)
                // It happened so that all public values had been removed from segments.
                return null;

            GridCacheMapEntry<K, V> entry = seg.randomEntry();

            if (entry == null)
                continue;

            assert !(entry.key() instanceof GridCacheInternal);

            return entry;
        }
    }

    /**
     * Returns the entry associated with the specified key in the
     * HashMap.  Returns null if the HashMap contains no mapping
     * for this key.
     *
     * @param key Key.
     * @return Entry.
     */
    @Nullable public GridCacheMapEntry<K, V> getEntry(Object key) {
        assert key != null;

        checkWeakQueue();

        int hash = hash(key.hashCode());

        return segmentFor(hash).get(key, hash);
    }

    /**
     * @param topVer Topology version.
     * @param key Key.
     * @param val Value.
     * @param ttl Time to live.
     * @return Cache entry for corresponding key-value pair.
     */
    public GridCacheMapEntry<K, V> putEntry(long topVer, K key, @Nullable V val, long ttl) {
        assert key != null;

        checkWeakQueue();

        int hash = hash(key.hashCode());

        return segmentFor(hash).put(key, hash, val, topVer, ttl);
    }

    /**
     * @param topVer Topology version.
     * @param key Key.
     * @param val Value.
     * @param ttl Time to live.
     * @param create Create flag.
     * @return Triple where the first element is current entry associated with the key,
     *      the second is created entry and the third is doomed (all may be null).
     */
    public GridTriple<GridCacheMapEntry<K, V>> putEntryIfObsoleteOrAbsent(long topVer, K key, @Nullable V val,
        long ttl, boolean create) {
        assert key != null;

        checkWeakQueue();

        int hash = hash(key.hashCode());

        return segmentFor(hash).putIfObsolete(key, hash, val, topVer, ttl, create);
    }

    /**
     * Copies all of the mappings from the specified map to this map
     * These mappings will replace any mappings that
     * this map had for any of the keys currently in the specified map.
     *
     * @param m mappings to be stored in this map.
     * @param ttl Time to live.
     * @throws NullPointerException If the specified map is null.
     */
    public void putAll(Map<? extends K, ? extends V> m, long ttl) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
            putEntry(-1, e.getKey(), e.getValue(), ttl);
    }

    /**
     * Removes passed in entry if it presents in the map.
     *
     * @param e Entry to remove.
     * @return {@code True} if remove happened.
     */
    public boolean removeEntry(GridCacheEntryEx<K, V> e) {
        assert e != null;

        checkWeakQueue();

        K key = e.key();

        int hash = hash(key.hashCode());

        return segmentFor(hash).remove(key, hash, same(e)) != null;
    }

    /**
     * @param p Entry to check equality.
     * @return Predicate to filter the same (equal by ==) entry.
     */
    private IgnitePredicate<GridCacheMapEntry<K, V>> same(final GridCacheEntryEx<K, V> p) {
        return new P1<GridCacheMapEntry<K,V>>() {
            @Override public boolean apply(GridCacheMapEntry<K, V> e) {
                return e == p;
            }
        };
    }

    /**
     * Removes and returns the entry associated with the specified key
     * in the HashMap if entry is obsolete. Returns null if the HashMap
     * contains no mapping for this key.
     *
     * @param key Key.
     * @return Removed entry, possibly {@code null}.
     */
    @SuppressWarnings( {"unchecked"})
    @Nullable public GridCacheMapEntry<K, V> removeEntryIfObsolete(K key) {
        assert key != null;

        checkWeakQueue();

        int hash = hash(key.hashCode());

        return segmentFor(hash).remove(key, hash, obsolete);
    }

    /**
     * Entry wrapper set.
     *
     * @param filter Filter.
     * @return Entry wrapper set.
     */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    public Set<GridCacheEntryImpl<K, V>> wrappers(IgnitePredicate<CacheEntry<K, V>>[] filter) {
        checkWeakQueue();

        return (Set<GridCacheEntryImpl<K, V>>)(Set<? extends CacheEntry<K, V>>)entries(filter);
    }

    /**
     * Entry wrapper set casted to projections.
     *
     * @param filter Filter to check.
     * @return Entry projections set.
     */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    public Set<CacheEntry<K, V>> projections(IgnitePredicate<CacheEntry<K, V>>[] filter) {
        checkWeakQueue();

        return (Set<CacheEntry<K, V>>)(Set<? extends CacheEntry<K, V>>)wrappers(filter);
    }

    /**
     * Same as {@link #wrappers(org.apache.ignite.lang.IgnitePredicate[])}
     *
     * @param filter Filter.
     * @return Set of the mappings contained in this map.
     */
    @SuppressWarnings({"unchecked"})
    public Set<CacheEntry<K, V>> entries(IgnitePredicate<CacheEntry<K, V>>... filter) {
        checkWeakQueue();

        return new EntrySet<>(this, filter);
    }

    /**
     * Returns entry set containing internal entries.
     *
     * @param filter Filter.
     * @return Set of the mappings contained in this map.
     */
    @SuppressWarnings({"unchecked"})
    public Set<CacheEntry<K, V>> entriesx(IgnitePredicate<CacheEntry<K, V>>... filter) {
        checkWeakQueue();

        return new EntrySet<>(this, filter, true);
    }

    /**
     * Internal entry set, excluding {@link GridCacheInternal} entries.
     *
     * @return Set of the mappings contained in this map.
     */
    public Set<GridCacheEntryEx<K, V>> entries0() {
        checkWeakQueue();

        return new Set0<>(this, GridCacheConcurrentMap.<K, V>nonInternal());
    }

    /**
     * Get striped entry iterator.
     *
     * @param id Expected modulo.
     * @param totalCnt Maximum modulo.
     * @return Striped entry iterator.
     */
    public Iterator<GridCacheEntryEx<K, V>> stripedEntryIterator(int id, int totalCnt) {
        checkWeakQueue();

        return new Iterator0<>(this, false, GridCacheConcurrentMap.<K, V>nonInternal(), id, totalCnt);
    }

    /**
     * Gets all internal entry set, including {@link GridCacheInternal} entries.
     *
     * @return All internal entry set, including {@link GridCacheInternal} entries.
     */
    public Set<GridCacheEntryEx<K, V>> allEntries0() {
        checkWeakQueue();

        return new Set0<>(this, CU.<K, V>empty());
    }

    /**
     * Key set.
     *
     * @param filter Filter.
     * @return Set of the keys contained in this map.
     */
    public Set<K> keySet(IgnitePredicate<CacheEntry<K, V>>... filter) {
        checkWeakQueue();

        return new KeySet<>(this, filter);
    }

    /**
     * Collection of non-{@code null} values.
     *
     * @param filter Filter.
     * @return Collection view of the values contained in this map.
     */
    public Collection<V> values(IgnitePredicate<CacheEntry<K, V>>... filter) {
        checkWeakQueue();

        return allValues(filter);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheConcurrentMap.class, this, "size", mapSize, "pubSize", mapPubSize);
    }

    /**
     *
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    void printDebugInfo() {
        for (Segment s : segs)
            s.lock();

        try {
            X.println(">>> Cache map debug info: " + ctx.namexx());

            for (int i = 0; i < segs.length; i++) {
                Segment seg = segs[i];

                X.println("    Segment [idx=" + i + ", size=" + seg.size() + ']');

                SegmentHeader<K, V> segHdr = seg.hdr;

                GridCacheMapEntry<K, V>[] tab = segHdr.table();

                for (int j = 0; j < tab.length; j++)
                    X.println("        Bucket [idx=" + j + ", bucket=" + tab[j] + ']');
            }

            checkConsistency();
        }
        finally {
            for (Segment s : segs)
                s.unlock();
        }
    }

    /**
     *
     */
    public void checkConsistency() {
        int size = 0;
        int pubSize = 0;

        IgniteLogger log = ctx.logger(GridCacheConcurrentMap.class);

        for (Segment s : segs) {
            SegmentHeader<K, V> segHdr = s.hdr;

            GridCacheMapEntry<K, V>[] tab = segHdr.table();

            for (GridCacheMapEntry<K, V> b : tab) {
                if (b != null) {
                    GridCacheMapEntry<K, V> e = b;

                    assert e != null;

                    int cnt = 0;
                    int pubCnt = 0;

                    while (e != null) {
                        cnt++;

                        log.info("Cache map entry: " + e);

                        if (!e.deleted()) {
                            if (!(e.key instanceof GridCacheInternal))
                                pubCnt++;
                        }

                        e = e.next(segHdr.id());
                    }

                    size += cnt;
                    pubSize += pubCnt;
                }
            }
        }

        assert size() == size : "Invalid size [expected=" + size() + ", actual=" + size + ']';
        assert publicSize() == pubSize : "Invalid public size [expected=" + publicSize() + ", actual=" + pubSize + ']';
    }

    /**
     * Segments are specialized versions of hash tables.  This
     * subclasses from ReentrantLock opportunistically,
     * just to simplify some locking and avoid separate construction.
     */
    private class Segment extends ReentrantLock {
        /** */
        private static final long serialVersionUID = 0L;

        /*
         * Segments maintain a table of entry lists that are ALWAYS
         * kept in a consistent state, so can be read without locking.
         * Next fields of nodes are immutable (final).  All list
         * additions are performed at the front of each bin. This
         * makes it easy to check changes, and also fast to traverse.
         * When nodes would otherwise be changed, new nodes are
         * created to replace them. This works well for hash tables
         * since the bin lists tend to be short. (The average length
         * is less than two for the default load factor threshold.)
         *
         * Read operations can thus proceed without locking, but rely
         * on selected uses of volatiles to ensure that completed
         * write operations performed by other threads are
         * noticed. For most purposes, the "count" field, tracking the
         * number of elements, serves as that volatile variable
         * ensuring visibility.  This is convenient because this field
         * needs to be read in many read operations anyway:
         *
         *   - All (unsynchronized) read operations must first read the
         *     "count" field, and should not look at table entries if
         *     it is 0.
         *
         *   - All (synchronized) write operations should write to
         *     the "count" field after structurally changing any bin.
         *     The operations must not take any action that could even
         *     momentarily cause a concurrent read operation to see
         *     inconsistent data. This is made easier by the nature of
         *     the read operations in Map. For example, no operation
         *     can reveal that the table has grown but the threshold
         *     has not yet been updated, so there are no atomicity
         *     requirements for this with respect to reads.
         *
         * As a guide, all critical volatile reads and writes to the
         * count field are marked in code comments.
         */

        /**
         * The table is rehashed when its size exceeds this threshold.
         * (The value of this field is always <tt>(int)(capacity * loadFactor)</tt>.)
         */
        private int threshold;

        /** Segment header. */
        private volatile SegmentHeader<K, V> hdr;

        /** The number of public elements in this segment's region. */
        private final LongAdder pubSize = new LongAdder();

        /**
         * The load factor for the hash table. Even though this value
         * is same for all segments, it is replicated to avoid needing
         * links to outer object.
         * @serial
         */
        private final float loadFactor;

        /**
         * @param initCap Initial capacity.
         * @param lf Load factor.
         */
        @SuppressWarnings("unchecked")
        Segment(int initCap, float lf) {
            loadFactor = lf;

            hdr = new SegmentHeader<>(initCap, 0, null);

            threshold = (int)(hdr.length() * loadFactor);
        }

        /**
         * Returns properly casted first entry for given hash.
         *
         * @param tbl Table.
         * @param hash Hash.
         * @return Entry for hash.
         */
        @Nullable GridCacheMapEntry<K, V> getFirst(GridCacheMapEntry<K, V>[] tbl, int hash) {
            GridCacheMapEntry<K, V> bin = tbl[hash & (tbl.length - 1)];

            return bin != null ? bin : null;
        }

        /**
         * @return Segment header for read operation.
         */
        private SegmentHeader<K, V> headerForRead() {
            while (true) {
                SegmentHeader<K, V> hdr = this.hdr;

                hdr.onReadStart();

                // Check if 2 rehashes didn't happen in between.
                if (hdr == this.hdr)
                    return hdr;
                else
                    hdr.onReadEnd();
            }
        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @return Value.
         */
        @Nullable GridCacheMapEntry<K, V> get(Object key, int hash) {
            SegmentHeader<K, V> hdr = headerForRead();

            try {
                if (hdr.size() != 0) {
                    GridCacheMapEntry<K, V> e = getFirst(hdr.table(), hash);

                    while (e != null) {
                        if (e.hash() == hash && key.equals(e.key()))
                            return e;

                        e = e.next(hdr.id());
                    }
                }

                return null;
            }
            finally {
                hdr.onReadEnd();
            }
        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @return {@code True} if segment contains value.
         */
        boolean containsKey(Object key, int hash) {
            SegmentHeader<K, V> hdr = headerForRead();

            try {
                if (hdr.size() != 0) {
                    GridCacheMapEntry<K, V> e = getFirst(hdr.table(), hash);

                    while (e != null) {
                        if (e.hash() == hash && key.equals(e.key))
                            return true;

                        e = e.next(hdr.id());
                    }
                }

                return false;
            }
            finally {
                hdr.onReadEnd();
            }
        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @param val Value.
         * @param topVer Topology version.
         * @param ttl TTL.
         * @return Associated value.
         */
        @SuppressWarnings({"unchecked"})
        GridCacheMapEntry<K, V> put(K key, int hash, @Nullable V val, long topVer, long ttl) {
            lock();

            try {
                return put0(key, hash, val, topVer, ttl);
            }
            finally {
                unlock();
            }
        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @param val Value.
         * @param topVer Topology version.
         * @param ttl TTL.
         * @return Associated value.
         */
        @SuppressWarnings({"unchecked", "SynchronizationOnLocalVariableOrMethodParameter"})
        private GridCacheMapEntry<K, V> put0(K key, int hash, V val, long topVer, long ttl) {
            try {
                SegmentHeader<K, V> hdr = this.hdr;

                int c = hdr.size();

                if (c++ > threshold) {// Ensure capacity.
                    rehash();

                    hdr = this.hdr;
                }

                int hdrId = hdr.id();

                GridCacheMapEntry<K, V>[] tab = hdr.table();

                int idx = hash & (tab.length - 1);

                GridCacheMapEntry<K, V> bin = tab[idx];

                GridCacheMapEntry<K, V> e = bin;

                while (e != null && (e.hash() != hash || !key.equals(e.key)))
                    e = e.next(hdrId);

                GridCacheMapEntry<K, V> retVal;

                if (e != null) {
                    retVal = e;

                    e.rawPut(val, ttl);
                }
                else {
                    GridCacheMapEntry next = bin != null ? bin : null;

                    GridCacheMapEntry newRoot = factory.create(ctx, topVer, key, hash, val, next, ttl, hdr.id());

                    // Avoiding delete (decrement) before creation (increment).
                    synchronized (newRoot) {
                        tab[idx] = newRoot;

                        retVal = newRoot;

                        // Modify counters.
                        if (!retVal.isInternal()) {
                            mapPubSize.increment();

                            pubSize.increment();
                        }
                    }

                    mapSize.increment();

                    hdr.size(c);
                }

                return retVal;
            }
            finally {
                if (DEBUG)
                    checkSegmentConsistency();
            }
        }

        /**
         * @param key Key.
         * @param hash Hash.
         * @param val Value.
         * @param topVer Topology version.
         * @param ttl TTL.
         * @param create Create flag.
         * @return Triple where the first element is current entry associated with the key,
         *      the second is created entry and the third is doomed (all may be null).
         */
        @SuppressWarnings( {"unchecked"})
        GridTriple<GridCacheMapEntry<K, V>> putIfObsolete(K key, int hash, @Nullable V val, long topVer, long ttl,
            boolean create) {
            lock();

            try {
                SegmentHeader<K, V> hdr = this.hdr;

                int hdrId = hdr.id();

                GridCacheMapEntry<K, V>[] tab = hdr.table();

                int idx = hash & (tab.length - 1);

                GridCacheMapEntry<K, V> bin = tab[idx];

                GridCacheMapEntry<K, V> cur = null;
                GridCacheMapEntry<K, V> created = null;
                GridCacheMapEntry<K, V> doomed = null;

                if (bin == null) {
                    if (create)
                        cur = created = put0(key, hash, val, topVer, ttl);

                    return new GridTriple<>(cur, created, doomed);
                }

                GridCacheMapEntry<K, V> e = bin;

                while (e != null && (e.hash() != hash || !key.equals(e.key)))
                    e = e.next(hdrId);

                if (e != null) {
                    if (e.obsolete()) {
                        doomed = remove(key, hash, null);

                        if (create)
                            cur = created = put0(key, hash, val, topVer, ttl);
                    }
                    else
                        cur = e;
                }
                else if (create)
                    cur = created = put0(key, hash, val, topVer, ttl);

                return new GridTriple<>(cur, created, doomed);
            }
            finally {
                unlock();
            }
        }

        /**
         *
         */
        @SuppressWarnings("unchecked")
        void rehash() {
            SegmentHeader<K, V> oldHdr = hdr;

            if (oldHdr.previous() != null && oldHdr.previous().hasReads())
                return; // Wait for previous header to free up.

            int oldId = hdr.id();

            GridCacheMapEntry<K, V>[] oldTbl = oldHdr.table();

            int oldCap = oldTbl.length;

            if (oldCap >= MAX_CAP)
                return;

            /*
             * Reclassify nodes in each list to new Map.  Because we are
             * using power-of-two expansion, the elements from each bin
             * must either stay at same index, or move with a power of two
             * offset. We eliminate unnecessary node creation by catching
             * cases where old nodes can be reused because their next
             * fields won't change. Statistically, at the default
             * threshold, only about one-sixth of them need cloning when
             * a table doubles. The nodes they replace will be eligible for GC
             * as soon as they are no longer referenced by any
             * reader thread that may be in the midst of traversing table
             * right now.
             */
            SegmentHeader<K, V> newHdr = new SegmentHeader<>(oldCap << 1, oldId + 1, oldHdr);

            oldHdr.next(newHdr); // Link.

            newHdr.size(oldHdr.size());

            GridCacheMapEntry<K, V>[] newTbl = newHdr.table();

            threshold = (int)(newTbl.length * loadFactor);

            int sizeMask = newTbl.length - 1;

            for (GridCacheMapEntry<K, V> bin1 : oldTbl) {
                // Relink all nodes.
                for (GridCacheMapEntry<K, V> e = bin1; e != null; e = e.next(oldId)) {
                    int idx = e.hash() & sizeMask;

                    GridCacheMapEntry<K, V> bin2 = newTbl[idx];

                    newTbl[idx] = e;

                    e.next(newHdr.id(), bin2);
                }
            }

            hdr = newHdr;

            if (DEBUG)
                checkSegmentConsistency();
        }

        /**
         * Remove; match on key only if value null, else match both.
         *
         * @param key Key.
         * @param hash Hash.
         * @param filter Optional predicate.
         * @return Removed value.
         */
        @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
        @Nullable GridCacheMapEntry<K, V> remove(Object key, int hash,
            @Nullable IgnitePredicate<GridCacheMapEntry<K, V>> filter) {
            lock();

            try {
                SegmentHeader<K, V> hdr = this.hdr;

                GridCacheMapEntry<K, V>[] tbl = hdr.table();

                int idx = hash & (tbl.length - 1);

                GridCacheMapEntry<K, V> bin = tbl[idx];

                if (bin == null)
                    return null;

                GridCacheMapEntry<K, V> prev = null;
                GridCacheMapEntry<K, V> e = bin;

                while (e != null && (e.hash() != hash || !key.equals(e.key))) {
                    prev = e;

                    e = e.next(hdr.id());
                }

                if (e != null) {
                    if (filter != null && !filter.apply(e))
                        return null;

                    if (prev == null)
                        tbl[idx] = e.next(hdr.id());
                    else
                        prev.next(hdr.id(), e.next(hdr.id()));

                    // Modify counters.
                    synchronized (e) {
                        if (!e.isInternal() && !e.deleted()) {
                            mapPubSize.decrement();

                            pubSize.decrement();
                        }
                    }

                    mapSize.decrement();

                    hdr.decrementSize();
                }

                return e;
            }
            finally {
                if (DEBUG)
                    checkSegmentConsistency();

                unlock();
            }
        }

        /**
         * @return Entries count within segment.
         */
        int size() {
            return hdr.size();
        }

        /**
         * @return Public entries count within segment.
         */
        int publicSize() {
            return pubSize.intValue();
        }

        /**
         * Decrements segment public size.
         */
        void decrementPublicSize() {
            pubSize.decrement();
        }

        /**
         * Decrements segment public size.
         */
        void incrementPublicSize() {
            pubSize.increment();
        }

        /**
         * @return Random cache map entry from this segment.
         */
        @Nullable GridCacheMapEntry<K, V> randomEntry() {
            SegmentHeader<K, V> hdr = headerForRead();

            try {
                GridCacheMapEntry<K, V>[] tbl = hdr.table();

                Collection<GridCacheMapEntry<K, V>> entries = new ArrayList<>(3);

                int pubCnt = 0;

                int start = RAND.nextInt(tbl.length);

                for (int i = start; i < start + tbl.length; i++) {
                    GridCacheMapEntry<K, V> first = tbl[i % tbl.length];

                    if (first == null)
                        continue;

                    entries.add(first);

                    for (GridCacheMapEntry<K, V> e = first; e != null; e = e.next(hdr.id()))
                        if (!e.isInternal())
                            pubCnt++;

                    if (entries.size() == 3)
                        break;
                }

                if (entries.isEmpty())
                    return null;

                if (pubCnt == 0)
                    return null;

                // Desired and current indexes.
                int idx = RAND.nextInt(pubCnt);

                int i = 0;

                GridCacheMapEntry<K, V> retVal = null;

                for (GridCacheMapEntry<K, V> e : entries) {
                    for (; e != null; e = e.next(hdr.id())) {
                        if (!(e.key instanceof GridCacheInternal)) {
                            // In case desired entry was deleted, we return the closest one from left.
                            retVal = e;

                            if (idx == i++)
                                break;
                        }
                    }
                }

                return retVal;
            }
            finally {
                hdr.onReadEnd();
            }
        }

        /**
         *
         */
        void checkSegmentConsistency() {
            SegmentHeader<K, V> hdr = this.hdr;

            GridCacheMapEntry<K, V>[] tbl = hdr.table();

            int cnt = 0;
            int pubCnt = 0;

            for (GridCacheMapEntry<K, V> b : tbl) {
                if (b != null) {
                    GridCacheMapEntry<K, V> e = b;

                    assert e != null;

                    while (e != null) {
                        cnt++;

                        if (!(e.key instanceof GridCacheInternal))
                            pubCnt++;

                        e = e.next(hdr.id());
                    }
                }
            }

            assert cnt == hdr.size() : "Entry count and header size mismatch [cnt=" + cnt + ", hdrSize=" +
                hdr.size() + ", segment=" + this + ", hdrId=" + hdr.id() + ']';
            assert pubCnt == pubSize.intValue();
        }
    }

    /**
     * Segment header.
     */
    private static class SegmentHeader<K, V> {
        /** Entry table. */
        private final GridCacheMapEntry<K, V>[] tbl;

        /** Id for rehash. */
        private final int id;

        /** Reads. */
        private final LongAdder reads = new LongAdder();

        /** */
        private volatile SegmentHeader<K, V> prev;

        /** */
        private volatile SegmentHeader<K, V> next;

        /** The number of elements in this segment's region. */
        private volatile int size;

        /** Cleaned flag. */
        private final AtomicBoolean cleaned = new AtomicBoolean();

        /**
         * Constructs new segment header. New header is created initially and then
         * every time during rehash operation.
         *
         * @param size Size of the table.
         * @param id ID.
         * @param prev Previous header.
         */
        @SuppressWarnings("unchecked")
        private SegmentHeader(int size, int id, @Nullable SegmentHeader<K, V> prev) {
            tbl = new GridCacheMapEntry[size];

            assert id >= 0;

            this.id = id;
            this.prev = prev;
        }

        /**
         * Increment reads.
         */
        void onReadStart() {
            reads.increment();
        }

        /**
         * Decrement reads.
         */
        void onReadEnd() {
            reads.decrement();

            checkClean();
        }

        /**
         * Cleans stale links if needed.
         */
        void checkClean() {
            // Check if rehashing didn't occur for the next segment.
            if (next != null && next.next() == null) {
                long leftReads = reads.sum();

                assert leftReads >= 0;

                // Clean up.
                if (leftReads == 0 && cleaned.compareAndSet(false, true)) {
                    for (GridCacheMapEntry<K, V> bin : tbl) {
                        if (bin != null) {
                            for (GridCacheMapEntry<K, V> e = bin; e != null; ) {
                                GridCacheMapEntry<K, V> next = e.next(id);

                                e.next(id, null); // Unlink.

                                e = next;
                            }
                        }
                    }
                }
            }
        }

        /**
         * @return {@code True} if has reads.
         */
        boolean hasReads() {
            return reads.sum() > 0;
        }

        /**
         * @return Number of reads.
         */
        long reads() {
            return reads.sum();
        }

        /**
         * @return Header ID.
         */
        int id() {
            return id;
        }

        /**
         * @return {@code True} if {@code ID} is even.
         */
        boolean even() {
            return id % 2 == 0;
        }

        /**
         * @return {@code True} if {@code ID} is odd.
         */
        @SuppressWarnings("BadOddness")
        boolean odd() {
            return id % 2 == 1;
        }

        /**
         * @return Table.
         */
        GridCacheMapEntry<K, V>[] table() {
            return tbl;
        }

        /**
         * @return Table length.
         */
        int length() {
            return tbl.length;
        }

        /**
         * @return Next header.
         */
        SegmentHeader<K, V> next() {
            return next;
        }

        /**
         * @param next Next header.
         */
        void next(SegmentHeader<K, V> next) {
            this.next = next;
        }

        /**
         * @return Previous header.
         */
        SegmentHeader<K, V> previous() {
            return prev;
        }

        /**
         * @param prev Previous header.
         */
        void previous(SegmentHeader<K, V> prev) {
            this.prev = prev;
        }

        /**
         * @return New size.
         */
        int decrementSize() {
            return --size;
        }

        /**
         * @return Size.
         */
        int size() {
            return size;
        }

        /**
         * @param size Size.
         */
        void size(int size) {
            this.size = size;
        }
    }

    /**
     * Phantom segment header to be used in iterators.
     */
    private static class WeakIterator<K, V> extends WeakReference<Iterator0<K, V>> {
        /**
         * Creates a new phantom reference that refers to the given segment header
         * and is registered with the given queue.
         *
         * @param ref Referred segment header.
         * @param q Reference queue.
         */
        WeakIterator(Iterator0<K, V> ref, ReferenceQueue<Iterator0<K, V>> q) {
            super(ref, q);

            assert ref != null;
            assert q != null;
        }
    }

    /**
     * Iterator over {@link GridCacheEntryEx} elements.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     */
    private static class Iterator0<K, V> implements Iterator<GridCacheEntryEx<K, V>>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int nextSegIdx;

        /** */
        private int nextTblIdx;

        /** Segment header. */
        private SegmentHeader<K, V> curSegHdr;

        /** */
        private GridCacheMapEntry<K,V>[] curTbl;

        /** */
        private GridCacheMapEntry<K, V> nextEntry;

        /** Next entry to return. */
        private GridCacheMapEntry<K, V> next;

        /** Next value. */
        private V nextVal;

        /** Current value. */
        private V curVal;

        /** */
        private boolean isVal;

        /** Current entry. */
        private GridCacheMapEntry<K, V> cur;

        /** Iterator filter. */
        private IgnitePredicate<CacheEntry<K, V>>[] filter;

        /** Outer cache map. */
        private GridCacheConcurrentMap<K, V> map;

        /** Cache context. */
        private GridCacheContext<K, V> ctx;

        /** Soft reference. */
        private final WeakIterator<K, V> weakRef;

        /** Mod. */
        private int id;

        /** Mod count. */
        private int totalCnt;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public Iterator0() {
            weakRef = null;
        }

        /**
         * @param map Cache map.
         * @param isVal {@code True} if value iterator.
         * @param filter Entry filter.
         * @param id ID of the iterator.
         * @param totalCnt Total count of iterators.
         */
        @SuppressWarnings({"unchecked"})
        Iterator0(GridCacheConcurrentMap<K, V> map, boolean isVal,
            IgnitePredicate<CacheEntry<K, V>>[] filter, int id, int totalCnt) {
            this.filter = filter;
            this.isVal = isVal;
            this.id = id;
            this.totalCnt = totalCnt;

            this.map = map;

            ctx = map.ctx;

            nextSegIdx = map.segs.length - 1;
            nextTblIdx = -1;

            weakRef = new WeakIterator<>(this, map.iteratorQueue());

            advance();

            if (curSegHdr != null)
               map.addWeakIterator(weakRef, curSegHdr); // Keep pointer to soft reference.
        }

        /**
         *
         */
        @SuppressWarnings({"unchecked"})
        private void advance() {
            if (nextEntry != null && advanceInBucket(nextEntry, true))
                return;

            while (nextTblIdx >= 0) {
                GridCacheMapEntry<K, V> bucket = curTbl[nextTblIdx--];

                if (bucket != null && advanceInBucket(bucket, false))
                    return;
            }

            while (nextSegIdx >= 0) {
                int nextSegIdx0 = nextSegIdx--;

                GridCacheConcurrentMap.Segment seg = map.segs[nextSegIdx0];

                if (seg.size() != 0 && (id == -1 || nextSegIdx0 % totalCnt == id)) {
                    if (curSegHdr != null)
                        curSegHdr.onReadEnd();

                    curSegHdr = seg.headerForRead();

                    assert curSegHdr != null;

                    map.addWeakIterator(weakRef, curSegHdr);

                    curTbl = curSegHdr.table();

                    for (int j = curTbl.length - 1; j >= 0; --j) {
                        GridCacheMapEntry<K, V> bucket = curTbl[j];

                        if (bucket != null && advanceInBucket(bucket, false)) {
                            nextTblIdx = j - 1;

                            return;
                        }
                    }
                }
            }
        }

        /**
         * @param e Current next.
         * @param skipFirst {@code True} to skip check on first iteration.
         * @return {@code True} if advance succeeded.
         */
        @SuppressWarnings( {"unchecked"})
        private boolean advanceInBucket(@Nullable GridCacheMapEntry<K, V> e, boolean skipFirst) {
            if (e == null)
                return false;

            nextEntry = e;

            do {
                if (!skipFirst) {
                    next = nextEntry;

                    // Check if entry is visitable first before doing projection-aware peek.
                    if (!next.visitable(filter))
                        continue;

                    if (isVal) {
                        nextVal = next.wrap(true).peek();

                        if (nextVal == null)
                            continue;
                    }

                    return true;
                }

                // Perform checks in any case.
                skipFirst = false;
            }
            while ((nextEntry = nextEntry.next(curSegHdr.id())) != null);

            assert nextEntry == null;

            next = null;
            nextVal = null;

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            boolean hasNext = next != null && (!isVal || nextVal != null);

            if (!hasNext && curSegHdr != null) {
                curSegHdr.onReadEnd();

                weakRef.clear(); // Do not enqueue.

                map.removeWeakIterator(weakRef); // Remove hard pointer.
            }

            return hasNext;
        }

        /**
         * @return Next value.
         */
        public V currentValue() {
            return curVal;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public GridCacheEntryEx<K, V> next() {
            GridCacheMapEntry<K, V> e = next;
            V v = nextVal;

            if (e == null)
                throw new NoSuchElementException();

            advance();

            cur = e;
            curVal = v;

            return cur;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            if (cur == null)
                throw new IllegalStateException();

            GridCacheMapEntry<K, V> e = cur;

            cur = null;
            curVal = null;

            try {
                ctx.cache().remove(e.key(), CU.<K, V>empty());
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteException(ex);
            }
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(ctx);
            out.writeObject(filter);
            out.writeBoolean(isVal);
            out.writeInt(id);
            out.writeInt(totalCnt);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            ctx = (GridCacheContext<K, V>)in.readObject();
            filter = (IgnitePredicate<CacheEntry<K, V>>[])in.readObject();
            isVal = in.readBoolean();
            id = in.readInt();
            totalCnt = in.readInt();
        }

        /**
         * Reconstructs object on unmarshalling.
         *
         * @return Reconstructed object.
         * @throws ObjectStreamException Thrown in case of unmarshalling error.
         */
        protected Object readResolve() throws ObjectStreamException {
            return new Iterator0<>(ctx.cache().map(), isVal, filter, id, totalCnt);
        }
    }

    /**
     * Entry set.
     */
    @SuppressWarnings("unchecked")
    private static class Set0<K, V> extends AbstractSet<GridCacheEntryEx<K, V>> implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Filter. */
        private IgnitePredicate<CacheEntry<K, V>>[] filter;

        /** Base map. */
        private GridCacheConcurrentMap<K, V> map;

        /** Context. */
        private GridCacheContext<K, V> ctx;

        /** */
        private GridCacheProjectionImpl prjPerCall;

        /** */
        private CacheFlag[] forcedFlags;

        /** */
        private boolean clone;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public Set0() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Filter.
         */
        private Set0(GridCacheConcurrentMap<K, V> map, IgnitePredicate<CacheEntry<K, V>>[] filter) {
            assert map != null;

            this.map = map;
            this.filter = filter;

            ctx = map.ctx;

            prjPerCall = ctx.projectionPerCall();
            forcedFlags = ctx.forcedFlags();
            clone = ctx.hasFlag(CLONE);
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<GridCacheEntryEx<K, V>> iterator() {
            return new Iterator0<>(map, false, filter, -1, -1);
        }

        /**
         * @return Entry iterator.
         */
        Iterator<CacheEntry<K, V>> entryIterator() {
            return new EntryIterator<>(map, filter, ctx, prjPerCall, forcedFlags);
        }

        /**
         * @return Key iterator.
         */
        Iterator<K> keyIterator() {
            return new KeyIterator<>(map, filter);
        }

        /**
         * @return Value iterator.
         */
        Iterator<V> valueIterator() {
            return new ValueIterator<>(map, filter, ctx, clone);
        }

        /**
         * Checks for key containment.
         *
         * @param k Key to check.
         * @return {@code True} if key is in the map.
         */
        boolean containsKey(K k) {
            GridCacheEntryEx<K, V> e = ctx.cache().peekEx(k);

            try {
                return e != null && !e.obsolete() && (!e.deleted() || e.lockedByThread()) && F.isAll(e.wrap(false), filter);
            }
            catch (GridCacheEntryRemovedException ignore) {
                return false;
            }
        }

        /**
         * @param v Checks if value is contained in
         * @return {@code True} if value is in the set.
         */
        boolean containsValue(V v) {
            A.notNull(v, "value");

            if (v == null)
                return false;

            for (Iterator<V> it = valueIterator(); it.hasNext(); ) {
                V v0 = it.next();

                if (F.eq(v0, v))
                    return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            if (!(o instanceof GridCacheEntryEx))
                return false;

            GridCacheEntryEx<K, V> e = (GridCacheEntryEx<K, V>)o;

            GridCacheEntryEx<K, V> cur = ctx.cache().peekEx(e.key());

            return cur != null && cur.equals(e);
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            return o instanceof CacheEntry && removeKey(((Map.Entry<K, V>)o).getKey());
        }

        /**
         * @param k Key to remove.
         * @return If key has been removed.
         */
        boolean removeKey(K k) {
            try {
                return ctx.cache().remove(k, CU.<K, V>empty()) != null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to remove cache entry for key: " + k, e);
            }
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return F.isEmpty(filter) ? map.publicSize() : F.size(iterator());
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return F.isEmpty(filter) ? map.publicSize() == 0 : !iterator().hasNext();
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            ctx.cache().clearLocally0(new KeySet<>(map, filter), CU.<K, V>empty());
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(ctx);
            out.writeObject(filter);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            ctx = (GridCacheContext<K, V>)in.readObject();
            filter = (IgnitePredicate<CacheEntry<K, V>>[])in.readObject();
        }

        /**
         * Reconstructs object on unmarshalling.
         *
         * @return Reconstructed object.
         * @throws ObjectStreamException Thrown in case of unmarshalling error.
         */
        protected Object readResolve() throws ObjectStreamException {
            return new Set0<>(ctx.cache().map(), filter);
        }
    }

    /**
     * Iterator over hash table.
     * <p>
     * Note, class is static for {@link Externalizable}.
     */
    private static class EntryIterator<K, V> implements Iterator<CacheEntry<K, V>>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Base iterator. */
        private Iterator0<K, V> it;

        /** */
        private GridCacheContext<K, V> ctx;

        /** */
        private GridCacheProjectionImpl<K, V> prjPerCall;

        /** */
        private CacheFlag[] forcedFlags;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public EntryIterator() {
            // No-op.
        }

        /**
         * @param map Cache map.
         * @param filter Entry filter.
         * @param ctx Cache context.
         * @param prjPerCall Projection per call.
         * @param forcedFlags Forced flags.
         */
        EntryIterator(
            GridCacheConcurrentMap<K, V> map,
            IgnitePredicate<CacheEntry<K, V>>[] filter,
            GridCacheContext<K, V> ctx,
            GridCacheProjectionImpl<K, V> prjPerCall,
            CacheFlag[] forcedFlags) {
            it = new Iterator0<>(map, false, filter, -1, -1);

            this.ctx = ctx;
            this.prjPerCall = prjPerCall;
            this.forcedFlags = forcedFlags;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public CacheEntry<K, V> next() {
            GridCacheProjectionImpl<K, V> oldPrj = ctx.projectionPerCall();

            ctx.projectionPerCall(prjPerCall);

            CacheFlag[] oldFlags = ctx.forceFlags(forcedFlags);

            try {
                return it.next().wrap(true);
            }
            finally {
                ctx.projectionPerCall(oldPrj);
                ctx.forceFlags(oldFlags);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            it.remove();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(it);
            out.writeObject(ctx);
            out.writeObject(prjPerCall);
            out.writeObject(forcedFlags);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            it = (Iterator0<K, V>)in.readObject();
            ctx = (GridCacheContext<K, V>)in.readObject();
            prjPerCall = (GridCacheProjectionImpl<K, V>)in.readObject();
            forcedFlags = (CacheFlag[])in.readObject();
        }
    }

    /**
     * Value iterator.
     * <p>
     * Note that class is static for {@link Externalizable}.
     */
    private static class ValueIterator<K, V> implements Iterator<V>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Hash table iterator. */
        private Iterator0<K, V> it;

        /** Context. */
        private GridCacheContext<K, V> ctx;

        /** */
        private boolean clone;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public ValueIterator() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Value filter.
         * @param ctx Cache context.
         * @param clone Clone flag.
         */
        private ValueIterator(
            GridCacheConcurrentMap<K, V> map,
            IgnitePredicate<CacheEntry<K, V>>[] filter,
            GridCacheContext<K, V> ctx,
            boolean clone) {
            it = new Iterator0<>(map, true, filter, -1, -1);

            this.ctx = ctx;
            this.clone = clone;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Nullable @Override public V next() {
            it.next();

            // Cached value.
            V val = it.currentValue();

            try {
                return clone ? ctx.cloneValue(val) : val;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            it.remove();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(it);
            out.writeObject(ctx);
            out.writeBoolean(clone);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            it = (Iterator0)in.readObject();
            ctx = (GridCacheContext<K, V>)in.readObject();
            clone = in.readBoolean();
        }
    }

    /**
     * Key iterator.
     */
    private static class KeyIterator<K, V> implements Iterator<K>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Hash table iterator. */
        private Iterator0<K, V> it;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public KeyIterator() {
            // No-op.
        }

        /**
         * @param map Cache map.
         * @param filter Filter.
         */
        private KeyIterator(GridCacheConcurrentMap<K, V> map, IgnitePredicate<CacheEntry<K, V>>[] filter) {
            it = new Iterator0<>(map, false, filter, -1, -1);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public K next() {
            return it.next().key();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            it.remove();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(it);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            it = (Iterator0)in.readObject();
        }
    }

    /**
     * Key set.
     */
    private static class KeySet<K, V> extends AbstractSet<K> implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Base entry set. */
        private Set0<K, V> set;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public KeySet() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Key filter.
         */
        private KeySet(GridCacheConcurrentMap<K, V> map, IgnitePredicate<CacheEntry<K, V>>[] filter) {
            assert map != null;

            set = new Set0<>(map, nonInternal(filter));
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<K> iterator() {
            return set.keyIterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return set.size();
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean contains(Object o) {
            return set.containsKey((K)o);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean remove(Object o) {
            return set.removeKey((K)o);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            set.clear();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(set);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            set = (Set0<K, V>)in.readObject();
        }
    }

    /**
     * Value set.
     * <p>
     * Note that the set is static for {@link Externalizable} support.
     */
    private static class Values<K, V> extends AbstractCollection<V> implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Base entry set. */
        private Set0<K, V> set;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public Values() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Value filter.
         */
        private Values(GridCacheConcurrentMap<K, V> map, IgnitePredicate<CacheEntry<K, V>>[] filter) {
            assert map != null;

            set = new Set0<>(map, nonInternal(filter));
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<V> iterator() {
            return set.valueIterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return set.size();
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean contains(Object o) {
            return set.containsValue((V)o);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            set.clear();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(set);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            set = (Set0<K, V>)in.readObject();
        }
    }

    /**
     * Entry set.
     */
    private static class EntrySet<K, V> extends AbstractSet<CacheEntry<K, V>> implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Base entry set. */
        private Set0<K, V> set;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public EntrySet() {
            // No-op.
        }

        /**
         * @param map Base map.
         * @param filter Key filter.
         */
        private EntrySet(GridCacheConcurrentMap<K, V> map, IgnitePredicate<CacheEntry<K, V>>[] filter) {
            this(map, filter, false);
        }

        /**
         * @param map Base map.
         * @param filter Key filter.
         * @param internal Whether to allow internal entries.
         */
        private EntrySet(GridCacheConcurrentMap<K, V> map, IgnitePredicate<CacheEntry<K, V>>[] filter,
            boolean internal) {
            assert map != null;

            set = new Set0<>(map, internal ? filter : nonInternal(filter));
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<CacheEntry<K, V>> iterator() {
            return set.entryIterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return set.size();
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean contains(Object o) {
            if (o instanceof GridCacheEntryImpl) {
                GridCacheEntryEx<K, V> unwrapped = ((GridCacheEntryImpl<K, V>)o).unwrapNoCreate();

                return unwrapped != null && set.contains(unwrapped);
            }

            return false;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean remove(Object o) {
            return set.removeKey((K)o);
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            set.clear();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(set);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            set = (Set0<K, V>)in.readObject();
        }
    }
}
