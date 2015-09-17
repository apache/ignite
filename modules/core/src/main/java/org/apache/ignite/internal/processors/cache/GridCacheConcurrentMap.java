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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.LongAdder8;

/**
 * Concurrent implementation of cache map.
 */
public class GridCacheConcurrentMap {
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
    private GridCacheMapEntryFactory factory;

    /** Cache context. */
    protected final GridCacheContext ctx;

    /** */
    private final LongAdder8 mapPubSize = new LongAdder8();

    /** */
    private final LongAdder8 mapSize = new LongAdder8();

    /** Filters cache internal entry. */
    private static final CacheEntryPredicate NON_INTERNAL =
        new CacheEntrySerializablePredicate(new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx entry) {
                return !entry.isInternal();
            }
        });

    /** Non-internal predicate array. */
    public static final CacheEntryPredicate[] NON_INTERNAL_ARR = new CacheEntryPredicate[] {NON_INTERNAL};

    /** Filters obsolete cache map entry. */
    private final IgnitePredicate<GridCacheMapEntry> obsolete =
        new P1<GridCacheMapEntry>() {
            @Override public boolean apply(GridCacheMapEntry entry) {
                return entry.obsolete();
            }
        };

    /** Soft iterator queue. */
    private final ReferenceQueue<Iterator0> itQ = new ReferenceQueue<>();

    /** Soft iterator set. */
    private final Map<WeakIterator<KeyCacheObject, CacheObject>, SegmentHeader> itMap =
        new ConcurrentHashMap8<>();

    /**
     * Checks phantom queue.
     */
    private void checkWeakQueue() {
        // If queue is empty, then it is a lock-free volatile read which should happen
        // in most cases. Otherwise queue uses synchronization to poll elements.
        for (Reference<? extends Iterator0> itRef = itQ.poll();
             itRef != null; itRef = itQ.poll()) {
            assert itRef instanceof WeakIterator;

            if (DEBUG)
                X.println("Checking weak queue [itSetSize=" + itMap.size() + ']');

            SegmentHeader lastSeg = removeWeakIterator(itRef);

            // Segment may be null if iterator is empty at creation time.
            if (lastSeg != null)
                lastSeg.onReadEnd();
        }
    }

    /**
     * @param itRef Iterator reference.
     * @return Last segment.
     */
    private SegmentHeader removeWeakIterator(
        Reference<? extends Iterator0> itRef) {
        assert itRef instanceof WeakIterator;

        SegmentHeader hdr = itMap.remove(itRef);

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
    private void addWeakIterator(WeakIterator itRef, SegmentHeader hdr) {
        SegmentHeader prev = itMap.put(itRef, hdr);

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
    private ReferenceQueue<Iterator0> iteratorQueue() {
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
    protected GridCacheConcurrentMap(GridCacheContext ctx, int initCap, float loadFactor,
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
     * @param factory Entries factory.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative or the load factor is non-positive.
     */
    public GridCacheConcurrentMap(GridCacheContext ctx,
        int initCap,
        float loadFactor,
        @Nullable GridCacheMapEntryFactory factory) {
        this(ctx, initCap, loadFactor, DFLT_CONCUR_LEVEL);

        this.factory = factory;
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
    public GridCacheConcurrentMap(GridCacheContext ctx, int initCap) {
        this(ctx, initCap, DFLT_LOAD_FACTOR, DFLT_CONCUR_LEVEL);
    }

    /**
     * Sets factory for entries.
     *
     * @param factory Entry factory.
     */
    public void setEntryFactory(GridCacheMapEntryFactory factory) {
        assert factory != null;

        this.factory = factory;
    }

    /**
     * @return Entries factory.
     */
    public GridCacheMapEntryFactory getEntryFactory() {
        return factory;
    }

    /**
     * @return Non-internal predicate.
     */
    private static CacheEntryPredicate[] nonInternal() {
        return NON_INTERNAL_ARR;
    }

    /**
     * @param filter Filter to add to non-internal-key filter.
     * @return Non-internal predicate.
     */
    private static CacheEntryPredicate[] nonInternal(
        CacheEntryPredicate[] filter) {
        return F.asArray(F0.and0(NON_INTERNAL_ARR, filter));
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
    public void decrementSize(GridCacheMapEntry e) {
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
    public void incrementSize(GridCacheMapEntry e) {
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
    public <K, V> Collection<V> allValues(CacheEntryPredicate[] filter) {
        checkWeakQueue();

        return new Values<>(this, filter);
    }

    /**
     * @return Random entry out of hash map.
     */
    @Nullable public GridCacheMapEntry randomEntry() {
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

            GridCacheMapEntry entry = seg.randomEntry();

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
    @Nullable public GridCacheMapEntry getEntry(Object key) {
        assert key != null;

        checkWeakQueue();

        int hash = hash(key.hashCode());

        return segmentFor(hash).get(key, hash);
    }

    /**
     * @param topVer Topology version.
     * @param key Key.
     * @param val Value.
     * @return Cache entry for corresponding key-value pair.
     */
    public GridCacheMapEntry putEntry(AffinityTopologyVersion topVer, KeyCacheObject key, @Nullable CacheObject val) {
        assert key != null;

        checkWeakQueue();

        int hash = hash(key.hashCode());

        return segmentFor(hash).put(key, hash, val, topVer);
    }

    /**
     * @param topVer Topology version.
     * @param key Key.
     * @param val Value.
     * @param create Create flag.
     * @return Triple where the first element is current entry associated with the key,
     *      the second is created entry and the third is doomed (all may be null).
     */
    public GridTriple<GridCacheMapEntry> putEntryIfObsoleteOrAbsent(
        AffinityTopologyVersion topVer,
        KeyCacheObject key,
        @Nullable CacheObject val,
        boolean create)
    {
        assert key != null;

        checkWeakQueue();

        int hash = hash(key.hashCode());

        return segmentFor(hash).putIfObsolete(key, hash, val, topVer, create);
    }

    /**
     * Copies all of the mappings from the specified map to this map
     * These mappings will replace any mappings that
     * this map had for any of the keys currently in the specified map.
     *
     * @param m mappings to be stored in this map.
     * @throws NullPointerException If the specified map is null.
     */
    public void putAll(Map<KeyCacheObject, CacheObject> m) {
        for (Map.Entry<KeyCacheObject, CacheObject> e : m.entrySet())
            putEntry(AffinityTopologyVersion.NONE, e.getKey(), e.getValue());
    }

    /**
     * Removes passed in entry if it presents in the map.
     *
     * @param e Entry to remove.
     * @return {@code True} if remove happened.
     */
    public boolean removeEntry(GridCacheEntryEx e) {
        assert e != null;

        checkWeakQueue();

        KeyCacheObject key = e.key();

        int hash = hash(key.hashCode());

        return segmentFor(hash).remove(key, hash, same(e)) != null;
    }

    /**
     * @param p Entry to check equality.
     * @return Predicate to filter the same (equal by ==) entry.
     */
    private IgnitePredicate<GridCacheMapEntry> same(final GridCacheEntryEx p) {
        return new P1<GridCacheMapEntry>() {
            @Override public boolean apply(GridCacheMapEntry e) {
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
    @Nullable public GridCacheMapEntry removeEntryIfObsolete(KeyCacheObject key) {
        assert key != null;

        checkWeakQueue();

        int hash = hash(key.hashCode());

        return segmentFor(hash).remove(key, hash, obsolete);
    }

    /**
     * @param filter Filter.
     * @return Set of the mappings contained in this map.
     */
    @SuppressWarnings({"unchecked"})
    public <K, V> Set<Cache.Entry<K, V>> entries(CacheEntryPredicate... filter) {
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
    public <K, V> Set<Cache.Entry<K, V>> entriesx(CacheEntryPredicate... filter) {
        checkWeakQueue();

        return new EntrySet<>(this, filter, true);
    }

    /**
     * Internal entry set, excluding {@link GridCacheInternal} entries.
     *
     * @return Set of the mappings contained in this map.
     */
    public Set<GridCacheEntryEx> entries0() {
        checkWeakQueue();

        return new Set0<>(this, GridCacheConcurrentMap.nonInternal());
    }

    /**
     * Get striped entry iterator.
     *
     * @param id Expected modulo.
     * @param totalCnt Maximum modulo.
     * @return Striped entry iterator.
     */
    public Iterator<GridCacheEntryEx> stripedEntryIterator(int id, int totalCnt) {
        checkWeakQueue();

        return new Iterator0<>(this, false, GridCacheConcurrentMap.nonInternal(), id, totalCnt);
    }

    /**
     * Gets all internal entry set, including {@link GridCacheInternal} entries.
     *
     * @return All internal entry set, including {@link GridCacheInternal} entries.
     */
    public Set<GridCacheEntryEx> allEntries0() {
        checkWeakQueue();

        return new Set0<>(this, CU.empty0());
    }

    /**
     * Key set.
     *
     * @param filter Filter.
     * @return Set of the keys contained in this map.
     */
    public <K, V> Set<K> keySet(CacheEntryPredicate... filter) {
        checkWeakQueue();

        return new KeySet<>(this, filter, false);
    }

    /**
     * Key set including internal keys.
     *
     * @param filter Filter.
     * @return Set of the keys contained in this map.
     */
    public <K, V> Set<K> keySetx(CacheEntryPredicate... filter) {
        checkWeakQueue();

        return new KeySet<>(this, filter, true);
    }

    /**
     * Collection of non-{@code null} values.
     *
     * @param filter Filter.
     * @return Collection view of the values contained in this map.
     */
    public <K, V> Collection<V> values(CacheEntryPredicate... filter) {
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

                SegmentHeader segHdr = seg.hdr;

                GridCacheMapEntry[] tab = segHdr.table();

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
            SegmentHeader segHdr = s.hdr;

            GridCacheMapEntry[] tab = segHdr.table();

            for (GridCacheMapEntry b : tab) {
                if (b != null) {
                    GridCacheMapEntry e = b;

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
        private volatile SegmentHeader hdr;

        /** The number of public elements in this segment's region. */
        private final LongAdder8 pubSize = new LongAdder8();

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

            hdr = new SegmentHeader(initCap, 0, null);

            threshold = (int)(hdr.length() * loadFactor);
        }

        /**
         * Returns properly casted first entry for given hash.
         *
         * @param tbl Table.
         * @param hash Hash.
         * @return Entry for hash.
         */
        @Nullable GridCacheMapEntry getFirst(GridCacheMapEntry[] tbl, int hash) {
            GridCacheMapEntry bin = tbl[hash & (tbl.length - 1)];

            return bin != null ? bin : null;
        }

        /**
         * @return Segment header for read operation.
         */
        private SegmentHeader headerForRead() {
            while (true) {
                SegmentHeader hdr = this.hdr;

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
        @Nullable GridCacheMapEntry get(Object key, int hash) {
            SegmentHeader hdr = headerForRead();

            try {
                if (hdr.size() != 0) {
                    GridCacheMapEntry e = getFirst(hdr.table(), hash);

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
            SegmentHeader hdr = headerForRead();

            try {
                if (hdr.size() != 0) {
                    GridCacheMapEntry e = getFirst(hdr.table(), hash);

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
         * @return Associated value.
         */
        @SuppressWarnings({"unchecked"})
        GridCacheMapEntry put(KeyCacheObject key, int hash, @Nullable CacheObject val, AffinityTopologyVersion topVer) {
            lock();

            try {
                return put0(key, hash, val, topVer);
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
         * @return Associated value.
         */
        @SuppressWarnings({"unchecked", "SynchronizationOnLocalVariableOrMethodParameter"})
        private GridCacheMapEntry put0(KeyCacheObject key, int hash, CacheObject val, AffinityTopologyVersion topVer) {
            try {
                SegmentHeader hdr = this.hdr;

                int c = hdr.size();

                if (c++ > threshold) {// Ensure capacity.
                    rehash();

                    hdr = this.hdr;
                }

                int hdrId = hdr.id();

                GridCacheMapEntry[] tab = hdr.table();

                int idx = hash & (tab.length - 1);

                GridCacheMapEntry bin = tab[idx];

                GridCacheMapEntry e = bin;

                while (e != null && (e.hash() != hash || !key.equals(e.key)))
                    e = e.next(hdrId);

                GridCacheMapEntry retVal;

                if (e != null) {
                    retVal = e;

                    e.rawPut(val, 0);
                }
                else {
                    GridCacheMapEntry next = bin != null ? bin : null;

                    GridCacheMapEntry newRoot = factory.create(ctx, topVer, key, hash, val, next, hdr.id());

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
         * @param create Create flag.
         * @return Triple where the first element is current entry associated with the key,
         *      the second is created entry and the third is doomed (all may be null).
         */
        @SuppressWarnings( {"unchecked"})
        GridTriple<GridCacheMapEntry> putIfObsolete(KeyCacheObject key,
            int hash,
            @Nullable CacheObject val,
            AffinityTopologyVersion topVer,
            boolean create)
        {
            lock();

            try {
                SegmentHeader hdr = this.hdr;

                int hdrId = hdr.id();

                GridCacheMapEntry[] tab = hdr.table();

                int idx = hash & (tab.length - 1);

                GridCacheMapEntry bin = tab[idx];

                GridCacheMapEntry cur = null;
                GridCacheMapEntry created = null;
                GridCacheMapEntry doomed = null;

                if (bin == null) {
                    if (create)
                        cur = created = put0(key, hash, val, topVer);

                    return new GridTriple<>(cur, created, doomed);
                }

                GridCacheMapEntry e = bin;

                while (e != null && (e.hash() != hash || !key.equals(e.key)))
                    e = e.next(hdrId);

                if (e != null) {
                    if (e.obsolete()) {
                        doomed = remove(key, hash, null);

                        if (create)
                            cur = created = put0(key, hash, val, topVer);
                    }
                    else
                        cur = e;
                }
                else if (create)
                    cur = created = put0(key, hash, val, topVer);

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
            SegmentHeader oldHdr = hdr;

            if (oldHdr.previous() != null && oldHdr.previous().hasReads())
                return; // Wait for previous header to free up.

            int oldId = hdr.id();

            GridCacheMapEntry[] oldTbl = oldHdr.table();

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
            SegmentHeader newHdr = new SegmentHeader(oldCap << 1, oldId + 1, oldHdr);

            oldHdr.next(newHdr); // Link.

            newHdr.size(oldHdr.size());

            GridCacheMapEntry[] newTbl = newHdr.table();

            threshold = (int)(newTbl.length * loadFactor);

            int sizeMask = newTbl.length - 1;

            for (GridCacheMapEntry bin1 : oldTbl) {
                // Relink all nodes.
                for (GridCacheMapEntry e = bin1; e != null; e = e.next(oldId)) {
                    int idx = e.hash() & sizeMask;

                    GridCacheMapEntry bin2 = newTbl[idx];

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
        @Nullable GridCacheMapEntry remove(Object key, int hash,
            @Nullable IgnitePredicate<GridCacheMapEntry> filter) {
            lock();

            try {
                SegmentHeader hdr = this.hdr;

                GridCacheMapEntry[] tbl = hdr.table();

                int idx = hash & (tbl.length - 1);

                GridCacheMapEntry bin = tbl[idx];

                if (bin == null)
                    return null;

                GridCacheMapEntry prev = null;
                GridCacheMapEntry e = bin;

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
        @Nullable GridCacheMapEntry randomEntry() {
            SegmentHeader hdr = headerForRead();

            try {
                GridCacheMapEntry[] tbl = hdr.table();

                Collection<GridCacheMapEntry> entries = new ArrayList<>(3);

                int pubCnt = 0;

                int start = RAND.nextInt(tbl.length);

                for (int i = start; i < start + tbl.length; i++) {
                    GridCacheMapEntry first = tbl[i % tbl.length];

                    if (first == null)
                        continue;

                    entries.add(first);

                    for (GridCacheMapEntry e = first; e != null; e = e.next(hdr.id()))
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

                GridCacheMapEntry retVal = null;

                for (GridCacheMapEntry e : entries) {
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
            SegmentHeader hdr = this.hdr;

            GridCacheMapEntry[] tbl = hdr.table();

            int cnt = 0;
            int pubCnt = 0;

            for (GridCacheMapEntry b : tbl) {
                if (b != null) {
                    GridCacheMapEntry e = b;

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
    private static class SegmentHeader {
        /** Entry table. */
        private final GridCacheMapEntry[] tbl;

        /** Id for rehash. */
        private final int id;

        /** Reads. */
        private final LongAdder8 reads = new LongAdder8();

        /** */
        private volatile SegmentHeader prev;

        /** */
        private volatile SegmentHeader next;

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
        private SegmentHeader(int size, int id, @Nullable SegmentHeader prev) {
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
                    for (GridCacheMapEntry bin : tbl) {
                        if (bin != null) {
                            for (GridCacheMapEntry e = bin; e != null; ) {
                                GridCacheMapEntry next = e.next(id);

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
        GridCacheMapEntry[] table() {
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
        SegmentHeader next() {
            return next;
        }

        /**
         * @param next Next header.
         */
        void next(SegmentHeader next) {
            this.next = next;
        }

        /**
         * @return Previous header.
         */
        SegmentHeader previous() {
            return prev;
        }

        /**
         * @param prev Previous header.
         */
        void previous(SegmentHeader prev) {
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
        WeakIterator(Iterator0<K, V> ref, ReferenceQueue<Iterator0> q) {
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
    private static class Iterator0<K, V> implements Iterator<GridCacheEntryEx>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int nextSegIdx;

        /** */
        private int nextTblIdx;

        /** Segment header. */
        private SegmentHeader curSegHdr;

        /** */
        private GridCacheMapEntry[] curTbl;

        /** */
        private GridCacheMapEntry nextEntry;

        /** Next entry to return. */
        private GridCacheMapEntry next;

        /** Next value. */
        private V nextVal;

        /** Current value. */
        private V curVal;

        /** */
        private boolean isVal;

        /** Current entry. */
        private GridCacheMapEntry cur;

        /** Iterator filter. */
        private CacheEntryPredicate[] filter;

        /** Outer cache map. */
        private GridCacheConcurrentMap map;

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
        Iterator0(GridCacheConcurrentMap map, boolean isVal,
            CacheEntryPredicate[] filter, int id, int totalCnt) {
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
                GridCacheMapEntry bucket = curTbl[nextTblIdx--];

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
                        GridCacheMapEntry bucket = curTbl[j];

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
        private boolean advanceInBucket(@Nullable GridCacheMapEntry e, boolean skipFirst) {
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
                        nextVal = next.<K, V>wrap().getValue();

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
        @Override public GridCacheEntryEx next() {
            GridCacheMapEntry e = next;
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

            GridCacheMapEntry e = cur;

            cur = null;
            curVal = null;

            try {
                ((IgniteKernal)ctx.grid()).getCache(ctx.name()).getAndRemove(e.key());
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
            filter = (CacheEntryPredicate[])in.readObject();
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
    private static class Set0<K, V> extends AbstractSet<GridCacheEntryEx> implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Filter. */
        private CacheEntryPredicate[] filter;

        /** Base map. */
        private GridCacheConcurrentMap map;

        /** Context. */
        private GridCacheContext<K, V> ctx;

        /** */
        private CacheOperationContext opCtxPerCall;

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
        private Set0(GridCacheConcurrentMap map, CacheEntryPredicate[] filter) {
            assert map != null;

            this.map = map;
            this.filter = filter;

            ctx = map.ctx;

            opCtxPerCall = ctx.operationContextPerCall();
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<GridCacheEntryEx> iterator() {
            return new Iterator0<>(map, false, filter, -1, -1);
        }

        /**
         * @return Entry iterator.
         */
        Iterator<Cache.Entry<K, V>> entryIterator() {
            return new EntryIterator<>(map, filter, ctx, opCtxPerCall);
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
            return new ValueIterator<>(map, filter, ctx);
        }

        /**
         * Checks for key containment.
         *
         * @param k Key to check.
         * @return {@code True} if key is in the map.
         */
        boolean containsKey(K k) {
            KeyCacheObject cacheKey = ctx.toCacheKeyObject(k);

            GridCacheEntryEx e = ctx.cache().peekEx(cacheKey);

            try {
                return e != null && !e.obsolete() &&
                    (!e.deleted() || e.lockedByThread()) &&
                    F.isAll(e, filter);
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

            GridCacheEntryEx e = (GridCacheEntryEx)o;

            GridCacheEntryEx cur = ctx.cache().peekEx(e.key());

            return cur != null && cur.equals(e);
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            return o instanceof Cache.Entry && removeKey(((Map.Entry<K, V>)o).getKey());
        }

        /**
         * @param k Key to remove.
         * @return If key has been removed.
         */
        boolean removeKey(K k) {
            try {
                return ((IgniteKernal)ctx.grid()).getCache(ctx.name()).remove(k, CU.<K, V>empty());
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
            ctx.cache().clearLocally0(new KeySet<K, V>(map, filter, false));
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(ctx);
            out.writeObject(filter);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            ctx = (GridCacheContext<K, V>)in.readObject();
            filter = (CacheEntryPredicate[])in.readObject();
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
    private static class EntryIterator<K, V> implements Iterator<Cache.Entry<K, V>>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Base iterator. */
        private Iterator0<K, V> it;

        /** */
        private GridCacheContext<K, V> ctx;

        /** */
        private CacheOperationContext opCtxPerCall;

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
         * @param opCtxPerCall Operation context per call.
         */
        EntryIterator(
            GridCacheConcurrentMap map,
            CacheEntryPredicate[] filter,
            GridCacheContext<K, V> ctx,
            CacheOperationContext opCtxPerCall) {
            it = new Iterator0<>(map, false, filter, -1, -1);

            this.ctx = ctx;
            this.opCtxPerCall = opCtxPerCall;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public Cache.Entry<K, V> next() {
            CacheOperationContext old = ctx.operationContextPerCall();

            ctx.operationContextPerCall(opCtxPerCall);

            try {
                return it.next().wrapLazyValue();
            }
            finally {
                ctx.operationContextPerCall(old);
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
            out.writeObject(opCtxPerCall);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            it = (Iterator0<K, V>)in.readObject();
            ctx = (GridCacheContext<K, V>)in.readObject();
            opCtxPerCall = (CacheOperationContext)in.readObject();
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
         */
        private ValueIterator(
            GridCacheConcurrentMap map,
            CacheEntryPredicate[] filter,
            GridCacheContext ctx) {
            it = new Iterator0<>(map, true, filter, -1, -1);

            this.ctx = ctx;
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

            return val;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            it.remove();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(it);
            out.writeObject(ctx);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            it = (Iterator0)in.readObject();
            ctx = (GridCacheContext<K, V>)in.readObject();
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
        private KeyIterator(GridCacheConcurrentMap map, CacheEntryPredicate[] filter) {
            it = new Iterator0<>(map, false, filter, -1, -1);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public K next() {
            return it.next().key().value(it.ctx.cacheObjectContext(), true);
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
         * @param internal Whether to allow internal keys.
         */
        private KeySet(GridCacheConcurrentMap map, CacheEntryPredicate[] filter, boolean internal) {
            assert map != null;

            set = new Set0<>(map, internal ? filter : nonInternal(filter));
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
        private Values(GridCacheConcurrentMap map, CacheEntryPredicate[] filter) {
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
    private static class EntrySet<K, V> extends AbstractSet<Cache.Entry<K, V>> implements Externalizable {
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
        private EntrySet(GridCacheConcurrentMap map, CacheEntryPredicate[] filter) {
            this(map, filter, false);
        }

        /**
         * @param map Base map.
         * @param filter Key filter.
         * @param internal Whether to allow internal entries.
         */
        private EntrySet(GridCacheConcurrentMap map, CacheEntryPredicate[] filter,
            boolean internal) {
            assert map != null;

            set = new Set0<>(map, internal ? filter : nonInternal(filter));
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Cache.Entry<K, V>> iterator() {
            return set.entryIterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return set.size();
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return set.isEmpty();
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public boolean contains(Object o) {
            if (o instanceof CacheEntryImpl) {
                GridCacheEntryEx unwrapped = set.map.getEntry(((CacheEntryImpl)o).getKey());

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