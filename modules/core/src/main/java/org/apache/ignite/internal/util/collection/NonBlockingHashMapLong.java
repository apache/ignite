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

/*
 * This file is a port of org.jctools.maps.NonBlockingHashMapLong from JCTools
 * (https://github.com/JCTools/JCTools, Apache License 2.0), which is Cliff Click's
 * public-domain high-scale-lib implementation. See the class javadoc for the differences.
 */

package org.apache.ignite.internal.util.collection;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;

/**
 * A lock-free alternate implementation of {@link java.util.concurrent.ConcurrentHashMap} with <strong>primitive
 * {@code long} keys</strong>. The map provides the same correctness properties as {@code ConcurrentHashMap}: all
 * operations are non-blocking and thread-safe, retrievals reflect the results of the most recently
 * <em>completed</em> update operations holding upon their onset, iterators are weakly consistent and never throw
 * {@link java.util.ConcurrentModificationException}, {@code null} values are not allowed.
 * <p>
 * The primitive overloads ({@link #get(long)}, {@link #put(long, Object)}, {@link #remove(long)},
 * {@link #putIfAbsent(long, Object)}, {@link #replace(long, Object)}, {@link #computeIfAbsent(long, LongFunction)},
 * {@link #keySetLong()}, {@link LongKeyIterator#nextLong()}) never box the key. The {@link ConcurrentMap} methods
 * are thin wrappers over them; since auto-boxing is automatic, it is easy to accidentally use the boxed versions and
 * negate the benefits.
 * <p>
 * The table is an open-addressed array of keys and a parallel array of values probed linearly, with no per-entry
 * node allocation. Key slots are claimed once and never change. Value slots move from {@code null} to a value and
 * then between values and {@code TOMBSTONE}. A resize creates a new table and all visiting threads help to copy
 * the old one over; while a slot is being copied its value is boxed into a {@code Prime} so that updates are
 * redirected to the new table.
 * <p>
 * Differences from the JCTools original: {@code sun.misc.Unsafe} is replaced by {@link AtomicLongArray},
 * {@link AtomicReferenceArray} and the field updaters, the striped counter by {@link LongAdder}, the debug printing
 * and the reprobe counter are dropped, {@link #computeIfAbsent(long, LongFunction)} is added, and the code follows
 * the Ignite style.
 *
 * @param <V> Type of the mapped values.
 */
public class NonBlockingHashMapLong<V> extends AbstractMap<Long, V> implements ConcurrentMap<Long, V>, Cloneable,
    Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Too many reprobes then force a table resize. */
    private static final int REPROBE_LIMIT = 10;

    /** Log2 of the minimum table size. */
    private static final int MIN_SIZE_LOG = 4;

    /** Minimum table size, must be a power of 2. */
    private static final int MIN_SIZE = 1 << MIN_SIZE_LOG;

    /**
     * Largest table size that a high key churn rate alone is allowed to grow the table to. Removed keys leave dead
     * slots which are reclaimed only by a resize, so a table with few live keys but a high churn rate is resized
     * repeatedly. Below this size a resize soon after the previous one doubles the table to make resizes rarer;
     * above it the table is copied at the same size, which bounds the memory and the copy cost per operation.
     */
    private static final int MAX_CHURN_SIZE = 1 << 16;

    /** Sentinel: {@code putIfMatch} updates regardless of the old value (a wildcard match). */
    private static final Object NO_MATCH_OLD = new Object();

    /** Sentinel: {@code putIfMatch} updates only if it finds a real (non-deleted) old value. */
    private static final Object MATCH_ANY = new Object();

    /** Sentinel: this K/V pair has been deleted, but the key slot is forever claimed. */
    private static final Object TOMBSTONE = new Object();

    /** Boxed version of {@link #TOMBSTONE}: the pair was deleted and then a resize started. */
    private static final Prime TOMBPRIME = new Prime(TOMBSTONE);

    /**
     * The one {@code long} excluded from the table: it is the initial value of the key array and means
     * "no key inserted yet". Its mapping is kept in {@link #zeroVal} instead.
     */
    private static final long NO_KEY = 0L;

    /** Updater of {@link #tbl}. */
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<NonBlockingHashMapLong, Table> TBL_UPD =
        AtomicReferenceFieldUpdater.newUpdater(NonBlockingHashMapLong.class, Table.class, "tbl");

    /** Updater of {@link #zeroVal}. */
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<NonBlockingHashMapLong, Object> ZERO_VAL_UPD =
        AtomicReferenceFieldUpdater.newUpdater(NonBlockingHashMapLong.class, Object.class, "zeroVal");

    /** The current (top-level) hash table. */
    private transient volatile Table tbl;

    /** Value mapped to {@link #NO_KEY}, {@link #TOMBSTONE} when absent. */
    private transient volatile Object zeroVal;

    /** Time of the last resize. */
    private transient volatile long lastResizeMillis;

    /** Optimize for space: use a half-sized table and allow more reprobes. */
    private final boolean optForSpace;

    /** Creates a new map with the default minimum size. */
    public NonBlockingHashMapLong() {
        this(MIN_SIZE, true);
    }

    /**
     * Creates a new map with initial room for the given number of elements, thus avoiding internal resizing
     * operations to reach an appropriate size. The initial size is rounded up to the next power of 2.
     *
     * @param initSz Initial size.
     */
    public NonBlockingHashMapLong(int initSz) {
        this(initSz, true);
    }

    /**
     * Creates a new map, setting the space-for-speed tradeoff.
     *
     * @param optForSpace {@code True} optimizes for space and is the default, {@code false} optimizes for speed and
     *      doubles space costs for roughly a 10% speed improvement.
     */
    public NonBlockingHashMapLong(boolean optForSpace) {
        this(1, optForSpace);
    }

    /**
     * Creates a new map, setting both the initial size and the space-for-speed tradeoff.
     *
     * @param initSz Initial size.
     * @param optForSpace {@code True} optimizes for space and is the default, {@code false} optimizes for speed and
     *      doubles space costs for roughly a 10% speed improvement.
     */
    public NonBlockingHashMapLong(int initSz, boolean optForSpace) {
        this.optForSpace = optForSpace;

        initialize(initSz);
    }

    /**
     * @param initSz Initial size.
     */
    private void initialize(int initSz) {
        if (initSz < 0)
            throw new IllegalArgumentException("Initial size must be non-negative: " + initSz);

        int log2 = MIN_SIZE_LOG;

        while ((1 << log2) < initSz)
            log2++;

        tbl = new Table(this, new LongAdder(), log2);
        zeroVal = TOMBSTONE;
        lastResizeMillis = System.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return (zeroVal == TOMBSTONE ? 0 : 1) + tbl.size();
    }

    /**
     * @param key Key.
     * @return {@code True} if the key is in the map.
     */
    public boolean containsKey(long key) {
        return get(key) != null;
    }

    /**
     * Maps the specified key to the specified value. The value cannot be {@code null}.
     *
     * @param key Key.
     * @param val Value.
     * @return The previous value associated with the key, or {@code null} if there was no mapping for it.
     * @throws NullPointerException If the value is {@code null}.
     */
    public V put(long key, V val) {
        return putIfMatch(key, val, NO_MATCH_OLD);
    }

    /**
     * Atomically does a {@link #put(long, Object)} if-and-only-if the key is not mapped.
     *
     * @param key Key.
     * @param val Value.
     * @return The previous value associated with the key, or {@code null} if there was no mapping for it.
     * @throws NullPointerException If the value is {@code null}.
     */
    public V putIfAbsent(long key, V val) {
        return putIfMatch(key, val, TOMBSTONE);
    }

    /**
     * Removes the key and its value from the map. Does nothing if the key is not in the map.
     *
     * @param key Key.
     * @return The previous value associated with the key, or {@code null} if there was no mapping for it.
     */
    public V remove(long key) {
        return putIfMatch(key, TOMBSTONE, NO_MATCH_OLD);
    }

    /**
     * Atomically does a {@link #remove(long)} if-and-only-if the key is mapped to a value which is {@code equals}
     * to the given value.
     *
     * @param key Key.
     * @param val Expected value.
     * @return {@code True} if the mapping was removed.
     * @throws NullPointerException If the value is {@code null}.
     */
    public boolean remove(long key, Object val) {
        return putIfMatch(key, TOMBSTONE, val) == val;
    }

    /**
     * Atomically does a {@link #put(long, Object)} if-and-only-if the key is mapped to some value already.
     *
     * @param key Key.
     * @param val Value.
     * @return The previous value associated with the key, or {@code null} if there was no mapping for it.
     * @throws NullPointerException If the value is {@code null}.
     */
    public V replace(long key, V val) {
        return putIfMatch(key, val, MATCH_ANY);
    }

    /**
     * Atomically does a {@link #put(long, Object)} if-and-only-if the key is mapped to a value which is
     * {@code equals} to the expected old value.
     *
     * @param key Key.
     * @param oldVal Expected old value.
     * @param newVal New value.
     * @return {@code True} if the value was replaced.
     * @throws NullPointerException If any value is {@code null}.
     */
    public boolean replace(long key, V oldVal, V newVal) {
        return putIfMatch(key, newVal, oldVal) == oldVal;
    }

    /**
     * If the key is not already mapped, computes its value using the given function and enters it into the map,
     * unless the function returns {@code null}. The function may be invoked concurrently by several threads for
     * the same key; only one of the computed values is kept.
     *
     * @param key Key.
     * @param mappingFn Function to compute a value.
     * @return The current (existing or computed) value, or {@code null} if the computed value is {@code null}.
     */
    public V computeIfAbsent(long key, LongFunction<? extends V> mappingFn) {
        Objects.requireNonNull(mappingFn, "mappingFn");

        V val = get(key);

        if (val != null)
            return val;

        V newVal = mappingFn.apply(key);

        if (newVal == null)
            return null;

        V prev = putIfAbsent(key, newVal);

        return prev != null ? prev : newVal;
    }

    /**
     * @param key Key.
     * @param newVal New value, {@link #TOMBSTONE} to remove.
     * @param oldVal Expected old value, or {@link #NO_MATCH_OLD}, {@link #MATCH_ANY}, {@link #TOMBSTONE}.
     * @return The previous value, or {@code null} if there was none.
     */
    private V putIfMatch(long key, Object newVal, Object oldVal) {
        if (oldVal == null || newVal == null)
            throw new NullPointerException();

        if (key == NO_KEY) {
            Object curVal = zeroVal;

            if (oldVal == NO_MATCH_OLD || // Do we care about the expected value at all?
                curVal == oldVal || // Instant match?
                (oldVal == MATCH_ANY && curVal != TOMBSTONE) ||
                oldVal.equals(curVal)) { // Expensive equals check.
                if (!ZERO_VAL_UPD.compareAndSet(this, curVal, newVal)) // One-shot CAS update attempt.
                    curVal = zeroVal; // Failed; get the failing witness.
            }

            return curVal == TOMBSTONE ? null : (V)curVal;
        }

        Object res = tbl.putIfMatch(key, newVal, oldVal);

        assert !(res instanceof Prime);
        assert res != null;

        return res == TOMBSTONE ? null : (V)res;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        Table newTbl = new Table(this, new LongAdder(), MIN_SIZE_LOG);

        Table cur;

        do {
            cur = tbl;
        }
        while (!TBL_UPD.compareAndSet(this, cur, newTbl));

        zeroVal = TOMBSTONE;
    }

    /**
     * Note: this method requires a full traversal of the table and is much slower than {@link #containsKey(long)}.
     *
     * @param val Value.
     * @return {@code True} if the map maps one or more keys to the value.
     */
    @Override public boolean containsValue(Object val) {
        if (val == null)
            return false;

        if (val == zeroVal)
            return true;

        for (V v : values()) {
            if (v == val || v.equals(val))
                return true;
        }

        return false;
    }

    /**
     * @param key Key.
     * @return The value to which the key is mapped, or {@code null} if there is no mapping for it.
     */
    public final V get(long key) {
        if (key == NO_KEY) {
            Object v = zeroVal;

            return v == TOMBSTONE ? null : (V)v;
        }

        Object v = tbl.get(key);

        assert !(v instanceof Prime);
        assert v != TOMBSTONE;

        return (V)v;
    }

    /** {@inheritDoc} */
    @Override public V get(Object key) {
        return key instanceof Long ? get(((Long)key).longValue()) : null;
    }

    /** {@inheritDoc} */
    @Override public V remove(Object key) {
        return key instanceof Long ? remove(((Long)key).longValue()) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object key, Object val) {
        return key instanceof Long && remove(((Long)key).longValue(), val);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return key instanceof Long && containsKey(((Long)key).longValue());
    }

    /** {@inheritDoc} */
    @Override public V putIfAbsent(Long key, V val) {
        return putIfAbsent(key.longValue(), val);
    }

    /** {@inheritDoc} */
    @Override public V replace(Long key, V val) {
        return replace(key.longValue(), val);
    }

    /** {@inheritDoc} */
    @Override public V put(Long key, V val) {
        return put(key.longValue(), val);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Long key, V oldVal, V newVal) {
        return replace(key.longValue(), oldVal, newVal);
    }

    /**
     * Helps along an existing resize operation. Always helps the top-most table copy, even if there are nested
     * table copies in progress.
     */
    private void helpCopy() {
        Table top = tbl;

        if (top.next == null)
            return; // No copy in progress.

        top.helpCopy(false);
    }

    /**
     * Spreads lousy hash codes.
     *
     * @param h Key.
     * @return Hash.
     */
    private static int hash(long h) {
        h ^= (h >>> 20) ^ (h >>> 12);
        h ^= (h >>> 7) ^ (h >>> 4);
        h += h << 7; // Smear low bits up high, for keys that only differ by 1.

        return (int)h;
    }

    /**
     * @param len Table length.
     * @return Reprobe limit: running over it on a {@code get} acts as a miss, on a {@code put} it can trigger
     *      a table resize.
     */
    private static int reprobeLimit(int len) {
        return REPROBE_LIMIT + (len >> 4);
    }

    /**
     * Returns an enumeration of the values in this map.
     *
     * @return Enumeration of the values.
     * @see #values()
     */
    public Enumeration<V> elements() {
        return new ValIterator();
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map. The collection is backed by the map,
     * so changes to the map are reflected in the collection, and vice-versa. The collection supports element
     * removal, which removes the corresponding mapping from this map, but does not support the {@code add} or
     * {@code addAll} operations. The view's iterator is weakly consistent.
     */
    @Override public Collection<V> values() {
        return new AbstractCollection<V>() {
            /** {@inheritDoc} */
            @Override public void clear() {
                NonBlockingHashMapLong.this.clear();
            }

            /** {@inheritDoc} */
            @Override public int size() {
                return NonBlockingHashMapLong.this.size();
            }

            /** {@inheritDoc} */
            @Override public boolean contains(Object v) {
                return NonBlockingHashMapLong.this.containsValue(v);
            }

            /** {@inheritDoc} */
            @Override public Iterator<V> iterator() {
                return new ValIterator();
            }
        };
    }

    /**
     * Returns an enumeration of the <strong>auto-boxed</strong> keys in this map.
     *
     * @return Enumeration of the boxed keys.
     * @see #keySet()
     */
    public Enumeration<Long> keys() {
        return new LongKeyIterator();
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map; with care the keys may be iterated over
     * <strong>without auto-boxing</strong>: cast the iterator to {@link LongKeyIterator} and use
     * {@link LongKeyIterator#nextLong()}. The set is backed by the map, so changes to the map are reflected in the
     * set, and vice-versa. The set supports element removal, which removes the corresponding mapping from this map,
     * but does not support the {@code add} or {@code addAll} operations. The view's iterator is weakly consistent.
     */
    @Override public Set<Long> keySet() {
        return new AbstractSet<Long>() {
            /** {@inheritDoc} */
            @Override public void clear() {
                NonBlockingHashMapLong.this.clear();
            }

            /** {@inheritDoc} */
            @Override public int size() {
                return NonBlockingHashMapLong.this.size();
            }

            /** {@inheritDoc} */
            @Override public boolean contains(Object k) {
                return NonBlockingHashMapLong.this.containsKey(k);
            }

            /** {@inheritDoc} */
            @Override public boolean remove(Object k) {
                return NonBlockingHashMapLong.this.remove(k) != null;
            }

            /** {@inheritDoc} */
            @Override public LongKeyIterator iterator() {
                return new LongKeyIterator();
            }
        };
    }

    /**
     * @return Keys as a {@code long} array. The array may be zero-padded if keys are concurrently deleted.
     */
    public long[] keySetLong() {
        long[] res = new long[size()];

        LongKeyIterator iter = new LongKeyIterator();

        int i = 0;

        while (i < res.length && iter.hasNext())
            res[i++] = iter.nextLong();

        return res;
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map. The set is backed by the map, so changes
     * to the map are reflected in the set, and vice-versa. The set supports element removal, which removes the
     * corresponding mapping from the map, but does not support the {@code add} or {@code addAll} operations. The
     * view's iterator is weakly consistent.
     * <p>
     * <strong>Warning:</strong> each step of the iterator creates a {@link Map.Entry} and boxes the key.
     * Iterating {@link #keySet()} or {@link #values()} is more efficient.
     */
    @Override public Set<Map.Entry<Long, V>> entrySet() {
        return new AbstractSet<Map.Entry<Long, V>>() {
            /** {@inheritDoc} */
            @Override public void clear() {
                NonBlockingHashMapLong.this.clear();
            }

            /** {@inheritDoc} */
            @Override public int size() {
                return NonBlockingHashMapLong.this.size();
            }

            /** {@inheritDoc} */
            @Override public boolean remove(Object o) {
                if (!(o instanceof Map.Entry))
                    return false;

                Map.Entry<?, ?> e = (Map.Entry<?, ?>)o;

                return NonBlockingHashMapLong.this.remove(e.getKey(), e.getValue());
            }

            /** {@inheritDoc} */
            @Override public boolean contains(Object o) {
                if (!(o instanceof Map.Entry))
                    return false;

                Map.Entry<?, ?> e = (Map.Entry<?, ?>)o;

                V v = get(e.getKey());

                return v != null && v.equals(e.getValue());
            }

            /** {@inheritDoc} */
            @Override public Iterator<Map.Entry<Long, V>> iterator() {
                return new EntryIterator();
            }
        };
    }

    /**
     * @param s Stream.
     * @throws IOException If failed.
     */
    private void writeObject(ObjectOutputStream s) throws IOException {
        s.defaultWriteObject(); // Writes nothing.

        LongKeyIterator iter = new LongKeyIterator();

        while (iter.hasNext()) {
            long k = iter.nextLong();

            Object v = get(k); // Do an official get.

            s.writeLong(k);
            s.writeObject(v);
        }

        s.writeLong(NO_KEY); // Sentinel to indicate end of data.
        s.writeObject(null);
    }

    /**
     * @param s Stream.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If failed.
     */
    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject(); // Reads nothing.

        initialize(MIN_SIZE);

        while (true) {
            long k = s.readLong();
            V v = (V)s.readObject();

            if (k == NO_KEY && v == null)
                break;

            put(k, v); // Insert with an official put.
        }
    }

    /**
     * Creates a shallow copy of this map: the structure is copied, the keys and values are not cloned.
     * This is a relatively expensive operation.
     *
     * @return A clone of the map.
     */
    @Override public NonBlockingHashMapLong<V> clone() {
        try {
            // Must clone to get the class right: the map might have been extended.
            NonBlockingHashMapLong<V> t = (NonBlockingHashMapLong<V>)super.clone();

            // There is no atomic clone operation: the table is undergoing rapid change.
            // Wipe out the shallow-copied structure and copy sanely.
            t.clear();

            for (long k : keySetLong())
                t.put(k, get(k));

            return t;
        }
        catch (CloneNotSupportedException e) {
            throw new InternalError(e); // Can not happen: Cloneable.
        }
    }

    /** Wrapper adding a 'prime' bit to a value: the slot is being copied to the new table. */
    private static final class Prime {
        /** Boxed value. */
        private final Object val;

        /**
         * @param val Value.
         */
        private Prime(Object val) {
            this.val = val;
        }
    }

    /** One generation of the hash table: the open-addressed key and value arrays and the resize machinery. */
    private static final class Table {
        /** Updater of {@link #next}. */
        private static final AtomicReferenceFieldUpdater<Table, Table> NEXT_UPD =
            AtomicReferenceFieldUpdater.newUpdater(Table.class, Table.class, "next");

        /** Updater of {@link #resizers}. */
        private static final AtomicLongFieldUpdater<Table> RESIZERS_UPD =
            AtomicLongFieldUpdater.newUpdater(Table.class, "resizers");

        /** Updater of {@link #copyIdx}. */
        private static final AtomicLongFieldUpdater<Table> COPY_IDX_UPD =
            AtomicLongFieldUpdater.newUpdater(Table.class, "copyIdx");

        /** Updater of {@link #copyDone}. */
        private static final AtomicLongFieldUpdater<Table> COPY_DONE_UPD =
            AtomicLongFieldUpdater.newUpdater(Table.class, "copyDone");

        /** Back-pointer to the top-level structure. */
        private final NonBlockingHashMapLong<?> map;

        /** Size in active K/V pairs. Shared with the next table, since a copy does not change the size. */
        private final LongAdder size;

        /**
         * Count of used key slots, to tell when the table is full of dead unusable slots. When it nears a large
         * fraction of the table we probably end up reprobing too much.
         */
        private final LongAdder slots;

        /** Keys. {@link #NO_KEY} means the slot is free. */
        private final AtomicLongArray keys;

        /** Values. {@code null} means never set, {@link #TOMBSTONE} deleted, {@link Prime} being copied. */
        private final AtomicReferenceArray<Object> vals;

        /**
         * The next table, created during a resize operation: the new table being copied from this one. It is the
         * volatile variable read as we cross from one table to the next, to get the required memory orderings.
         * Monotonically transits from {@code null} to set, once.
         */
        private volatile Table next;

        /** Count of threads attempting an initial resize, to throttle allocation of large junk tables by losers. */
        private volatile long resizers;

        /**
         * The next part of the table to copy. Monotonically transits from zero to {@code 2 * keys.length()}.
         * Visitors claim work chunks by CAS'ing it up, then copy the indicated slots to the new table. Workers are
         * not required to finish any chunk: the counter wraps and the work is copied repeatedly until somebody
         * completes the count.
         */
        private volatile long copyIdx;

        /** Count of slots confirmed copied to the new table, from zero to {@code keys.length()}. */
        private volatile long copyDone;

        /**
         * @param map Map.
         * @param size Size counter.
         * @param logSz Log2 of the table size.
         */
        Table(NonBlockingHashMapLong<?> map, LongAdder size, int logSz) {
            this.map = map;
            this.size = size;

            slots = new LongAdder();
            keys = new AtomicLongArray(1 << logSz);
            vals = new AtomicReferenceArray<>(1 << logSz);
        }

        /**
         * @return Size in active K/V pairs.
         */
        int size() {
            return (int)size.sum();
        }

        /**
         * @param newTbl New table.
         * @return {@code True} if this thread installed the new table.
         */
        private boolean casNext(Table newTbl) {
            return NEXT_UPD.compareAndSet(this, null, newTbl);
        }

        /**
         * @param idx Slot index.
         * @param old Expected key.
         * @param key New key.
         * @return {@code True} if the key was set.
         */
        private boolean casKey(int idx, long old, long key) {
            return keys.compareAndSet(idx, old, key);
        }

        /**
         * @param idx Slot index.
         * @param old Expected value.
         * @param val New value.
         * @return {@code True} if the value was set.
         */
        private boolean casVal(int idx, Object old, Object val) {
            return vals.compareAndSet(idx, old, val);
        }

        /**
         * @param key Key.
         * @return Value, never a {@link Prime} nor a {@link #TOMBSTONE}, {@code null} if absent.
         */
        private Object get(long key) {
            int hash = hash(key);
            int len = keys.length();
            int idx = hash & (len - 1);

            int reprobeCnt = 0;

            // Main spin/reprobe loop, looking for a key hit.
            while (true) {
                long k = keys.get(idx);
                Object v = vals.get(idx);

                if (k == NO_KEY)
                    return null; // A clear miss.

                if (k == key) {
                    // Key hit! No table copy in progress?
                    if (!(v instanceof Prime))
                        return v == TOMBSTONE ? null : v;

                    // Key hit, but the slot is (possibly partially) copied to the new table.
                    // Finish the copy and retry in the new table.
                    return copySlotAndCheck(idx, true).get(key);
                }

                // 'get' and 'put' must have the same key lookup logic, but only 'put' needs to force
                // a table resize for a too-long key reprobe sequence.
                if (++reprobeCnt >= reprobeLimit(len)) {
                    return next == null // Table copy in progress?
                        ? null // No: a clear miss.
                        : copySlotAndCheck(idx, true).get(key); // Retry in the new table.
                }

                idx = (idx + 1) & (len - 1); // Reprobe by 1.
            }
        }

        /**
         * Put, remove, putIfAbsent, etc. If the returned value is equal to {@code expVal} (or {@code expVal} is
         * {@link #NO_MATCH_OLD}) then the put can be assumed to have worked, although it might have been
         * immediately overwritten. Only the path through {@link #copySlot(int)} passes in an expected value of
         * {@code null}, and only then {@code null} can be returned.
         *
         * @param key Key.
         * @param putVal New value, {@link #TOMBSTONE} to remove.
         * @param expVal Expected old value, a sentinel, or {@code null} from the copy path.
         * @return The old value.
         */
        private Object putIfMatch(long key, Object putVal, Object expVal) {
            assert putVal != null;
            assert !(putVal instanceof Prime);
            assert !(expVal instanceof Prime);

            int hash = hash(key);
            int len = keys.length();
            int idx = hash & (len - 1);

            int reprobeCnt = 0;
            long k;
            Object v;

            // Key-claim stanza: spin till we can claim a key slot or force a resize.
            while (true) {
                v = vals.get(idx);
                k = keys.get(idx);

                if (k == NO_KEY) {
                    // Found an empty key slot, which means this key has never been in this table.
                    if (putVal == TOMBSTONE)
                        return TOMBSTONE; // Not now and never been in this table: nothing to remove.

                    if (expVal == MATCH_ANY)
                        return TOMBSTONE; // Will not match, even after the key is inserted.

                    if (casKey(idx, NO_KEY, key)) { // Claim the slot for the key.
                        slots.increment();

                        break;
                    }

                    // CAS failed: somebody else claimed the slot. Keys never change, so re-reading is safe.
                    k = keys.get(idx);

                    assert k != NO_KEY;
                }

                if (k == key)
                    break; // Got it!

                if (++reprobeCnt >= reprobeLimit(len)) {
                    // We simply must have a new table to do a put. At this point a get will also go to the
                    // new table (if any). We can not claim a key slot: there is no free one to claim.
                    Table newTbl = resize();

                    if (expVal != null)
                        map.helpCopy(); // Help along an existing copy.

                    return newTbl.putIfMatch(key, putVal, expVal);
                }

                idx = (idx + 1) & (len - 1); // Reprobe.
            }

            // Found the proper key slot, now update the matching value slot. We never put a null, so value slots
            // monotonically move from null to not-null (deleted values use TOMBSTONE).
            while (true) {
                if (putVal == v)
                    return v; // Fast cutout for no change.

                // See if we want to move to a new table to avoid high average reprobe counts. We only check on
                // the initial set of a value from null to not-null, i.e. once per key insert. Or we found
                // a Prime: a resize is already in progress.
                if ((v == null && tableFull(reprobeCnt, len)) || v instanceof Prime) {
                    resize(); // Force the new table copy to start.

                    return copySlotAndCheck(idx, expVal != null).putIfMatch(key, putVal, expVal);
                }

                // Must match the old value, and we do not? Then bail out now. Either v or expVal might be
                // TOMBSTONE, v can be null if a value has never been inserted, expVal can be null from copySlot.
                if (expVal != NO_MATCH_OLD && // Do we care about the expected value at all?
                    v != expVal && // Instant match?
                    (expVal != MATCH_ANY || v == TOMBSTONE || v == null) &&
                    !(v == null && expVal == TOMBSTONE) && // Match on the null/TOMBSTONE combo.
                    (expVal == null || !expVal.equals(v))) // Expensive equals check at the last.
                    return v == null ? TOMBSTONE : v; // Do not update!

                if (casVal(idx, v, putVal))
                    break; // We did the update!

                // CAS failed. Because we have no witness, we do not know why. Re-read the value: if a Prime got
                // installed, re-run the put on the new table, otherwise we lost the CAS to another racing
                // put and simply retry.
                v = vals.get(idx);

                if (v instanceof Prime)
                    return copySlotAndCheck(idx, expVal != null).putIfMatch(key, putVal, expVal);
            }

            // Both normal puts and the table copy call putIfMatch, but the table copy does not change the number
            // of live K/V pairs.
            if (expVal != null) {
                if ((v == null || v == TOMBSTONE) && putVal != TOMBSTONE)
                    size.increment();

                if (!(v == null || v == TOMBSTONE) && putVal == TOMBSTONE)
                    size.decrement();
            }

            return (v == null && expVal != null) ? TOMBSTONE : v;
        }

        /**
         * Heuristic to decide if this table is too full and we should start a new one. If a get has reprobed too
         * many times and decided the table must be full, then the slot count must be high and we must report the
         * table is full too, otherwise a put could insert into this table a key which a get would not find.
         *
         * @param reprobeCnt Reprobe count of the current put.
         * @param len Table length.
         * @return {@code True} if the table is full.
         */
        private boolean tableFull(int reprobeCnt, int len) {
            // Cheap check first: some number of reprobes is always allowed.
            return reprobeCnt >= REPROBE_LIMIT &&
                (reprobeCnt >= reprobeLimit(len) ||
                    slots.sum() >= (len >> 1)); // More expensive check: is the table more than half full?
        }

        /**
         * Starts a resize after too many probes, or returns the resize already in progress. Callers must
         * {@link NonBlockingHashMapLong#helpCopy()} afterwards, lest a path forever runs through this method only
         * to discover a copy in progress which never progresses.
         *
         * @return The new table.
         */
        private Table resize() {
            Table newTbl = next; // Volatile read.

            if (newTbl != null)
                return newTbl; // Resize is already in progress: use the new table.

            // No copy in progress, so start one. First: compute the new table size.
            int oldLen = keys.length();
            int sz = size();
            int newSz = sz;

            // We expect plenty of dead slots with keys and need some decent padding to avoid endless reprobing.
            if (map.optForSpace) {
                // This heuristic leads to a much denser table with a higher reprobe rate.
                if (sz >= (oldLen >> 1)) // More than 50% full of keys?
                    newSz = oldLen << 1; // Double the size.
            }
            else {
                if (sz >= (oldLen >> 2)) { // More than 25% full of keys?
                    newSz = oldLen << 1; // Double the size.

                    if (sz >= (oldLen >> 1)) // More than 50% full of keys?
                        newSz = oldLen << 2; // Quadruple the size.
                }
            }

            // Last resize was very recent? Then double again despite having few live keys: this slows down
            // resizes of tables with a high key churn rate, which need a steady state of rare same-size
            // resizes to clean out the dead keys. The doubling is bounded by MAX_CHURN_SIZE (unlike the JCTools
            // original), otherwise a table with a put/remove churn of millions of keys per second grows
            // without limit and every resize copies ever larger arrays.
            long now = System.currentTimeMillis();

            if (newSz <= oldLen && oldLen < MAX_CHURN_SIZE && now <= map.lastResizeMillis + 10_000)
                newSz = oldLen << 1;

            // Never shrink: if we hit this size once, assume we will again.
            if (newSz < oldLen)
                newSz = oldLen;

            int log2 = MIN_SIZE_LOG;

            while ((1 << log2) < newSz)
                log2++;

            long len = ((1L << log2) << 1) + 2;

            // Prevent integer overflow: 2^30 + 2 is the largest number of elements in the table.
            if ((int)len != len) {
                log2 = 30;
                len = (1L << log2) + 2;

                if (sz > ((len >> 2) + (len >> 1)))
                    throw new IllegalStateException("Table is full.");
            }

            // Limit the number of threads actually allocating memory to a handful, lest hundreds of threads
            // all try to allocate a giant resized array.
            long r = resizers;

            while (!RESIZERS_UPD.compareAndSet(this, r, r + 1))
                r = resizers;

            // Size estimate: 2 words (K+V) per table entry plus a handful, assuming 64-bit pointers.
            long megs = ((((1L << log2) << 1) + 8) << 3) >> 20;

            if (r >= 2 && megs > 0) { // Already two threads trying: wait a tad and see.
                newTbl = next;

                if (newTbl != null)
                    return newTbl;

                try {
                    Thread.sleep(megs);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // Last check, since the allocation below is expensive and another thread may have slipped in.
            newTbl = next;

            if (newTbl != null)
                return newTbl;

            newTbl = new Table(map, size, log2); // Actually allocate the big arrays.

            if (next != null) // Another check after the slow allocation.
                return next;

            // The new table must be CAS'ed in so there is only one winner among racing resizers.
            // Extra tables are garbage collected.
            if (!casNext(newTbl))
                newTbl = next;

            return newTbl;
        }

        /**
         * Helps along an existing resize operation. We hope it is the top-level copy (it was when we started) but
         * this table might have been promoted out of the top position.
         *
         * @param copyAll {@code True} to copy until done, {@code false} to do one chunk of work.
         */
        private void helpCopy(boolean copyAll) {
            assert next != null; // Already checked by the caller.

            int oldLen = keys.length();
            int minCopyWork = Math.min(oldLen, 1024); // Limit per-thread work.

            int panicStart = -1;
            int start = -1;

            while (copyDone < oldLen) { // Still needing to copy?
                // Carve out a chunk of work. The counter wraps around so every thread eventually tries to copy
                // every slot repeatedly. We 'panic' if we have tried twice to copy every slot and it still has
                // not happened: some thread claimed a slot but never finished. Then we do the copy ourselves.
                if (panicStart == -1) {
                    start = (int)copyIdx;

                    while (start < (oldLen << 1) && !COPY_IDX_UPD.compareAndSet(this, start, start + minCopyWork))
                        start = (int)copyIdx;

                    if (!(start < (oldLen << 1)))
                        panicStart = start; // Record where we started to panic-copy.
                }

                int workDone = 0;

                for (int i = 0; i < minCopyWork; i++) {
                    if (copySlot((start + i) & (oldLen - 1))) // Made an old-table slot go dead?
                        workDone++;
                }

                if (workDone > 0)
                    copyCheckAndPromote(workDone); // See if we can promote.

                start += minCopyWork;

                if (!copyAll && panicStart == -1)
                    return; // Done copying after one chunk of work.
            }

            // Extra promotion check, in case another thread finished all copying but stalled before promoting.
            copyCheckAndPromote(0);
        }

        /**
         * Copies the slot from this table to the new one. If this thread confirmed the copy, updates the counters
         * and checks for promotion. Called with one-shot copy requests, typically because the caller found a
         * {@link Prime} and has to retry its operation in the new table.
         *
         * @param idx Slot index.
         * @param help {@code True} to also help along the whole copy.
         * @return The new table.
         */
        private Table copySlotAndCheck(int idx, boolean help) {
            assert next != null; // We are only here because the caller saw a Prime.

            if (copySlot(idx)) // Copy the desired slot.
                copyCheckAndPromote(1); // Record the slot copied.

            if (help)
                map.helpCopy();

            return next;
        }

        /**
         * @param workDone Number of slots this thread confirmed copied.
         */
        private void copyCheckAndPromote(int workDone) {
            int oldLen = keys.length();

            long done = copyDone;
            long nowDone = done + workDone;

            assert nowDone <= oldLen;

            if (workDone > 0) {
                while (!COPY_DONE_UPD.compareAndSet(this, done, nowDone)) {
                    done = copyDone;
                    nowDone = done + workDone;

                    assert nowDone <= oldLen;
                }
            }

            // Check for the copy being all done, and promote. We might have nested in-progress copies and finish
            // a nested copy before the top-level one: only top-level copies are promoted.
            if (nowDone == oldLen && map.tbl == this && TBL_UPD.compareAndSet(map, this, next))
                map.lastResizeMillis = System.currentTimeMillis();
        }

        /**
         * Copies one K/V pair from this table to the new one. Returns only after updating the new table. We need
         * an accurate confirmed-copy count to know when we can promote: if the new table is promoted too soon,
         * other threads may miss values not yet copied from the old table.
         *
         * @param idx Slot index.
         * @return {@code True} if this thread set the slot to {@link #TOMBPRIME}, i.e. confirmed the copy.
         */
        private boolean copySlot(int idx) {
            // Blindly set a free key slot to some key which hashes here, to eagerly stop fresh puts from
            // inserting new values in the old table mid-resize. Correctness stems from boxing the value,
            // slamming the key is a minor speed optimization.
            long key;

            while ((key = keys.get(idx)) == NO_KEY)
                casKey(idx, NO_KEY, idx + keys.length());

            // Box what we see in the old table, to prevent further updates.
            Object oldVal = vals.get(idx);

            while (!(oldVal instanceof Prime)) {
                Prime box = (oldVal == null || oldVal == TOMBSTONE) ? TOMBPRIME : new Prime(oldVal);

                if (casVal(idx, oldVal, box)) {
                    // A TOMBPRIME both prevents further updates here and makes the (absent) old value
                    // vacuously available in the new table: any thread can go straight to the new table.
                    if (box == TOMBPRIME)
                        return true;

                    // Otherwise we boxed something, but it still needs to be copied into the new table.
                    oldVal = box;

                    break;
                }

                oldVal = vals.get(idx); // Else try, try again.
            }

            if (oldVal == TOMBPRIME)
                return false; // Copy already complete here.

            // Copy the value into the new table, but only if we overwrite a null. If another value is already in
            // the new table, then somebody else wrote it there and that write happens-after any value that
            // appears in the old table.
            Object unboxed = ((Prime)oldVal).val;

            assert unboxed != TOMBSTONE;

            boolean copied = next.putIfMatch(key, unboxed, null) == null;

            // Now that any old value is exposed in the new table, forever hide the old-table value by slapping
            // a TOMBPRIME down, so other threads do not uselessly attempt to copy this slot.
            while (oldVal != TOMBPRIME && !casVal(idx, oldVal, TOMBPRIME))
                oldVal = vals.get(idx);

            return copied;
        }
    }

    /**
     * Iterator over the values. It "snapshots" a clean view of the key array: every key in the snapshot table is
     * visited, keys added later might be skipped.
     */
    private class ValIterator implements Iterator<V>, Enumeration<V> {
        /** Snapshot table. */
        private final Table snap;

        /** Position: -1 before checking {@link #NO_KEY}, then an index into the key array. */
        private int idx;

        /** Next key found. */
        private long nextKey;

        /** Last key returned. */
        private long prevKey;

        /** Next value found, {@code null} when there is none. */
        private V nextVal;

        /** Last value returned. */
        private V prevVal;

        /** */
        ValIterator() {
            Table top;

            while (true) {
                top = tbl;

                if (top.next == null) // No table copy in progress?
                    break;

                // A copy is in progress, so we can not get a clean iteration: help to finish it first.
                top.helpCopy(true);
            }

            snap = top; // The linearization point of the iteration.

            idx = -1;

            next(); // Warm up: find the first pair.
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return nextVal != null;
        }

        /** {@inheritDoc} */
        @Override public V next() {
            // 'next' already knows what the next value is, lest 'hasNext' reports true and some other thread
            // deletes the last value. Here it finds the pair that comes after.
            if (idx != -1 && nextVal == null)
                throw new NoSuchElementException();

            prevKey = nextKey;
            prevVal = nextVal;
            nextVal = null;

            if (idx == -1) { // Check NO_KEY first.
                idx = 0;
                nextKey = NO_KEY;

                if ((nextVal = get(nextKey)) != null)
                    return prevVal;
            }

            while (idx < snap.keys.length()) { // Scan the key array.
                nextKey = snap.keys.get(idx++); // A key that definitely is in the set, for the moment.

                if (nextKey != NO_KEY && (nextVal = get(nextKey)) != null)
                    break;
            }

            return prevVal;
        }

        /** Removes the last key returned. */
        void removeKey() {
            if (prevVal == null)
                throw new IllegalStateException();

            NonBlockingHashMapLong.this.putIfMatch(prevKey, TOMBSTONE, NO_MATCH_OLD);

            prevVal = null;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            // The JDK always removes by key, even when the value has changed.
            removeKey();
        }

        /** {@inheritDoc} */
        @Override public V nextElement() {
            return next();
        }

        /** {@inheritDoc} */
        @Override public boolean hasMoreElements() {
            return hasNext();
        }
    }

    /** Iterator over the keys supporting a <strong>non-boxing</strong> {@link #nextLong()}. */
    public class LongKeyIterator implements Iterator<Long>, Enumeration<Long> {
        /** Underlying value iterator. */
        private final ValIterator iter = new ValIterator();

        /** Removes the last key returned by {@link #next()} or {@link #nextLong()}. */
        @Override public void remove() {
            iter.removeKey();
        }

        /** {@inheritDoc} */
        @Override public Long next() {
            return nextLong();
        }

        /**
         * @return The next key as a primitive {@code long}.
         */
        public long nextLong() {
            iter.next();

            return iter.prevKey;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public Long nextElement() {
            return next();
        }

        /** {@inheritDoc} */
        @Override public boolean hasMoreElements() {
            return hasNext();
        }
    }

    /** Entry of the {@link #entrySet()} view. */
    private class EntryImpl implements Map.Entry<Long, V> {
        /** Key. */
        private final long key;

        /** Value. */
        private V val;

        /**
         * @param key Key.
         * @param val Value.
         */
        EntryImpl(long key, V val) {
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Long getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public V setValue(V val) {
            if (val == null)
                throw new NullPointerException();

            V old = this.val;

            this.val = val;

            put(key, val);

            return old;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (!(o instanceof Map.Entry))
                return false;

            Map.Entry<?, ?> e = (Map.Entry<?, ?>)o;

            return Objects.equals(key, e.getKey()) && Objects.equals(val, e.getValue());
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Long.hashCode(key) ^ Objects.hashCode(val);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return key + "=" + val;
        }
    }

    /** Iterator over the entries. Each step creates a new {@link EntryImpl} and boxes the key. */
    private class EntryIterator implements Iterator<Map.Entry<Long, V>> {
        /** Underlying value iterator. */
        private final ValIterator iter = new ValIterator();

        /** {@inheritDoc} */
        @Override public void remove() {
            // The JDK always removes by key, even when the value has changed.
            iter.removeKey();
        }

        /** {@inheritDoc} */
        @Override public Map.Entry<Long, V> next() {
            iter.next();

            return new EntryImpl(iter.prevKey, iter.prevVal);
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return iter.hasNext();
        }
    }
}
