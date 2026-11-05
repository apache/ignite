/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
 * Apache Ignite: a cut-down derivative of java.util.concurrent.ConcurrentHashMap specialised to primitive long keys.
 * The text was taken from OpenJDK 17.0.16 (src.zip), which is identical to the jsr166 CVS version of the class.
 * It differs from the original as follows:
 *
 *  - keys are primitive long values hashed by spread((int)(k ^ (k >>> 32))), compared with == (no equals());
 *  - no Map interface: the API is the primitive-key subset of ConcurrentMap plus values(), keyIterator()
 *    and forEach(); no bulk parallel operations, no spliterators, no serialization, no equals/hashCode/toString;
 *  - no tree bins: a bin reaching TREEIFY_THRESHOLD nodes grows the table while the table is smaller than
 *    MIN_TREEIFY_CAPACITY and is left as a linked list otherwise (the JDK converts it to a red-black tree);
 *  - counter cells are padded by hand instead of @Contended and the per-thread probe is derived from
 *    Thread.getId() instead of ThreadLocalRandom's probe, neither of which is accessible outside java.base;
 *  - memory access goes through GridUnsafe instead of jdk.internal.misc.Unsafe (tabAt/setTabAt use volatile
 *    access where the JDK uses acquire/release).
 */

package org.jsr166;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * A hash table supporting full concurrency of retrievals and
 * high expected concurrency for updates, keyed by primitive {@code long}
 * values. This class obeys the same functional specification as
 * {@link java.util.concurrent.ConcurrentHashMap} (see the differences
 * listed in the file header), and includes versions of methods
 * corresponding to each method of {@code ConcurrentMap} that is useful
 * with primitive keys. Retrieval operations (including {@code get})
 * generally do not block, so may overlap with update operations
 * (including {@code put} and {@code remove}). Retrievals reflect the
 * results of the most recently <em>completed</em> update operations
 * holding upon their onset. Iterators are weakly consistent: they
 * traverse elements as they existed upon construction exactly once,
 * and may (but are not guaranteed to) reflect any modifications
 * subsequent to construction.
 *
 * <p>Like {@code ConcurrentHashMap}, this class does <em>not</em> allow
 * {@code null} to be used as a value.
 *
 * @param <V> the type of mapped values
 */
public class ConcurrentLongHashMap<V> {

    /*
     * Overview: see the description in java.util.concurrent.ConcurrentHashMap,
     * which applies unchanged except for the parts concerning tree bins.
     */

    /* ---------------- Constants -------------- */

    /**
     * The largest possible table capacity.  This value must be
     * exactly 1<<30 to stay within Java array allocation and indexing
     * bounds for power of two table sizes, and is further required
     * because the top two bits of 32bit hash fields are used for
     * control purposes.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The default initial table capacity.  Must be a power of 2
     * (i.e., at least 1) and at most MAXIMUM_CAPACITY.
     */
    private static final int DEFAULT_CAPACITY = 16;

    /**
     * The load factor for this table. Overrides of this value in
     * constructors affect only the initial table capacity.  The
     * actual floating point value isn't normally used -- it is
     * simpler to use expressions such as {@code n - (n >>> 2)} for
     * the associated resizing threshold.
     */
    private static final float LOAD_FACTOR = 0.75f;

    /**
     * The bin count threshold at which a long bin triggers a table
     * resize (when the table is smaller than MIN_TREEIFY_CAPACITY).
     * In the JDK this is the threshold for converting a list to a tree.
     */
    static final int TREEIFY_THRESHOLD = 8;

    /**
     * The smallest table capacity at which an overlong bin no longer
     * triggers a resize. (In the JDK, bins may be treeified from this
     * capacity on; here they are left as lists.)
     */
    static final int MIN_TREEIFY_CAPACITY = 64;

    /**
     * Minimum number of rebinnings per transfer step. Ranges are
     * subdivided to allow multiple resizer threads.  This value
     * serves as a lower bound to avoid resizers encountering
     * excessive memory contention.  The value should be at least
     * DEFAULT_CAPACITY.
     */
    private static final int MIN_TRANSFER_STRIDE = 16;

    /**
     * The number of bits used for generation stamp in sizeCtl.
     * Must be at least 6 for 32bit arrays.
     */
    private static final int RESIZE_STAMP_BITS = 16;

    /**
     * The maximum number of threads that can help resize.
     * Must fit in 32 - RESIZE_STAMP_BITS bits.
     */
    private static final int MAX_RESIZERS = (1 << (32 - RESIZE_STAMP_BITS)) - 1;

    /**
     * The bit shift for recording size stamp in sizeCtl.
     */
    private static final int RESIZE_STAMP_SHIFT = 32 - RESIZE_STAMP_BITS;

    /*
     * Encodings for Node hash fields. See above for explanation.
     */
    static final int MOVED     = -1; // hash for forwarding nodes
    static final int RESERVED  = -3; // hash for transient reservations
    static final int HASH_BITS = 0x7fffffff; // usable bits of normal node hash

    /** Number of CPUS, to place bounds on some sizings */
    static final int NCPU = Runtime.getRuntime().availableProcessors();

    /* ---------------- Functional interfaces -------------- */

    /**
     * An action on a key-value mapping that avoids boxing the key.
     *
     * @param <V> the type of mapped values
     */
    @FunctionalInterface
    public interface EntryConsumer<V> {
        /**
         * @param key the key
         * @param val the value
         */
        void accept(long key, V val);
    }

    /**
     * A remapping function that avoids boxing the key.
     *
     * @param <V> the type of mapped values
     */
    @FunctionalInterface
    public interface RemappingFunction<V> {
        /**
         * @param key the key
         * @param val the current value, or {@code null} if there is none
         * @return the new value, or {@code null} to remove the mapping
         */
        V apply(long key, V val);
    }

    /* ---------------- Nodes -------------- */

    /**
     * Key-value entry. Subclasses of Node with a negative hash field
     * are special, and contain null values (but are never
     * exported).  Otherwise, vals are never null.
     */
    static class Node<V> {
        final int hash;
        final long key;
        volatile V val;
        volatile Node<V> next;

        Node(int hash, long key, V val) {
            this.hash = hash;
            this.key = key;
            this.val = val;
        }

        Node(int hash, long key, V val, Node<V> next) {
            this(hash, key, val);
            this.next = next;
        }

        /**
         * Virtualized support for map.get(); overridden in subclasses.
         */
        Node<V> find(int h, long k) {
            Node<V> e = this;
            do {
                if (e.hash == h && e.key == k)
                    return e;
            } while ((e = e.next) != null);
            return null;
        }
    }

    /* ---------------- Static utilities -------------- */

    /**
     * Folds the key to an int the way Long.hashCode does, then spreads
     * (XORs) higher bits of the hash to lower and also forces top
     * bit to 0. Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will
     * always collide, so we apply a transform that spreads the impact
     * of higher bits downward.
     */
    static final int spread(long k) {
        int h = (int)(k ^ (k >>> 32));
        return (h ^ (h >>> 16)) & HASH_BITS;
    }

    /**
     * Returns a power of two table size for the given desired capacity.
     * See Hackers Delight, sec 3.2
     */
    private static final int tableSizeFor(int c) {
        int n = -1 >>> Integer.numberOfLeadingZeros(c - 1);
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    /* ---------------- Table element access -------------- */

    /*
     * Atomic access methods are used for table elements as well as
     * elements of in-progress next table while resizing.  All uses of
     * the tab arguments must be null checked by callers.  All callers
     * also paranoically precheck that tab's length is not zero (or an
     * equivalent check), thus ensuring that any index argument taking
     * the form of a hash value anded with (length - 1) is a valid
     * index.  Note that, to be correct wrt arbitrary concurrency
     * errors by users, these checks must operate on local variables,
     * which accounts for some odd-looking inline assignments below.
     * Note that calls to setTabAt always occur within locked regions,
     * and so require only release ordering.
     */

    @SuppressWarnings("unchecked")
    static final <V> Node<V> tabAt(Node<V>[] tab, int i) {
        return (Node<V>)GridUnsafe.getObjectVolatile(tab, ((long)i << ASHIFT) + ABASE);
    }

    static final <V> boolean casTabAt(Node<V>[] tab, int i,
                                      Node<V> c, Node<V> v) {
        return GridUnsafe.compareAndSwapObject(tab, ((long)i << ASHIFT) + ABASE, c, v);
    }

    static final <V> void setTabAt(Node<V>[] tab, int i, Node<V> v) {
        GridUnsafe.putObjectVolatile(tab, ((long)i << ASHIFT) + ABASE, v);
    }

    /* ---------------- Fields -------------- */

    /**
     * The array of bins. Lazily initialized upon first insertion.
     * Size is always a power of two. Accessed directly by iterators.
     */
    transient volatile Node<V>[] table;

    /**
     * The next table to use; non-null only while resizing.
     */
    private transient volatile Node<V>[] nextTable;

    /**
     * Base counter value, used mainly when there is no contention,
     * but also as a fallback during table initialization
     * races. Updated via CAS.
     */
    private transient volatile long baseCount;

    /**
     * Table initialization and resizing control.  When negative, the
     * table is being initialized or resized: -1 for initialization,
     * else -(1 + the number of active resizing threads).  Otherwise,
     * when table is null, holds the initial table size to use upon
     * creation, or 0 for default. After initialization, holds the
     * next element count value upon which to resize the table.
     */
    private transient volatile int sizeCtl;

    /**
     * The next table index (plus one) to split while resizing.
     */
    private transient volatile int transferIndex;

    /**
     * Spinlock (locked via CAS) used when resizing and/or creating CounterCells.
     */
    private transient volatile int cellsBusy;

    /**
     * Table of counter cells. When non-null, size is a power of 2.
     */
    private transient volatile CounterCell[] counterCells;

    // views
    private transient ValuesView<V> values;


    /* ---------------- Public operations -------------- */

    /**
     * Creates a new, empty map with the default initial table size (16).
     */
    public ConcurrentLongHashMap() {
    }

    /**
     * Creates a new, empty map with an initial table size
     * accommodating the specified number of elements without the need
     * to dynamically resize.
     *
     * @param initialCapacity The implementation performs internal
     * sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     * elements is negative
     */
    public ConcurrentLongHashMap(int initialCapacity) {
        this(initialCapacity, LOAD_FACTOR, 1);
    }

    /**
     * Creates a new, empty map with an initial table size based on
     * the given number of elements ({@code initialCapacity}), initial
     * table density ({@code loadFactor}), and number of concurrently
     * updating threads ({@code concurrencyLevel}).
     *
     * @param initialCapacity the initial capacity. The implementation
     * performs internal sizing to accommodate this many elements,
     * given the specified load factor.
     * @param loadFactor the load factor (table density) for
     * establishing the initial table size
     * @param concurrencyLevel the estimated number of concurrently
     * updating threads. The implementation may use this value as
     * a sizing hint.
     * @throws IllegalArgumentException if the initial capacity is
     * negative or the load factor or concurrencyLevel are
     * nonpositive
     */
    public ConcurrentLongHashMap(int initialCapacity,
                                 float loadFactor, int concurrencyLevel) {
        if (!(loadFactor > 0.0f) || initialCapacity < 0 || concurrencyLevel <= 0)
            throw new IllegalArgumentException();
        if (initialCapacity < concurrencyLevel)   // Use at least as many bins
            initialCapacity = concurrencyLevel;   // as estimated threads
        long size = (long)(1.0 + (long)initialCapacity / loadFactor);
        int cap = (size >= (long)MAXIMUM_CAPACITY) ?
            MAXIMUM_CAPACITY : tableSizeFor((int)size);
        this.sizeCtl = cap;
    }

    /**
     * {@inheritDoc Map#size()}
     */
    public int size() {
        long n = sumCount();
        return ((n < 0L) ? 0 :
                (n > (long)Integer.MAX_VALUE) ? Integer.MAX_VALUE :
                (int)n);
    }

    /**
     * Returns the number of mappings. This method should be used
     * instead of {@link #size} because a map may contain more mappings
     * than can be represented as an int. The value returned is an
     * estimate; the actual count may differ if there are concurrent
     * insertions or removals.
     *
     * @return the number of mappings
     */
    public long mappingCount() {
        long n = sumCount();
        return (n < 0L) ? 0L : n; // ignore transient negative values
    }

    /**
     * {@inheritDoc Map#isEmpty()}
     */
    public boolean isEmpty() {
        return sumCount() <= 0L; // ignore transient negative values
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code key == k},
     * then this method returns {@code v}; otherwise it returns
     * {@code null}.  (There can be at most one such mapping.)
     */
    public V get(long key) {
        Node<V>[] tab; Node<V> e, p; int n, eh;
        int h = spread(key);
        if ((tab = table) != null && (n = tab.length) > 0 &&
            (e = tabAt(tab, (n - 1) & h)) != null) {
            if ((eh = e.hash) == h) {
                if (e.key == key)
                    return e.val;
            }
            else if (eh < 0)
                return (p = e.find(h, key)) != null ? p.val : null;
            while ((e = e.next) != null) {
                if (e.hash == h && e.key == key)
                    return e.val;
            }
        }
        return null;
    }

    /**
     * Tests if the specified long is a key in this table.
     *
     * @param  key possible key
     * @return {@code true} if and only if the specified long
     *         is a key in this table, as determined by the
     *         {@code ==} operator; {@code false} otherwise
     */
    public boolean containsKey(long key) {
        return get(key) != null;
    }

    /**
     * Returns {@code true} if this map maps one or more keys to the
     * specified value. Note: This method may require a full traversal
     * of the map, and is much slower than method {@code containsKey}.
     *
     * @param value value whose presence in this map is to be tested
     * @return {@code true} if this map maps one or more keys to the
     *         specified value
     * @throws NullPointerException if the specified value is null
     */
    public boolean containsValue(Object value) {
        if (value == null)
            throw new NullPointerException();
        Node<V>[] t;
        if ((t = table) != null) {
            Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
            for (Node<V> p; (p = it.advance()) != null; ) {
                V v;
                if ((v = p.val) == value || (v != null && value.equals(v)))
                    return true;
            }
        }
        return false;
    }

    /**
     * Maps the specified key to the specified value in this table.
     * The value can be retrieved by calling the {@code get} method
     * with a key that is equal to the original key.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with {@code key}, or
     *         {@code null} if there was no mapping for {@code key}
     * @throws NullPointerException if the specified value is null
     */
    public V put(long key, V value) {
        return putVal(key, value, false);
    }

    /** Implementation for put and putIfAbsent */
    final V putVal(long key, V value, boolean onlyIfAbsent) {
        if (value == null) throw new NullPointerException();
        int hash = spread(key);
        int binCount = 0;
        for (Node<V>[] tab = table;;) {
            Node<V> f; int n, i, fh; V fv;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & hash)) == null) {
                if (casTabAt(tab, i, null, new Node<V>(hash, key, value)))
                    break;                   // no lock when adding to empty bin
            }
            else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else if (onlyIfAbsent // check first node without acquiring lock
                     && fh == hash
                     && f.key == key
                     && (fv = f.val) != null)
                return fv;
            else {
                V oldVal = null;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f;; ++binCount) {
                                if (e.hash == hash && e.key == key) {
                                    oldVal = e.val;
                                    if (!onlyIfAbsent)
                                        e.val = value;
                                    break;
                                }
                                Node<V> pred = e;
                                if ((e = e.next) == null) {
                                    pred.next = new Node<V>(hash, key, value);
                                    break;
                                }
                            }
                        }
                        else if (f instanceof ReservationNode)
                            throw new IllegalStateException("Recursive update");
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD)
                        treeifyBin(tab, i);
                    if (oldVal != null)
                        return oldVal;
                    break;
                }
            }
        }
        addCount(1L, binCount);
        return null;
    }

    /**
     * Removes the key (and its corresponding value) from this map.
     * This method does nothing if the key is not in the map.
     *
     * @param  key the key that needs to be removed
     * @return the previous value associated with {@code key}, or
     *         {@code null} if there was no mapping for {@code key}
     */
    public V remove(long key) {
        return replaceNode(key, null, null);
    }

    /**
     * Implementation for the four public remove/replace methods:
     * Replaces node value with v, conditional upon match of cv if
     * non-null.  If resulting value is null, delete.
     */
    final V replaceNode(long key, V value, Object cv) {
        int hash = spread(key);
        for (Node<V>[] tab = table;;) {
            Node<V> f; int n, i, fh;
            if (tab == null || (n = tab.length) == 0 ||
                (f = tabAt(tab, i = (n - 1) & hash)) == null)
                break;
            else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                V oldVal = null;
                boolean validated = false;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            validated = true;
                            for (Node<V> e = f, pred = null;;) {
                                if (e.hash == hash && e.key == key) {
                                    V ev = e.val;
                                    if (cv == null || cv == ev ||
                                        (ev != null && cv.equals(ev))) {
                                        oldVal = ev;
                                        if (value != null)
                                            e.val = value;
                                        else if (pred != null)
                                            pred.next = e.next;
                                        else
                                            setTabAt(tab, i, e.next);
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null)
                                    break;
                            }
                        }
                        else if (f instanceof ReservationNode)
                            throw new IllegalStateException("Recursive update");
                    }
                }
                if (validated) {
                    if (oldVal != null) {
                        if (value == null)
                            addCount(-1L, -1);
                        return oldVal;
                    }
                    break;
                }
            }
        }
        return null;
    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {
        long delta = 0L; // negative number of deletions
        int i = 0;
        Node<V>[] tab = table;
        while (tab != null && i < tab.length) {
            int fh;
            Node<V> f = tabAt(tab, i);
            if (f == null)
                ++i;
            else if ((fh = f.hash) == MOVED) {
                tab = helpTransfer(tab, f);
                i = 0; // restart
            }
            else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        Node<V> p = (fh >= 0 ? f : null);
                        while (p != null) {
                            --delta;
                            p = p.next;
                        }
                        setTabAt(tab, i++, null);
                    }
                }
            }
        }
        if (delta != 0L)
            addCount(delta, -1);
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are
     * reflected in the collection, and vice-versa.  The collection
     * supports element removal, which removes the corresponding
     * mapping from this map, via the {@code Iterator.remove},
     * {@code Collection.remove}, {@code removeAll},
     * {@code retainAll}, and {@code clear} operations.  It does not
     * support the {@code add} or {@code addAll} operations.
     *
     * <p>The view's iterators are weakly consistent.
     *
     * @return the collection view
     */
    public Collection<V> values() {
        ValuesView<V> vs;
        if ((vs = values) != null) return vs;
        return values = new ValuesView<V>(this);
    }

    /**
     * Returns an iterator over the keys in this map that does not box
     * them. The iterator is weakly consistent and supports removal.
     *
     * @return a primitive iterator over the keys
     */
    public PrimitiveIterator.OfLong keyIterator() {
        Node<V>[] t;
        int f = (t = table) == null ? 0 : t.length;
        return new KeyIterator<V>(t, f, 0, f, this);
    }

    // ConcurrentMap methods

    /**
     * Maps the specified key to the specified value if the key is
     * not already associated with a value.
     *
     * @return the previous value associated with the specified key,
     *         or {@code null} if there was no mapping for the key
     * @throws NullPointerException if the specified value is null
     */
    public V putIfAbsent(long key, V value) {
        return putVal(key, value, true);
    }

    /**
     * Removes the entry for a key only if currently mapped to a given value.
     *
     * @throws NullPointerException if the specified value is null
     */
    public boolean remove(long key, Object value) {
        if (value == null)
            throw new NullPointerException();
        return replaceNode(key, null, value) != null;
    }

    /**
     * Replaces the entry for a key only if currently mapped to a given value.
     *
     * @throws NullPointerException if any of the values is null
     */
    public boolean replace(long key, V oldValue, V newValue) {
        if (oldValue == null || newValue == null)
            throw new NullPointerException();
        return replaceNode(key, newValue, oldValue) != null;
    }

    /**
     * Replaces the entry for a key only if currently mapped to some value.
     *
     * @return the previous value associated with the specified key,
     *         or {@code null} if there was no mapping for the key
     * @throws NullPointerException if the specified value is null
     */
    public V replace(long key, V value) {
        if (value == null)
            throw new NullPointerException();
        return replaceNode(key, value, null);
    }

    // Overrides of JDK8+ Map extension method defaults

    /**
     * Returns the value to which the specified key is mapped, or the
     * given default value if this map contains no mapping for the key.
     *
     * @param key the key whose associated value is to be returned
     * @param defaultValue the value to return if this map contains
     * no mapping for the given key
     * @return the mapping for the key, if present; else the default value
     */
    public V getOrDefault(long key, V defaultValue) {
        V v;
        return (v = get(key)) == null ? defaultValue : v;
    }

    /**
     * Performs the given action for each mapping.
     *
     * @param action the action
     * @throws NullPointerException if the action is null
     */
    public void forEach(EntryConsumer<? super V> action) {
        if (action == null) throw new NullPointerException();
        Node<V>[] t;
        if ((t = table) != null) {
            Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
            for (Node<V> p; (p = it.advance()) != null; ) {
                action.accept(p.key, p.val);
            }
        }
    }

    /**
     * If the specified key is not already associated with a value,
     * attempts to compute its value using the given mapping function
     * and enters it into this map unless {@code null}.  The entire
     * method invocation is performed atomically.  The supplied
     * function is invoked exactly once per invocation of this method
     * if the key is absent, else not at all.  Some attempted update
     * operations on this map by other threads may be blocked while
     * computation is in progress, so the computation should be short
     * and simple.
     *
     * <p>The mapping function must not modify this map during computation.
     *
     * @param key key with which the specified value is to be associated
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value associated with
     *         the specified key, or null if the computed value is null
     * @throws NullPointerException if the specified mappingFunction
     *         is null
     * @throws IllegalStateException if the computation detectably
     *         attempts a recursive update to this map that would
     *         otherwise never complete
     * @throws RuntimeException or Error if the mappingFunction does so,
     *         in which case the mapping is left unestablished
     */
    public V computeIfAbsent(long key, LongFunction<? extends V> mappingFunction) {
        if (mappingFunction == null)
            throw new NullPointerException();
        int h = spread(key);
        V val = null;
        int binCount = 0;
        for (Node<V>[] tab = table;;) {
            Node<V> f; int n, i, fh; V fv;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                Node<V> r = new ReservationNode<V>();
                synchronized (r) {
                    if (casTabAt(tab, i, null, r)) {
                        binCount = 1;
                        Node<V> node = null;
                        try {
                            if ((val = mappingFunction.apply(key)) != null)
                                node = new Node<V>(h, key, val);
                        } finally {
                            setTabAt(tab, i, node);
                        }
                    }
                }
                if (binCount != 0)
                    break;
            }
            else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else if (fh == h    // check first node without acquiring lock
                     && f.key == key
                     && (fv = f.val) != null)
                return fv;
            else {
                boolean added = false;
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f;; ++binCount) {
                                if (e.hash == h && e.key == key) {
                                    val = e.val;
                                    break;
                                }
                                Node<V> pred = e;
                                if ((e = e.next) == null) {
                                    if ((val = mappingFunction.apply(key)) != null) {
                                        if (pred.next != null)
                                            throw new IllegalStateException("Recursive update");
                                        added = true;
                                        pred.next = new Node<V>(h, key, val);
                                    }
                                    break;
                                }
                            }
                        }
                        else if (f instanceof ReservationNode)
                            throw new IllegalStateException("Recursive update");
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD)
                        treeifyBin(tab, i);
                    if (!added)
                        return val;
                    break;
                }
            }
        }
        if (val != null)
            addCount(1L, binCount);
        return val;
    }

    /**
     * Attempts to compute a mapping for the specified key and its
     * current mapped value (or {@code null} if there is no current
     * mapping). The entire method invocation is performed atomically.
     * The supplied function is invoked exactly once per invocation of
     * this method.  Some attempted update operations on this map by
     * other threads may be blocked while computation is in progress,
     * so the computation should be short and simple.
     *
     * <p>The remapping function must not modify this map during computation.
     *
     * @param key key with which the specified value is to be associated
     * @param remappingFunction the function to compute a value
     * @return the new value associated with the specified key, or null if none
     * @throws NullPointerException if the specified remappingFunction
     *         is null
     * @throws IllegalStateException if the computation detectably
     *         attempts a recursive update to this map that would
     *         otherwise never complete
     * @throws RuntimeException or Error if the remappingFunction does so,
     *         in which case the mapping is unchanged
     */
    public V compute(long key, RemappingFunction<V> remappingFunction) {
        if (remappingFunction == null)
            throw new NullPointerException();
        int h = spread(key);
        V val = null;
        int delta = 0;
        int binCount = 0;
        for (Node<V>[] tab = table;;) {
            Node<V> f; int n, i, fh;
            if (tab == null || (n = tab.length) == 0)
                tab = initTable();
            else if ((f = tabAt(tab, i = (n - 1) & h)) == null) {
                Node<V> r = new ReservationNode<V>();
                synchronized (r) {
                    if (casTabAt(tab, i, null, r)) {
                        binCount = 1;
                        Node<V> node = null;
                        try {
                            if ((val = remappingFunction.apply(key, null)) != null) {
                                delta = 1;
                                node = new Node<V>(h, key, val);
                            }
                        } finally {
                            setTabAt(tab, i, node);
                        }
                    }
                }
                if (binCount != 0)
                    break;
            }
            else if ((fh = f.hash) == MOVED)
                tab = helpTransfer(tab, f);
            else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        if (fh >= 0) {
                            binCount = 1;
                            for (Node<V> e = f, pred = null;; ++binCount) {
                                if (e.hash == h && e.key == key) {
                                    val = remappingFunction.apply(key, e.val);
                                    if (val != null)
                                        e.val = val;
                                    else {
                                        delta = -1;
                                        Node<V> en = e.next;
                                        if (pred != null)
                                            pred.next = en;
                                        else
                                            setTabAt(tab, i, en);
                                    }
                                    break;
                                }
                                pred = e;
                                if ((e = e.next) == null) {
                                    val = remappingFunction.apply(key, null);
                                    if (val != null) {
                                        if (pred.next != null)
                                            throw new IllegalStateException("Recursive update");
                                        delta = 1;
                                        pred.next = new Node<V>(h, key, val);
                                    }
                                    break;
                                }
                            }
                        }
                        else if (f instanceof ReservationNode)
                            throw new IllegalStateException("Recursive update");
                    }
                }
                if (binCount != 0) {
                    if (binCount >= TREEIFY_THRESHOLD)
                        treeifyBin(tab, i);
                    break;
                }
            }
        }
        if (delta != 0)
            addCount((long)delta, binCount);
        return val;
    }

    /* ---------------- Special Nodes -------------- */

    /**
     * A node inserted at head of bins during transfer operations.
     */
    static final class ForwardingNode<V> extends Node<V> {
        final Node<V>[] nextTable;
        ForwardingNode(Node<V>[] tab) {
            super(MOVED, 0L, null);
            this.nextTable = tab;
        }

        Node<V> find(int h, long k) {
            // loop to avoid arbitrarily deep recursion on forwarding nodes
            outer: for (Node<V>[] tab = nextTable;;) {
                Node<V> e; int n;
                if (tab == null || (n = tab.length) == 0 ||
                    (e = tabAt(tab, (n - 1) & h)) == null)
                    return null;
                for (;;) {
                    int eh;
                    if ((eh = e.hash) == h && e.key == k)
                        return e;
                    if (eh < 0) {
                        if (e instanceof ForwardingNode) {
                            tab = ((ForwardingNode<V>)e).nextTable;
                            continue outer;
                        }
                        else
                            return e.find(h, k);
                    }
                    if ((e = e.next) == null)
                        return null;
                }
            }
        }
    }

    /**
     * A place-holder node used in computeIfAbsent and compute.
     */
    static final class ReservationNode<V> extends Node<V> {
        ReservationNode() {
            super(RESERVED, 0L, null);
        }

        Node<V> find(int h, long k) {
            return null;
        }
    }

    /* ---------------- Table Initialization and Resizing -------------- */

    /**
     * Returns the stamp bits for resizing a table of size n.
     * Must be negative when shifted left by RESIZE_STAMP_SHIFT.
     */
    static final int resizeStamp(int n) {
        return Integer.numberOfLeadingZeros(n) | (1 << (RESIZE_STAMP_BITS - 1));
    }

    /**
     * Initializes table, using the size recorded in sizeCtl.
     */
    private final Node<V>[] initTable() {
        Node<V>[] tab; int sc;
        while ((tab = table) == null || tab.length == 0) {
            if ((sc = sizeCtl) < 0)
                Thread.yield(); // lost initialization race; just spin
            else if (GridUnsafe.compareAndSwapInt(this, SIZECTL, sc, -1)) {
                try {
                    if ((tab = table) == null || tab.length == 0) {
                        int n = (sc > 0) ? sc : DEFAULT_CAPACITY;
                        @SuppressWarnings("unchecked")
                        Node<V>[] nt = (Node<V>[])new Node<?>[n];
                        table = tab = nt;
                        sc = n - (n >>> 2);
                    }
                } finally {
                    sizeCtl = sc;
                }
                break;
            }
        }
        return tab;
    }

    /**
     * Adds to count, and if table is too small and not already
     * resizing, initiates transfer. If already resizing, helps
     * perform transfer if work is available.  Rechecks occupancy
     * after a transfer to see if another resize is already needed
     * because resizings are lagging additions.
     *
     * @param x the count to add
     * @param check if <0, don't check resize, if <= 1 only check if uncontended
     */
    private final void addCount(long x, int check) {
        CounterCell[] cs; long b, s;
        if ((cs = counterCells) != null ||
            !GridUnsafe.compareAndSwapLong(this, BASECOUNT, b = baseCount, s = b + x)) {
            CounterCell c; long v; int m;
            boolean uncontended = true;
            if (cs == null || (m = cs.length - 1) < 0 ||
                (c = cs[probe() & m]) == null ||
                !(uncontended =
                  GridUnsafe.compareAndSwapLong(c, CELLVALUE, v = c.value, v + x))) {
                fullAddCount(x, uncontended);
                return;
            }
            if (check <= 1)
                return;
            s = sumCount();
        }
        if (check >= 0) {
            Node<V>[] tab, nt; int n, sc;
            while (s >= (long)(sc = sizeCtl) && (tab = table) != null &&
                   (n = tab.length) < MAXIMUM_CAPACITY) {
                int rs = resizeStamp(n) << RESIZE_STAMP_SHIFT;
                if (sc < 0) {
                    if (sc == rs + MAX_RESIZERS || sc == rs + 1 ||
                        (nt = nextTable) == null || transferIndex <= 0)
                        break;
                    if (GridUnsafe.compareAndSwapInt(this, SIZECTL, sc, sc + 1))
                        transfer(tab, nt);
                }
                else if (GridUnsafe.compareAndSwapInt(this, SIZECTL, sc, rs + 2))
                    transfer(tab, null);
                s = sumCount();
            }
        }
    }

    /**
     * Helps transfer if a resize is in progress.
     */
    final Node<V>[] helpTransfer(Node<V>[] tab, Node<V> f) {
        Node<V>[] nextTab; int sc;
        if (tab != null && (f instanceof ForwardingNode) &&
            (nextTab = ((ForwardingNode<V>)f).nextTable) != null) {
            int rs = resizeStamp(tab.length) << RESIZE_STAMP_SHIFT;
            while (nextTab == nextTable && table == tab &&
                   (sc = sizeCtl) < 0) {
                if (sc == rs + MAX_RESIZERS || sc == rs + 1 ||
                    transferIndex <= 0)
                    break;
                if (GridUnsafe.compareAndSwapInt(this, SIZECTL, sc, sc + 1)) {
                    transfer(tab, nextTab);
                    break;
                }
            }
            return nextTab;
        }
        return table;
    }

    /**
     * Tries to presize table to accommodate the given number of elements.
     *
     * @param size number of elements (doesn't need to be perfectly accurate)
     */
    private final void tryPresize(int size) {
        int c = (size >= (MAXIMUM_CAPACITY >>> 1)) ? MAXIMUM_CAPACITY :
            tableSizeFor(size + (size >>> 1) + 1);
        int sc;
        while ((sc = sizeCtl) >= 0) {
            Node<V>[] tab = table; int n;
            if (tab == null || (n = tab.length) == 0) {
                n = (sc > c) ? sc : c;
                if (GridUnsafe.compareAndSwapInt(this, SIZECTL, sc, -1)) {
                    try {
                        if (table == tab) {
                            @SuppressWarnings("unchecked")
                            Node<V>[] nt = (Node<V>[])new Node<?>[n];
                            table = nt;
                            sc = n - (n >>> 2);
                        }
                    } finally {
                        sizeCtl = sc;
                    }
                }
            }
            else if (c <= sc || n >= MAXIMUM_CAPACITY)
                break;
            else if (tab == table) {
                int rs = resizeStamp(n);
                if (GridUnsafe.compareAndSwapInt(this, SIZECTL, sc,
                                                 (rs << RESIZE_STAMP_SHIFT) + 2))
                    transfer(tab, null);
            }
        }
    }

    /**
     * Moves and/or copies the nodes in each bin to new table. See
     * above for explanation.
     */
    private final void transfer(Node<V>[] tab, Node<V>[] nextTab) {
        int n = tab.length, stride;
        if ((stride = (NCPU > 1) ? (n >>> 3) / NCPU : n) < MIN_TRANSFER_STRIDE)
            stride = MIN_TRANSFER_STRIDE; // subdivide range
        if (nextTab == null) {            // initiating
            try {
                @SuppressWarnings("unchecked")
                Node<V>[] nt = (Node<V>[])new Node<?>[n << 1];
                nextTab = nt;
            } catch (Throwable ex) {      // try to cope with OOME
                sizeCtl = Integer.MAX_VALUE;
                return;
            }
            nextTable = nextTab;
            transferIndex = n;
        }
        int nextn = nextTab.length;
        ForwardingNode<V> fwd = new ForwardingNode<V>(nextTab);
        boolean advance = true;
        boolean finishing = false; // to ensure sweep before committing nextTab
        for (int i = 0, bound = 0;;) {
            Node<V> f; int fh;
            while (advance) {
                int nextIndex, nextBound;
                if (--i >= bound || finishing)
                    advance = false;
                else if ((nextIndex = transferIndex) <= 0) {
                    i = -1;
                    advance = false;
                }
                else if (GridUnsafe.compareAndSwapInt
                         (this, TRANSFERINDEX, nextIndex,
                          nextBound = (nextIndex > stride ?
                                       nextIndex - stride : 0))) {
                    bound = nextBound;
                    i = nextIndex - 1;
                    advance = false;
                }
            }
            if (i < 0 || i >= n || i + n >= nextn) {
                int sc;
                if (finishing) {
                    nextTable = null;
                    table = nextTab;
                    sizeCtl = (n << 1) - (n >>> 1);
                    return;
                }
                if (GridUnsafe.compareAndSwapInt(this, SIZECTL, sc = sizeCtl, sc - 1)) {
                    if ((sc - 2) != resizeStamp(n) << RESIZE_STAMP_SHIFT)
                        return;
                    finishing = advance = true;
                    i = n; // recheck before commit
                }
            }
            else if ((f = tabAt(tab, i)) == null)
                advance = casTabAt(tab, i, null, fwd);
            else if ((fh = f.hash) == MOVED)
                advance = true; // already processed
            else {
                synchronized (f) {
                    if (tabAt(tab, i) == f) {
                        Node<V> ln, hn;
                        if (fh >= 0) {
                            int runBit = fh & n;
                            Node<V> lastRun = f;
                            for (Node<V> p = f.next; p != null; p = p.next) {
                                int b = p.hash & n;
                                if (b != runBit) {
                                    runBit = b;
                                    lastRun = p;
                                }
                            }
                            if (runBit == 0) {
                                ln = lastRun;
                                hn = null;
                            }
                            else {
                                hn = lastRun;
                                ln = null;
                            }
                            for (Node<V> p = f; p != lastRun; p = p.next) {
                                int ph = p.hash; long pk = p.key; V pv = p.val;
                                if ((ph & n) == 0)
                                    ln = new Node<V>(ph, pk, pv, ln);
                                else
                                    hn = new Node<V>(ph, pk, pv, hn);
                            }
                            setTabAt(nextTab, i, ln);
                            setTabAt(nextTab, i + n, hn);
                            setTabAt(tab, i, fwd);
                            advance = true;
                        }
                        else if (f instanceof ReservationNode)
                            throw new IllegalStateException("Recursive update");
                    }
                }
            }
        }
    }

    /* ---------------- Counter support -------------- */

    /**
     * A padded cell for distributing counts.  Adapted from LongAdder
     * and Striped64.  See their internal docs for explanation.
     * Apache Ignite: padded by hand because @Contended is not available
     * outside java.base.
     */
    static final class CounterCell {
        long p01, p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15;
        volatile long value;
        long q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15;
        CounterCell(long x) { value = x; }
    }

    final long sumCount() {
        CounterCell[] cs = counterCells;
        long sum = baseCount;
        if (cs != null) {
            for (CounterCell c : cs)
                if (c != null)
                    sum += c.value;
        }
        return sum;
    }

    /**
     * Apache Ignite: replacement for ThreadLocalRandom.getProbe(), which is
     * not accessible outside java.base. Derives a well-spread, non-zero
     * per-thread hash from the thread id.
     */
    static final int probe() {
        long id = Thread.currentThread().getId();
        int h = (int)(id ^ (id >>> 32)) * 0x9E3779B9;
        h ^= h >>> 16;
        return h == 0 ? 1 : h;
    }

    /**
     * Apache Ignite: replacement for ThreadLocalRandom.advanceProbe(int).
     * Unlike the JDK, the advanced probe is not stored for the thread.
     */
    static final int advanceProbe(int probe) {
        probe ^= probe << 13;   // xorshift
        probe ^= probe >>> 17;
        probe ^= probe << 5;
        return probe;
    }

    // See LongAdder version for explanation
    private final void fullAddCount(long x, boolean wasUncontended) {
        int h = probe();
        boolean collide = false;                // True if last slot nonempty
        for (;;) {
            CounterCell[] cs; CounterCell c; int n; long v;
            if ((cs = counterCells) != null && (n = cs.length) > 0) {
                if ((c = cs[(n - 1) & h]) == null) {
                    if (cellsBusy == 0) {            // Try to attach new Cell
                        CounterCell r = new CounterCell(x); // Optimistic create
                        if (cellsBusy == 0 &&
                            GridUnsafe.compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                            boolean created = false;
                            try {               // Recheck under lock
                                CounterCell[] rs; int m, j;
                                if ((rs = counterCells) != null &&
                                    (m = rs.length) > 0 &&
                                    rs[j = (m - 1) & h] == null) {
                                    rs[j] = r;
                                    created = true;
                                }
                            } finally {
                                cellsBusy = 0;
                            }
                            if (created)
                                break;
                            continue;           // Slot is now non-empty
                        }
                    }
                    collide = false;
                }
                else if (!wasUncontended)       // CAS already known to fail
                    wasUncontended = true;      // Continue after rehash
                else if (GridUnsafe.compareAndSwapLong(c, CELLVALUE, v = c.value, v + x))
                    break;
                else if (counterCells != cs || n >= NCPU)
                    collide = false;            // At max size or stale
                else if (!collide)
                    collide = true;
                else if (cellsBusy == 0 &&
                         GridUnsafe.compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                    try {
                        if (counterCells == cs) // Expand table unless stale
                            counterCells = Arrays.copyOf(cs, n << 1);
                    } finally {
                        cellsBusy = 0;
                    }
                    collide = false;
                    continue;                   // Retry with expanded table
                }
                h = advanceProbe(h);
            }
            else if (cellsBusy == 0 && counterCells == cs &&
                     GridUnsafe.compareAndSwapInt(this, CELLSBUSY, 0, 1)) {
                boolean init = false;
                try {                           // Initialize table
                    if (counterCells == cs) {
                        CounterCell[] rs = new CounterCell[2];
                        rs[h & 1] = new CounterCell(x);
                        counterCells = rs;
                        init = true;
                    }
                } finally {
                    cellsBusy = 0;
                }
                if (init)
                    break;
            }
            else if (GridUnsafe.compareAndSwapLong(this, BASECOUNT, v = baseCount, v + x))
                break;                          // Fall back on using base
        }
    }

    /* ---------------- Overlong bins -------------- */

    /**
     * Resizes the table if it is too small to spread an overlong bin.
     * Apache Ignite: the JDK converts the bin at the given index to a
     * tree once the table is MIN_TREEIFY_CAPACITY or larger; here it is
     * left as a list, so only the resize branch remains.
     */
    private final void treeifyBin(Node<V>[] tab, int index) {
        int n;
        if (tab != null) {
            if ((n = tab.length) < MIN_TREEIFY_CAPACITY)
                tryPresize(n << 1);
        }
    }

    /* ----------------Table Traversal -------------- */

    /**
     * Records the table, its length, and current traversal index for a
     * traverser that must process a region of a forwarded table before
     * proceeding with current table.
     */
    static final class TableStack<V> {
        int length;
        int index;
        Node<V>[] tab;
        TableStack<V> next;
    }

    /**
     * Encapsulates traversal for methods such as containsValue; also
     * serves as a base class for other iterators.
     *
     * Method advance visits once each still-valid node that was
     * reachable upon iterator construction. It might miss some that
     * were added to a bin after the bin was visited, which is OK wrt
     * consistency guarantees. Maintaining this property in the face
     * of possible ongoing resizes requires a fair amount of
     * bookkeeping state that is difficult to optimize away amidst
     * volatile accesses.  Even so, traversal maintains reasonable
     * throughput.
     *
     * Normally, iteration proceeds bin-by-bin traversing lists.
     * However, if the table has been resized, then all future steps
     * must traverse both the bin at the current index as well as at
     * (index + baseSize); and so on for further resizings. To
     * paranoically cope with potential sharing by users of iterators
     * across threads, iteration terminates if a bounds checks fails
     * for a table read.
     */
    static class Traverser<V> {
        Node<V>[] tab;        // current table; updated if resized
        Node<V> next;         // the next entry to use
        TableStack<V> stack, spare; // to save/restore on ForwardingNodes
        int index;              // index of bin to use next
        int baseIndex;          // current index of initial table
        int baseLimit;          // index bound for initial table
        final int baseSize;     // initial table size

        Traverser(Node<V>[] tab, int size, int index, int limit) {
            this.tab = tab;
            this.baseSize = size;
            this.baseIndex = this.index = index;
            this.baseLimit = limit;
            this.next = null;
        }

        /**
         * Advances if possible, returning next valid node, or null if none.
         */
        final Node<V> advance() {
            Node<V> e;
            if ((e = next) != null)
                e = e.next;
            for (;;) {
                Node<V>[] t; int i, n;  // must use locals in checks
                if (e != null)
                    return next = e;
                if (baseIndex >= baseLimit || (t = tab) == null ||
                    (n = t.length) <= (i = index) || i < 0)
                    return next = null;
                if ((e = tabAt(t, i)) != null && e.hash < 0) {
                    if (e instanceof ForwardingNode) {
                        tab = ((ForwardingNode<V>)e).nextTable;
                        e = null;
                        pushState(t, i, n);
                        continue;
                    }
                    else
                        e = null;
                }
                if (stack != null)
                    recoverState(n);
                else if ((index = i + baseSize) >= n)
                    index = ++baseIndex; // visit upper slots if present
            }
        }

        /**
         * Saves traversal state upon encountering a forwarding node.
         */
        private void pushState(Node<V>[] t, int i, int n) {
            TableStack<V> s = spare;  // reuse if possible
            if (s != null)
                spare = s.next;
            else
                s = new TableStack<V>();
            s.tab = t;
            s.length = n;
            s.index = i;
            s.next = stack;
            stack = s;
        }

        /**
         * Possibly pops traversal state.
         *
         * @param n length of current table
         */
        private void recoverState(int n) {
            TableStack<V> s; int len;
            while ((s = stack) != null && (index += (len = s.length)) >= n) {
                n = len;
                index = s.index;
                tab = s.tab;
                s.tab = null;
                TableStack<V> next = s.next;
                s.next = spare; // save for reuse
                stack = next;
                spare = s;
            }
            if (s == null && (index += baseSize) >= n)
                index = ++baseIndex;
        }
    }

    /**
     * Base of key and value Iterator classes
     */
    static class BaseIterator<V> extends Traverser<V> {
        final ConcurrentLongHashMap<V> map;
        Node<V> lastReturned;
        BaseIterator(Node<V>[] tab, int size, int index, int limit,
                     ConcurrentLongHashMap<V> map) {
            super(tab, size, index, limit);
            this.map = map;
            advance();
        }

        public final boolean hasNext() { return next != null; }

        public final void remove() {
            Node<V> p;
            if ((p = lastReturned) == null)
                throw new IllegalStateException();
            lastReturned = null;
            map.replaceNode(p.key, null, null);
        }
    }

    static final class KeyIterator<V> extends BaseIterator<V>
        implements PrimitiveIterator.OfLong {
        KeyIterator(Node<V>[] tab, int size, int index, int limit,
                    ConcurrentLongHashMap<V> map) {
            super(tab, size, index, limit, map);
        }

        public final long nextLong() {
            Node<V> p;
            if ((p = next) == null)
                throw new NoSuchElementException();
            long k = p.key;
            lastReturned = p;
            advance();
            return k;
        }
    }

    static final class ValueIterator<V> extends BaseIterator<V>
        implements Iterator<V> {
        ValueIterator(Node<V>[] tab, int size, int index, int limit,
                      ConcurrentLongHashMap<V> map) {
            super(tab, size, index, limit, map);
        }

        public final V next() {
            Node<V> p;
            if ((p = next) == null)
                throw new NoSuchElementException();
            V v = p.val;
            lastReturned = p;
            advance();
            return v;
        }
    }

    /* ----------------Views -------------- */

    /**
     * A view of a ConcurrentLongHashMap as a {@link Collection} of
     * values, in which additions are disabled. This class cannot be
     * directly instantiated. See {@link #values()}.
     */
    static final class ValuesView<V> extends AbstractCollection<V> {
        final ConcurrentLongHashMap<V> map;
        ValuesView(ConcurrentLongHashMap<V> map) { this.map = map; }

        public final void clear()      { map.clear(); }
        public final int size()        { return map.size(); }
        public final boolean isEmpty() { return map.isEmpty(); }

        public final boolean contains(Object o) {
            return map.containsValue(o);
        }

        public final boolean remove(Object o) {
            if (o != null) {
                for (Iterator<V> it = iterator(); it.hasNext();) {
                    if (o.equals(it.next())) {
                        it.remove();
                        return true;
                    }
                }
            }
            return false;
        }

        public final Iterator<V> iterator() {
            ConcurrentLongHashMap<V> m = map;
            Node<V>[] t;
            int f = (t = m.table) == null ? 0 : t.length;
            return new ValueIterator<V>(t, f, 0, f, m);
        }

        public final boolean add(V e) {
            throw new UnsupportedOperationException();
        }
        public final boolean addAll(Collection<? extends V> c) {
            throw new UnsupportedOperationException();
        }

        public void forEach(Consumer<? super V> action) {
            if (action == null) throw new NullPointerException();
            Node<V>[] t;
            if ((t = map.table) != null) {
                Traverser<V> it = new Traverser<V>(t, t.length, 0, t.length);
                for (Node<V> p; (p = it.advance()) != null; )
                    action.accept(p.val);
            }
        }
    }

    // Unsafe mechanics
    private static final long SIZECTL;
    private static final long TRANSFERINDEX;
    private static final long BASECOUNT;
    private static final long CELLSBUSY;
    private static final long CELLVALUE;
    private static final int ABASE = GridUnsafe.arrayBaseOffset(Node[].class);
    private static final int ASHIFT;

    static {
        try {
            SIZECTL = GridUnsafe.objectFieldOffset(ConcurrentLongHashMap.class.getDeclaredField("sizeCtl"));
            TRANSFERINDEX = GridUnsafe.objectFieldOffset(ConcurrentLongHashMap.class.getDeclaredField("transferIndex"));
            BASECOUNT = GridUnsafe.objectFieldOffset(ConcurrentLongHashMap.class.getDeclaredField("baseCount"));
            CELLSBUSY = GridUnsafe.objectFieldOffset(ConcurrentLongHashMap.class.getDeclaredField("cellsBusy"));
            CELLVALUE = GridUnsafe.objectFieldOffset(CounterCell.class.getDeclaredField("value"));
        } catch (NoSuchFieldException e) {
            throw new Error(e);
        }

        int scale = GridUnsafe.arrayIndexScale(Node[].class);
        if ((scale & (scale - 1)) != 0)
            throw new ExceptionInInitializerError("array index scale not a power of two");
        ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);

        // Eager class load observed to help JIT during startup
        Class<?> ensureLoaded = ReservationNode.class;
    }
}
