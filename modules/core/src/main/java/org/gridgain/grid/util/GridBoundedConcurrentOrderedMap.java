/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Concurrent ordered map that automatically manages its maximum size.
 * Once it exceeds its maximum, it will start removing smallest elements
 * until the maximum is reached again. If optional listener is set
 * via {@link #evictionListener(org.apache.ignite.lang.IgniteBiInClosure)} method, then listener
 * will be notified for every eviction.
 * <p>
 * Note that due to concurrent nature of this map, it may grow slightly
 * larger than its maximum allowed size, but in this case it will quickly
 * readjust back to allowed size.
 * <p>
 * Note that {@link #remove(Object)} and {@link #remove(Object, Object)} methods
 * are not supported for this kind of map.
 */
public class GridBoundedConcurrentOrderedMap<K, V> extends ConcurrentSkipListMap<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Element count. */
    private final AtomicInteger cnt = new AtomicInteger(0);

    /** Maximum size. */
    private int max;

    /** Listener. */
    private volatile IgniteBiInClosure<K, V> lsnr;

    /**
     * Constructs a new, empty map that orders its elements according to
     * their {@linkplain Comparable natural ordering}.
     *
     * @param max Upper bound of this map.
     */
    public GridBoundedConcurrentOrderedMap(int max) {
        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /**
     * Constructs a new, empty set that orders its elements according to
     * the specified comparator.
     *
     * @param max Upper bound of this map.
     * @param comparator The comparator that will be used to order this map.
     *      If <tt>null</tt>, the {@linkplain Comparable natural
     *      ordering} of the elements will be used.
     */
    public GridBoundedConcurrentOrderedMap(int max, Comparator<? super K> comparator) {
        super(comparator);

        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /**
     * Constructs a new map containing the elements in the specified
     * map, that orders its elements according to their
     * {@linkplain Comparable natural ordering}.
     *
     * @param max Upper bound of this map.
     * @param map The elements that will comprise the new map.
     * @throws ClassCastException if the elements in <tt>map</tt> are
     *      not {@link Comparable}, or are not mutually comparable.
     * @throws NullPointerException if the specified map or any
     *      of its elements are {@code null}.
     */
    public GridBoundedConcurrentOrderedMap(int max, Map<? extends K, ? extends V> map) {
        super(map);

        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /**
     * Constructs a new map containing the same elements and using the
     * same ordering as the specified sorted map.
     *
     * @param max Upper bound of this map.
     * @param map Sorted map whose elements will comprise the new map.
     * @throws NullPointerException if the specified sorted map or any
     *      of its elements are {@code null}.
     */
    public GridBoundedConcurrentOrderedMap(int max, SortedMap<K, V> map) {
        super(map);

        A.ensure(max > 0, "max > 0");

        this.max = max;
    }

    /**
     * Sets closure listener to be called for every eviction event.
     *
     * @param lsnr Closure to be called for every eviction event.
     */
    public void evictionListener(IgniteBiInClosure<K, V> lsnr) {
        this.lsnr = lsnr;
    }

    /**
     * Gets closure listener to be called for every eviction event.
     *
     * @return Closure to be called for every eviction event.
     */
    public IgniteBiInClosure<K, V> evictionListener() {
        return lsnr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V put(K k, V v) {
        A.notNull(k, "k", v, "v");

        V ret = super.put(k, v);

        onPut();

        return ret;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V putIfAbsent(K k, V v) {
        A.notNull(k, "k", v, "v");

        V ret = super.putIfAbsent(k, v);

        // Handle size only if put succeeded.
        if (ret == null)
            onPut();

        return ret;
    }

    /**
     * Handles size after put.
     */
    private void onPut() {
        cnt.incrementAndGet();

        int c;

        while ((c = cnt.get()) > max) {
            // Decrement count.
            if (cnt.compareAndSet(c, c - 1)) {
                try {
                    K key = firstEntry().getKey();

                    V val;

                    // Make sure that an element is removed.
                    while ((val = super.remove(firstEntry().getKey())) == null) {
                        // No-op.
                    }

                    assert val != null;

                    IgniteBiInClosure<K, V> lsnr = this.lsnr;

                    // Listener notification.
                    if (lsnr != null)
                        lsnr.apply(key, val);
                }
                catch (NoSuchElementException e1) {
                    e1.printStackTrace(); // Should never happen.

                    assert false : "Internal error in grid bounded ordered set.";
                }
            }
        }
    }

    /**
     * Approximate size at this point of time. Note, that unlike {@code size}
     * methods on other {@code concurrent} collections, this method executes
     * in constant time without traversal of the elements.
     *
     * @return Approximate set size at this point of time.
     */
    @Override public int size() {
        return cnt.get();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public GridBoundedConcurrentOrderedMap<K, V> clone() {
        GridBoundedConcurrentOrderedMap<K, V> map = (GridBoundedConcurrentOrderedMap<K, V>)super.clone();

        map.max = max;

        return map;
    }

    /**
     * This method is not supported and always throws {@link UnsupportedOperationException}.
     *
     * @param o {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override public V remove(Object o) {
        throw new UnsupportedOperationException("Remove is not supported on concurrent bounded map.");
    }

    /**
     * This method is not supported and always throws {@link UnsupportedOperationException}.
     *
     * @param key {@inheritDoc}
     * @param val {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override public boolean remove(Object key, Object val) {
        throw new UnsupportedOperationException("Remove is not supported on concurrent bounded map.");
    }
}
