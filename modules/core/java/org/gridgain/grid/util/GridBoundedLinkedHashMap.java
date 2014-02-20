// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import java.util.*;

/**
 * Generic map with an upper bound. Once map reaches its maximum capacity,
 * the eldest elements will be removed based on insertion order.
 *
 * @param <K> Map key.
 * @param <V> Map entry.
 * @author @java.author
 * @version @java.version
 */
public class GridBoundedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
    /** Maximum map capacity. */
    private int maxCap;

    /**
     * Constructs an empty insertion-ordered {@code GridBoundedLinkedHashMap} instance
     * with the specified maximum capacity, and a default load factor (0.75).
     *
     * @param maxCap Maximum map capacity.
     * @throws IllegalArgumentException If the maximum capacity is negative.
     */
    public GridBoundedLinkedHashMap(int maxCap) throws IllegalArgumentException {
        if (maxCap <= 0)
            throw new IllegalArgumentException("Maximum capacity is non-positive: " + maxCap);

        this.maxCap = maxCap;
    }

    /**
     * Constructs an empty insertion-ordered {@code GridBoundedLinkedHashMap} instance
     * with the specified initial capacity, maximum capacity, and a default load factor (0.75).
     *
     * @param initCap Initial map capacity.
     * @param maxCap Maximum map capacity.
     * @throws IllegalArgumentException If the initial capacity is negative,
     *      or maximum capacity is smaller than initial capacity.
     */
    public GridBoundedLinkedHashMap(int initCap, int maxCap) throws IllegalArgumentException {
        super(initCap);

        if (maxCap <= initCap)
            throw new IllegalArgumentException("Maximum capacity is smaller than initial capacity [maxCap=" +
                maxCap + ", initCap=" + initCap + ']');

        this.maxCap = maxCap;
    }

    /**
     * Constructs an empty insertion-ordered {@code GridBoundedLinkedHashMap} instance
     * with the specified initial capacity, maximum capacity, and load factor.
     *
     * @param initCap Initial map capacity.
     * @param maxCap Maximum map capacity.
     * @param loadFactor Load factor.
     * @throws IllegalArgumentException If the initial capacity is negative,
     *      the load factor is non-positive, or maximum capacity is smaller
     *      than initial capacity.
     */
    public GridBoundedLinkedHashMap(int initCap, int maxCap, float loadFactor) throws IllegalArgumentException {
        super(initCap, loadFactor);

        if (maxCap >= initCap)
            throw new IllegalArgumentException("Maximum capacity is smaller than initial capacity [maxCap=" +
                maxCap + ", initCap=" + initCap + ']');

        this.maxCap = maxCap;
    }

    /**
     * Constructs an empty {@code GridBoundedLinkedHashMap} instance with the
     * specified initial capacity, maximum capacity, load factor and ordering mode.
     *
     * @param initCap Initial map capacity.
     * @param maxCap Maximum map capacity.
     * @param loadFactor Load factor.
     * @param accessOrder The ordering mode - {@code true} for
     *      access-order, {@code false} for insertion-order.
     * @throws IllegalArgumentException If the initial capacity is negative,
     *      the load factor is non-positive, or maximum capacity is smaller
     *      than initial capacity.
     */
    public GridBoundedLinkedHashMap(int initCap, int maxCap, float loadFactor, boolean accessOrder)
        throws IllegalArgumentException {
        super(initCap, loadFactor, accessOrder);

        if (maxCap >= initCap)
            throw new IllegalArgumentException("Maximum capacity is smaller than initial capacity [maxCap=" +
                maxCap + ", initCap=" + initCap + ']');

        this.maxCap = maxCap;
    }

    /** {@inheritDoc} */
    @Override protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maxCap;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public Object clone() {
        GridBoundedLinkedHashMap<K, V> m = (GridBoundedLinkedHashMap<K, V>)super.clone();

        m.maxCap = maxCap;

        return m;
    }
}
