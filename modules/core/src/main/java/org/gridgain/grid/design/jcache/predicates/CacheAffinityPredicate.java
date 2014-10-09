// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.jcache.predicates;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheAffinityPredicate<K, V> implements GridPredicate<Cache.Entry<K, V>> {
    /** Predicate. */
    private CachePredicate<K, V> p;

    /** Keys. */
    private K[] keys;

    /** Partitions. */
    private int[] parts;

    /**
     * Empty constructor.
     */
    public CacheAffinityPredicate() {
        // No-op.
    }

    /**
     * Constructs affinity predicate with specified affinity keys.
     *
     * @param p Predicate.
     * @param keys Affinity keys.
     */
    public CacheAffinityPredicate(CachePredicate<K, V> p, K... keys) {
        this.p = p;
        this.keys = keys;
    }

    /**
     * Constructs affinity predicate with specified affinity partitions.
     *
     * @param p Predicate.
     * @param parts Affinity partitions.
     */
    public CacheAffinityPredicate(CachePredicate<K, V> p, int[] parts) {
        this.p = p;
        this.parts = parts;
    }

    /**
     * Gets wrapped predicate.
     *
     * @return Wrapped predicate.
     */
    public CachePredicate<K, V> getPredicate() {
        return p;
    }

    /**
     * Sets wrapped predicate.
     *
     * @param p Wrapped predicate.
     */
    public void setPredicate(CachePredicate<K, V> p) {
        this.p = p;
    }

    /**
     * Gets affinity keys.
     *
     * @return Affinity keys.
     */
    public K[] getKeys() {
        return keys;
    }

    /**
     * Sets affinity keys.
     *
     * @param keys Affinity keys.
     */
    public void setKeys(K... keys) {
        this.keys = keys;
    }

    /**
     * Gets affinity partitions.
     *
     * @return Affinity partitions.
     */
    public int[] getPartitions() {
        return parts;
    }

    /**
     * Sets affinity partitions.
     *
     * @param parts Affinity partitions.
     */
    public void setPartitions(int... parts) {
        this.parts = parts;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Cache.Entry<K, V> entry) {
        return p.apply(entry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheAffinityPredicate.class, this);
    }
}
