// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.jcache.query;

import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public final class QueryAffinityPredicate<K, V> extends QueryPredicate<K, V> {
    /** Predicate. */
    private QueryPredicate<K, V> p;

    /** Keys. */
    private K[] keys;

    /** Partitions. */
    private int[] parts;

    /**
     * Empty constructor.
     */
    public QueryAffinityPredicate() {
        // No-op.
    }

    /**
     * Constructs affinity predicate with specified affinity keys.
     *
     * @param p Predicate.
     * @param keys Affinity keys.
     */
    public QueryAffinityPredicate(QueryPredicate<K, V> p, K... keys) {
        this.p = p;
        this.keys = keys;
    }

    /**
     * Constructs affinity predicate with specified affinity partitions.
     *
     * @param p Predicate.
     * @param parts Affinity partitions.
     */
    public QueryAffinityPredicate(QueryPredicate<K, V> p, int[] parts) {
        this.p = p;
        this.parts = parts;
    }

    /**
     * Gets wrapped predicate.
     *
     * @return Wrapped predicate.
     */
    public QueryPredicate<K, V> getPredicate() {
        return p;
    }

    /**
     * Sets wrapped predicate.
     *
     * @param p Wrapped predicate.
     */
    public void setPredicate(QueryPredicate<K, V> p) {
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
    @Override public final boolean apply(Cache.Entry<K, V> entry) {
        return p.apply(entry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryAffinityPredicate.class, this);
    }
}
