/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

/**
 * Cache transaction key. This wrapper is needed because same keys may be enlisted in the same transaction
 * for multiple caches.
 */
public class GridCacheTxKey<K> {
    /** Key. */
    private K key;

    /** Cache ID. */
    private int cacheId;

    /**
     * @param key User key.
     * @param cacheId Cache ID.
     */
    public GridCacheTxKey(K key, int cacheId) {
        this.key = key;
        this.cacheId = cacheId;
    }

    /**
     * @return User key.
     */
    public K key() {
        return key;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridCacheTxKey))
            return false;

        GridCacheTxKey that = (GridCacheTxKey)o;

        return cacheId == that.cacheId && key.equals(that.key);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = key.hashCode();

        res = 31 * res + cacheId;

        return res;
    }
}
