package org.apache.ignite.ml.util;

/**
 * LRU cache expiration listener.
 *
 * @param <V> Type of a value.
 */
@FunctionalInterface
public interface LRUCacheExpirationListener<V> {
    /**
     * Handles entry expiration, is called before value is moved from cache.
     *
     * @param val Value to be expired and removed.
     */
    public void entryExpired(V val);
}
