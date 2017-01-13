package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;

/**
 * Used to report attempts of accessing stopped cache.
 */
public class CacheStoppedException extends IgniteCheckedException {
    private static final long serialVersionUID = 0L;

    public CacheStoppedException(String cacheName) {
        super("Failed to perform cache operation (cache is stopped): " + cacheName);
    }
}