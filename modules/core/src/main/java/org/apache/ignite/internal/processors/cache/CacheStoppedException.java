package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;

/**
 * Used to report attempts of accessing stopped cache.
 */
public class CacheStoppedException extends IgniteCheckedException {
    public CacheStoppedException(String cacheName) {
        super("Failed to perform cache operation (cache is stopped): " + cacheName);
    }
}