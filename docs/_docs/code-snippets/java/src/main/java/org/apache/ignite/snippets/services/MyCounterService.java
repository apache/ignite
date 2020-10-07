package org.apache.ignite.snippets.services;

import javax.cache.CacheException;

public interface MyCounterService {
    /**
     * Increment counter value and return the new value.
     */
    int increment() throws CacheException;

    /**
     * Get current counter value.
     */
    int get() throws CacheException;

}
