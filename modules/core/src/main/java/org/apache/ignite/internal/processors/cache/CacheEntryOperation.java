package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;

@FunctionalInterface
public interface CacheEntryOperation<R> {
    public R invoke() throws IgniteCheckedException, GridCacheEntryRemovedException;
}
