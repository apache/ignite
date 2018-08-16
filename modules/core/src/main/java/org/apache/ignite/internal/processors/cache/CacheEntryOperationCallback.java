package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;

@FunctionalInterface
public interface CacheEntryOperationCallback<R> {
    void invoke(GridCacheEntryEx entry, R result) throws IgniteCheckedException;
}
