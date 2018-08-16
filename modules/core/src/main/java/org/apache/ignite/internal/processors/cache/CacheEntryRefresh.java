package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;

@FunctionalInterface
public interface CacheEntryRefresh {
    public GridCacheEntryEx refresh(GridCacheEntryEx entry) throws IgniteCheckedException;
}
