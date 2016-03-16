package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Atomic cache version comparator.
 */
public interface CacheAtomicVersionComparator {
    /**
     * Compares two cache versions.
     *
     * @param one First version.
     * @param other Second version.
     * @param ignoreTime {@code True} if global time should be ignored.
     * @return Comparison value.
     */
    public int compare(GridCacheVersion one, GridCacheVersion other, boolean ignoreTime);
}
