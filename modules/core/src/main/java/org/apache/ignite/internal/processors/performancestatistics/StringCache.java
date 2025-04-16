package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Set;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

import static org.apache.ignite.IgniteCommonsSystemProperties.getInteger;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.DFLT_CACHED_STRINGS_THRESHOLD;

/** Caches strings for performance statistics writing. */
class StringCache {
    /** Maximum cached strings threshold. String caching will stop on threshold excess. */
    private final int cachedStrsThreshold = getInteger(IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD, DFLT_CACHED_STRINGS_THRESHOLD);

    /** Hashcodes of cached strings. */
    private final Set<Integer> knownStrs = new GridConcurrentHashSet<>();

    /** @return {@code True} if string was cached and can be written as hashcode. */
    boolean cacheIfPossible(String str) {
        if (knownStrs.size() >= cachedStrsThreshold)
            return false;

        return !knownStrs.add(str.hashCode());
    }
}
