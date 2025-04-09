package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.GridConcurrentHashSet;

/** Class to cache strings for performance statistics writing */
public class StringCache {
    /** Default maximum cached strings threshold. String caching will stop on threshold excess. */
    public static final int DFLT_CACHED_STRINGS_THRESHOLD = 10 * 1024;

    /** Maximum cached strings threshold. String caching will stop on threshold excess. */
    private final int cachedStrsThreshold = IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_PERF_STAT_CACHED_STRINGS_THRESHOLD,
        DFLT_CACHED_STRINGS_THRESHOLD);

    /** Hashcodes of cached strings. */
    private final Set<Integer> knownStrs = new GridConcurrentHashSet<>();

    /** Count of cached strings. */
    private volatile int knownStrsSz;

    /** @return {@code True} if string was cached and can be written as hashcode. */
    public boolean cacheIfPossible(String str) {
        // We can cache slightly more strings then threshold value.
        // Don't implement solution with synchronization here, because our primary goal is avoid any contention.
        if (knownStrsSz >= cachedStrsThreshold)
            return false;

        int hash = str.hashCode();

        // We can cache slightly more strings then threshold value.
        // Don't implement solution with synchronization here, because our primary goal is avoid any contention.
        if (knownStrs.contains(hash) || !knownStrs.add(hash))
            return true;

        knownStrsSz = knownStrs.size();

        return false;
    }
}
