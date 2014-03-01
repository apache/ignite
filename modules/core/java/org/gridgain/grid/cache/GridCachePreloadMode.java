/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.cache;

import org.gridgain.grid.cache.affinity.*;
import org.jetbrains.annotations.*;

/**
 * Cache preload mode. When preloading is enabled (i.e. has value other than {@link #NONE}), distributed caches
 * will attempt to preload all necessary values from other grid nodes. This enumeration is used to configure
 * preloading via {@link GridCacheConfiguration#getPreloadMode()} configuration property. If not configured
 * explicitly, then {@link GridCacheConfiguration#DFLT_PRELOAD_MODE} is used.
 * <p>
 * Replicated caches will try to load the full set of cache entries from other nodes (or as defined by
 * pluggable {@link GridCacheAffinityFunction}), while partitioned caches will only load the entries for which
 * current node is primary or back up.
 * <p>
 * Note that preload mode only makes sense for {@link GridCacheMode#REPLICATED} and {@link GridCacheMode#PARTITIONED}
 * caches. Caches with {@link GridCacheMode#LOCAL} mode are local by definition and therefore cannot preload
 * any values from neighboring nodes.
 *
 * @author @java.author
 * @version @java.version
 */
public enum GridCachePreloadMode {
    /**
     * Synchronous preload mode. Distributed caches will not start until all necessary data
     * is loaded from other available grid nodes.
     */
    SYNC,

    /**
     * Asynchronous preload mode. Distributed caches will start immediately and will load all necessary
     * data from other available grid nodes in the background.
     */
    ASYNC,

    /**
     * In this mode no preloading will take place which means that caches will be either loaded on
     * demand from persistent store whenever data is accessed, or will be populated explicitly.
     */
    NONE;

    /** Enumerated values. */
    private static final GridCachePreloadMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCachePreloadMode fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
