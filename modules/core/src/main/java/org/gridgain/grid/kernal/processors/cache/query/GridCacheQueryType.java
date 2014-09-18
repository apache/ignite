/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.cache.query.*;
import org.jetbrains.annotations.*;

/**
 * Defines different cache query types. For more information on cache query types
 * and their usage see {@link GridCacheQuery} documentation.
 * @see GridCacheQuery
 */
public enum GridCacheQueryType {
    /**
     * Fully scans cache returning only entries that pass certain filters.
     */
    SCAN,

    /**
     * SQL-based query.
     */
    SQL,

    /**
     * SQL-based fields query.
     */
    SQL_FIELDS,

    /**
     * Text search query.
     */
    TEXT,

    /**
     * Cache set items query.
     */
    SET;

    /** Enumerated values. */
    private static final GridCacheQueryType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridCacheQueryType fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
