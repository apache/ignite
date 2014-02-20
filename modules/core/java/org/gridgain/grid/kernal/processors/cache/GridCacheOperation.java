// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.jetbrains.annotations.*;

/**
 * Cache value operations.
 *
 * @author @java.author
 * @version @java.version
 */
public enum GridCacheOperation {
    /** Read operation. */
    READ,

    /** Create operation. */
    CREATE,

    /** Update operation. */
    UPDATE,

    /** Delete operation. */
    DELETE,

    /** Transform operation. A closure will be applied to the previous entry value. */
    TRANSFORM,

    /** Cache operation used to indicate reload during transaction recovery. */
    RELOAD,

    /**
     * This operation is used when lock has been acquired,
     * but filter validation failed.
     */
    NOOP;

    /** Enum values. */
    private static final GridCacheOperation[] VALS = values();

    /**
     * @param ord Ordinal value.
     * @return Enum value.
     */
    @Nullable public static GridCacheOperation fromOrdinal(int ord) {
        return ord < 0 || ord >= VALS.length ? null : VALS[ord];
    }
}