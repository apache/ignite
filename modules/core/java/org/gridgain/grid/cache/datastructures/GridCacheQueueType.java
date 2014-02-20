// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.datastructures;

import org.jetbrains.annotations.*;

/**
 * Supported distributed cache queue types.
 *
 * @author @java.author
 * @version @java.version
 * @see GridCacheQueue
 */
public enum GridCacheQueueType {
    /** FIFO (first-in-first-out). */
    FIFO,

    /** LIFO (last-in-first-out). */
    LIFO,

    /** Weighted priority. */
    PRIORITY;

    /** */
    private static final GridCacheQueueType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridCacheQueueType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
