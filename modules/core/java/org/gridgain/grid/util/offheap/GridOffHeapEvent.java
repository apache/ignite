/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap;

import org.jetbrains.annotations.*;

/**
 * Off-heap event types.
 *
 * @author @java.author
 * @version @java.version
 */
public enum GridOffHeapEvent {
    /** Rehash event. */
    REHASH,

    /** Memory allocate event. */
    ALLOCATE,

    /** Memory release event. */
    RELEASE;

    /** Enum values. */
    private static final GridOffHeapEvent[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridOffHeapEvent fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
