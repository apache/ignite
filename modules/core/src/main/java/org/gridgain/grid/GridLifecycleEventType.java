/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.jetbrains.annotations.*;

/**
 * Grid lifecycle event types. These events are used to notify lifecycle beans
 * about changes in grid lifecycle state.
 * <p>
 * For more information and detailed examples refer to {@link LifecycleBean}
 * documentation.
 */
public enum GridLifecycleEventType {
    /**
     * Invoked before grid startup routine. Grid is not
     * initialized and cannot be used.
     */
    BEFORE_GRID_START,

    /**
     * Invoked after grid startup is complete. Grid is fully
     * initialized and fully functional.
     */
    AFTER_GRID_START,

    /**
     * Invoked before grid stopping routine. Grid is fully functional
     * at this point.
     */
    BEFORE_GRID_STOP,

    /**
     * Invoked after grid had stopped. Grid is stopped and
     * cannot be used.
     */
    AFTER_GRID_STOP;

    /** Enumerated values. */
    private static final GridLifecycleEventType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridLifecycleEventType fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
