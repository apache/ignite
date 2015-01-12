/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.continuous;

import org.jetbrains.annotations.*;

/**
 * Continuous processor message types.
 */
enum GridContinuousMessageType {
    /** Consume start request. */
    MSG_START_REQ,

    /** Consume start acknowledgement. */
    MSG_START_ACK,

    /** Consume stop request. */
    MSG_STOP_REQ,

    /** Consume stop acknowledgement. */
    MSG_STOP_ACK,

    /** Remote event notification. */
    MSG_EVT_NOTIFICATION,

    /** Event notification acknowledgement for synchronous events. */
    MSG_EVT_ACK;

    /** Enumerated values. */
    private static final GridContinuousMessageType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridContinuousMessageType fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
