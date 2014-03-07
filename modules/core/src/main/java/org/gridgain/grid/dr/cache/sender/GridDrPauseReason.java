/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.sender;

import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

/**
 * Reason of data center replication pause.
 */
public enum GridDrPauseReason {
    /**
     * Method {@link GridCacheProjectionEx#drPause()} was called.
     */
    USER_REQUEST,

    /**
     * All sender hubs failed to process batch.
     */
    BATCH_FAILED,

    /**
     * All sender hubs left.
     */
    SEND_HUBS_LEFT;

    /** Enumerated values. */
    private static final GridDrPauseReason[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridDrPauseReason fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
