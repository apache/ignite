/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.jetbrains.annotations.*;

/**
 * Hadoop job state.
 */
public enum GridHadoopJobState {
    /** Running. */
    STATE_RUNNING,

    /** Succeeded. */
    STATE_SUCCEEDED,

    /** Failed. */
    STATE_FAILED,

    /** Killed. */
    STATE_KILLED;

    /** Enumerated values. */
    private static final GridHadoopJobState[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static GridHadoopJobState fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
