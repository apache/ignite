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
* Task type.
*/
public enum GridHadoopTaskType {
    /** Setup task. */
    SETUP,

    /** Map task. */
    MAP,

    /** Reduce task. */
    REDUCE,

    /** Combine task. */
    COMBINE,

    /** Commit task. */
    COMMIT,

    /** Abort task. */
    ABORT;

    /** Enumerated values. */
    private static final GridHadoopTaskType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridHadoopTaskType fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
