/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * This enumeration provides different types of actions following the last
 * received job result. See {@link GridComputeTask#result(ComputeJobResult, List)} for
 * more details.
 */
public enum GridComputeJobResultPolicy {
    /**
     * Wait for results if any are still expected. If all results have been received -
     * it will start reducing results.
     */
    WAIT,

    /** Ignore all not yet received results and start reducing results. */
    REDUCE,

    /**
     * Fail-over job to execute on another node.
     */
    FAILOVER;

    /** Enumerated values. */
    private static final GridComputeJobResultPolicy[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable
    public static GridComputeJobResultPolicy fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
