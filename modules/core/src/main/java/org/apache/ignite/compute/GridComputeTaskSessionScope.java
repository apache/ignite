/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.jetbrains.annotations.*;

/**
 * Defines life-time scopes for checkpoint operations. Such operations include:
 * <ul>
 *      <li>{@link ComputeTaskSession#saveCheckpoint(String, Object, GridComputeTaskSessionScope, long)}</li>
 * </ul>
 */
public enum GridComputeTaskSessionScope {
    /**
     * Data saved with this scope will be automatically removed
     * once the task session is completed (i.e. execution of the task is completed)
     * or when they time out. This is the most often used scope for checkpoints and swap space.
     * It provides behavior for use case when jobs can failover on other nodes
     * within the same session and thus checkpoints or data saved to swap space should be
     * preserved for the duration of the entire session.
     */
    SESSION_SCOPE,

    /**
     * Data saved with this scope will only be removed automatically
     * if they time out and time out is supported. Currently, only checkpoints support timeouts.
     * Any data, however, can always be removed programmatically via methods on {@link ComputeTaskSession}
     * interface.
     */
    GLOBAL_SCOPE;

    /** Enumerated values. */
    private static final GridComputeTaskSessionScope[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static GridComputeTaskSessionScope fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
