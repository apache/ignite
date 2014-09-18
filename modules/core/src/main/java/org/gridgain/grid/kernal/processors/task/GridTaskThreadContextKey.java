/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.task;

/**
 * Defines keys for thread-local context in task processor.
 */
public enum GridTaskThreadContextKey {
    /** Task name. */
    TC_TASK_NAME,

    /** No failover flag. */
    TC_NO_FAILOVER,

    /** Projection for the task. */
    TC_SUBGRID,

    /** Timeout in milliseconds associated with the task. */
    TC_TIMEOUT,

    /** Security subject ID. */
    TC_SUBJ_ID
}
