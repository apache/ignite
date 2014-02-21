// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.task;

import java.util.*;

/**
 * Defines keys for thread-local context in task processor.
 *
 * @author @java.author
 * @version @java.version
 */
public enum GridTaskThreadContextKey {
    /** Failover SPI name. */
    TC_FAILOVER_SPI,

    /** Load balancing SPI name. */
    TC_LOAD_BALANCING_SPI,

    /** Checkpoint SPI name. */
    TC_CHECKPOINT_SPI,

    /** Task name. */
    TC_TASK_NAME,

    /** Ad-hoc task {@link org.gridgain.grid.compute.GridComputeTask#result(org.gridgain.grid.compute.GridComputeJobResult, List)} method implementation. */
    TC_RESULT,

    /** Projection for the task. */
    TC_SUBGRID,

    /** Timeout in milliseconds associated with the task. */
    TC_TIMEOUT,

    /** Task session full support flag. */
    TC_SES_FULL_SUPPORT
}
