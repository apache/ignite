/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.failover;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;

import java.util.*;

/**
 * Failover test context.
 */
public class GridFailoverTestContext implements FailoverContext {
    /** */
    private static final Random RAND = new Random();

    /** Grid task session. */
    private final ComputeTaskSession taskSes;

    /** Failed job result. */
    private final ComputeJobResult jobRes;

    /** */
    public GridFailoverTestContext() {
        taskSes = null;
        jobRes = null;
    }

    /**
     * Initializes failover context.
     *
     * @param taskSes Grid task session.
     * @param jobRes Failed job result.
     */
    public GridFailoverTestContext(ComputeTaskSession taskSes, ComputeJobResult jobRes) {
        this.taskSes = taskSes;
        this.jobRes = jobRes;
    }

    /** {@inheritDoc} */
    @Override public ComputeTaskSession getTaskSession() {
        return taskSes;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResult getJobResult() {
        return jobRes;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getBalancedNode(List<ClusterNode> grid) throws IgniteCheckedException {
        return grid.get(RAND.nextInt(grid.size()));
    }
}
