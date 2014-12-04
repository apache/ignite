/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision;

import java.util.*;

/**
 * Context for resolving collisions. This context contains collections of
 * waiting jobs, active jobs, and held jobs. If continuations are not used
 * (see {@link org.apache.ignite.compute.ComputeJobContinuation}), then collection of held jobs will
 * always be empty. {@link GridCollisionSpi} will manipulate these lists
 * to make sure that only allowed number of jobs are running in parallel or
 * waiting to be executed.
 * @since 3.5
 */
public interface GridCollisionContext {
    /**
     * Gets ordered collection of collision contexts for jobs that are currently executing.
     * It can be empty but never {@code null}.
     *
     * @return Ordered number of collision contexts for currently executing jobs.
     */
    public Collection<GridCollisionJobContext> activeJobs();

    /**
     * Gets ordered collection of collision contexts for jobs that are currently waiting
     * for execution. It can be empty but never {@code null}. Note that a newly
     * arrived job, if any, will always be represented by the last item in this list.
     * <p>
     * This list is guaranteed not to change while
     * {@link GridCollisionSpi#onCollision(GridCollisionContext)} method is being executed.
     *
     * @return Ordered collection of collision contexts for waiting jobs.
     */
    public Collection<GridCollisionJobContext> waitingJobs();

    /**
     * Gets collection of jobs that are currently in {@code held} state. Job can enter
     * {@code held} state by calling {@link org.apache.ignite.compute.ComputeJobContinuation#holdcc()} method at
     * which point job will release all resources and will get suspended. If
     * {@link org.apache.ignite.compute.ComputeJobContinuation job continuations} are not used, then this list
     * will always be empty, but never {@code null}.
     *
     * @return Collection of jobs that are currently in {@code held} state.
     */
    public Collection<GridCollisionJobContext> heldJobs();
}
