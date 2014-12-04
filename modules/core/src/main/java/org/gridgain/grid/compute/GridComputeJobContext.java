/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute;

import org.apache.ignite.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.collision.*;
import org.gridgain.grid.spi.failover.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Context attached to every job executed on the grid. Note that unlike
 * {@link GridComputeTaskSession}, which distributes all attributes to all jobs
 * in the task including the task itself, job context attributes belong
 * to a job and do not get sent over network unless a job moves from one
 * node to another.
 * <p>
 * In most cases a job, once assigned to a node, will never move to another
 * node. However, it is possible that collision SPI rejects a job before
 * it ever got a chance to execute (job rejection) which will cause fail-over
 * to another node. Or user is not satisfied with the outcome of a job and
 * fails it over to another node by returning {@link GridComputeJobResultPolicy#FAILOVER}
 * policy from {@link GridComputeTask#result(GridComputeJobResult, List)} method. In this case
 * all context attributes set on one node will be available on any other node
 * this job travels to.
 * <p>
 * You can also use {@code GridComputeJobContext} to communicate between SPI's and jobs.
 * For example, if you need to cancel an actively running job from {@link GridCollisionSpi}
 * you may choose to set some context attribute on the job to mark the fact
 * that a job was cancelled by grid and not by a user. Context attributes can
 * also be assigned in {@link GridFailoverSpi} prior to failing over a job.
 * <p>
 * From within {@link GridComputeTask#result(GridComputeJobResult, List)} or {@link GridComputeTask#reduce(List)} methods,
 * job context is available via {@link GridComputeJobResult#getJobContext()} method which gives user the
 * ability to check context attributes from within grid task implementation for every job
 * returned from remote nodes.
 * <p>
 * Job context can be injected into {@link GridComputeJob} via {@link GridJobContextResource}
 * annotation. Refer to the {@link GridJobContextResource}
 * documentation for coding examples on how to inject job context.
 * <p>
 * Attribute names that start with {@code "gridgain:"} are reserved for internal system use.
 */
public interface GridComputeJobContext extends GridComputeJobContinuation {
    /**
     * Gets cache name for which job was co-located.
     *
     * @return Cache name if job was co-located or {@code null} otherwise.
     * @see #affinityKey()
     * @see IgniteCompute#affinityCall(String, Object, Callable)
     * @see IgniteCompute#affinityRun(String, Object, Runnable)
     */
    @Nullable public String cacheName();

    /**
     * Gets affinity key with which job was co-located.
     *
     * @return Affinity key if job was co-located or {@code null} otherwise.
     * @see #cacheName()
     * @see IgniteCompute#affinityCall(String, Object, Callable)
     * @see IgniteCompute#affinityRun(String, Object, Runnable)
     */
    @Nullable public <T> T affinityKey();

    /**
     * Gets ID of the job this context belongs to.
     *
     * @return ID of the job this context belongs to.
     */
    public IgniteUuid getJobId();

    /**
     * Sets an attribute into this job context.
     *
     * @param key Attribute key.
     * @param val Attribute value.
     */
    public void setAttribute(Object key, @Nullable Object val);

    /**
     * Sets map of attributes into this job context.
     *
     * @param attrs Local attributes.
     */
    public void setAttributes(Map<?, ?> attrs);

    /**
     * Gets attribute from this job context.
     *
     * @param key Attribute key.
     * @param <K> Type of the attribute key.
     * @param <V> Type of the attribute value.
     * @return Attribute value (possibly {@code null}).
     */
    public <K, V> V getAttribute(K key);

    /**
     * Gets all attributes present in this job context.
     *
     * @return All attributes.
     */
    public Map<?, ?> getAttributes();
}
