/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.failover.always.*;
import org.gridgain.grid.spi.failover.jobstealing.*;
import org.gridgain.grid.spi.failover.never.*;

import java.util.*;

/**
 * Failover SPI provides developer with ability to supply custom logic for handling
 * failed execution of a grid job. Job execution can fail for a number of reasons:
 * <ul>
 *      <li>Job execution threw an exception (runtime, assertion or error)</li>
 *      <li>Node on which job was execution left topology (crashed or stopped)</li>
 *      <li>Collision SPI on remote node cancelled a job before it got a chance to execute (job rejection).</li>
 * </ul>
 * In all cases failover SPI takes failed job (as failover context) and list of all
 * grid nodes and provides another node on which the job execution will be retried.
 * It is up to failover SPI to make sure that job is not mapped to the node it
 * failed on. The failed node can be retrieved from
 * {@link org.apache.ignite.compute.ComputeJobResult#getNode() GridFailoverContext.getJobResult().node()}
 * method.
 * <p>
 * GridGain comes with the following built-in failover SPI implementations:
 * <ul>
 *      <li>{@link GridNeverFailoverSpi}</li>
 *      <li>{@link GridAlwaysFailoverSpi}</li>
 *      <li>{@link GridJobStealingFailoverSpi}</li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface GridFailoverSpi extends GridSpi {
    /**
     * This method is called when method {@link org.apache.ignite.compute.GridComputeTask#result(org.apache.ignite.compute.ComputeJobResult, List)} returns
     * value {@link org.apache.ignite.compute.GridComputeJobResultPolicy#FAILOVER} policy indicating that the result of
     * job execution must be failed over. Implementation of this method should examine failover
     * context and choose one of the grid nodes from supplied {@code topology} to retry job execution
     * on it. For best performance it is advised that {@link GridFailoverContext#getBalancedNode(List)}
     * method is used to select node for execution of failed job.
     *
     * @param ctx Failover context.
     * @param top Collection of all grid nodes within task topology (may include failed node).
     * @return New node to route this job to or {@code null} if new node cannot be picked.
     *      If job failover fails (returns {@code null}) the whole task will be failed.
     */
    public ClusterNode failover(GridFailoverContext ctx, List<ClusterNode> top);
}
