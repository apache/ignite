/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.failover;

import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.spi.IgniteSpi;

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
 * Ignite comes with the following built-in failover SPI implementations:
 * <ul>
 *      <li>{@link org.apache.ignite.spi.failover.never.NeverFailoverSpi}</li>
 *      <li>{@link org.apache.ignite.spi.failover.always.AlwaysFailoverSpi}</li>
 *      <li>{@link org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi}</li>
 * </ul>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by Ignite kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 */
public interface FailoverSpi extends IgniteSpi {
    /**
     * This method is called when method {@link org.apache.ignite.compute.ComputeTask#result(org.apache.ignite.compute.ComputeJobResult, List)} returns
     * value {@link org.apache.ignite.compute.ComputeJobResultPolicy#FAILOVER} policy indicating that the result of
     * job execution must be failed over. Implementation of this method should examine failover
     * context and choose one of the grid nodes from supplied {@code topology} to retry job execution
     * on it. For best performance it is advised that {@link FailoverContext#getBalancedNode(List)}
     * method is used to select node for execution of failed job.
     *
     * @param ctx Failover context.
     * @param top Collection of all grid nodes within task topology (may include failed node).
     * @return New node to route this job to or {@code null} if new node cannot be picked.
     *      If job failover fails (returns {@code null}) the whole task will be failed.
     */
    public ClusterNode failover(FailoverContext ctx, List<ClusterNode> top);
}