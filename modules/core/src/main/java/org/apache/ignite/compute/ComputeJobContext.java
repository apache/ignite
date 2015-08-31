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

package org.apache.ignite.compute;

import java.util.List;
import java.util.Map;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Context attached to every job executed on the grid. Note that unlike
 * {@link ComputeTaskSession}, which distributes all attributes to all jobs
 * in the task including the task itself, job context attributes belong
 * to a job and do not get sent over network unless a job moves from one
 * node to another.
 * <p>
 * In most cases a job, once assigned to a node, will never move to another
 * node. However, it is possible that collision SPI rejects a job before
 * it ever got a chance to execute (job rejection) which will cause fail-over
 * to another node. Or user is not satisfied with the outcome of a job and
 * fails it over to another node by returning {@link ComputeJobResultPolicy#FAILOVER}
 * policy from {@link ComputeTask#result(ComputeJobResult, List)} method. In this case
 * all context attributes set on one node will be available on any other node
 * this job travels to.
 * <p>
 * You can also use {@code GridComputeJobContext} to communicate between SPI's and jobs.
 * For example, if you need to cancel an actively running job from {@link org.apache.ignite.spi.collision.CollisionSpi}
 * you may choose to set some context attribute on the job to mark the fact
 * that a job was cancelled by grid and not by a user. Context attributes can
 * also be assigned in {@link org.apache.ignite.spi.failover.FailoverSpi} prior to failing over a job.
 * <p>
 * From within {@link ComputeTask#result(ComputeJobResult, List)} or {@link ComputeTask#reduce(List)} methods,
 * job context is available via {@link ComputeJobResult#getJobContext()} method which gives user the
 * ability to check context attributes from within grid task implementation for every job
 * returned from remote nodes.
 * <p>
 * Job context can be injected into {@link ComputeJob} via {@link org.apache.ignite.resources.JobContextResource}
 * annotation. Refer to the {@link org.apache.ignite.resources.JobContextResource}
 * documentation for coding examples on how to inject job context.
 * <p>
 * Attribute names that start with {@code "apache.ignite:"} are reserved for internal system use.
 */
public interface ComputeJobContext extends ComputeJobContinuation {
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