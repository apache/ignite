/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.failover;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

/**
 * This interface defines a set of operations available to failover SPI
 * one a given failed job.
 */
public interface FailoverContext {
    /**
     * Gets current task session.
     *
     * @return Grid task session.
     */
    public ComputeTaskSession getTaskSession();

    /**
     * Gets failed result of job execution.
     *
     * @return Result of a failed job.
     */
    public ComputeJobResult getJobResult();

    /**
     * Gets the next balanced node for failed job. Internally this method will
     * delegate to load balancing SPI (see {@link org.apache.ignite.spi.loadbalancing.LoadBalancingSpi} to
     * determine the optimal node for execution.
     *
     * @param top Topology to pick balanced node from.
     * @return The next balanced node.
     * @throws IgniteException If anything failed.
     */
    public ClusterNode getBalancedNode(List<ClusterNode> top) throws IgniteException;

    /**
     * Gets partition for {@link IgniteCompute#affinityRun(Collection, int, IgniteRunnable)}
     * and {@link IgniteCompute#affinityCall(Collection, int, IgniteCallable)}.
     *
     * @return Partition number.
     */
    public int partition();

    /**
     * Returns affinity cache name {@link IgniteCompute#affinityRun(String, Object, IgniteRunnable)}
     * and {@link IgniteCompute#affinityCall(String, Object, IgniteCallable)}.
     *
     * @return Cache name.
     */
    @Nullable public String affinityCacheName();
}