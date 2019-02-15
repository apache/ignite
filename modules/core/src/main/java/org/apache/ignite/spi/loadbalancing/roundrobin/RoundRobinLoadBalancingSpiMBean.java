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

package org.apache.ignite.spi.loadbalancing.roundrobin;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management bean for {@link RoundRobinLoadBalancingSpi} SPI.
 */
@MXBeanDescription("MBean that provides access to round robin load balancing SPI configuration.")
public interface RoundRobinLoadBalancingSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Configuration parameter indicating whether a new round robin order should be
     * created for every task. If {@code true} then load balancer is guaranteed
     * to iterate through nodes sequentially for every task - so as long as number
     * of jobs is less than or equal to the number of nodes, jobs are guaranteed to
     * be assigned to unique nodes. If {@code false} then one round-robin order
     * will be maintained for all tasks, so when tasks execute concurrently, it
     * is possible for more than one job within task to be assigned to the same
     * node.
     * <p>
     * Default is {@code true}.
     *
     * @return Configuration parameter indicating whether a new round robin order should
     *      be created for every task. Default is {@code true}.
     */
    @MXBeanDescription("Configuration parameter indicating whether a new round robin order should be created for every task.")
    public boolean isPerTask();
}