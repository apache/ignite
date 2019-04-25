/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.loadbalancing.weightedrandom;

import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.spi.IgniteSpiManagementMBean;

/**
 * Management MBean for {@link WeightedRandomLoadBalancingSpi} SPI.
 */
@MXBeanDescription("MBean that provides access to weighted random load balancing SPI configuration.")
public interface WeightedRandomLoadBalancingSpiMBean extends IgniteSpiManagementMBean {
    /**
     * Checks whether node weights are considered when doing
     * random load balancing.
     *
     * @return If {@code true} then random load is distributed according
     *      to node weights.
     */
    @MXBeanDescription("Whether node weights are considered when doing random load balancing.")
    public boolean isUseWeights();

    /**
     * Gets weight of this node.
     *
     * @return Weight of this node.
     */
    @MXBeanDescription("Weight of this node.")
    public int getNodeWeight();
}