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

package org.apache.ignite.spi.loadbalancing.adaptive;

import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Implementation of node load probing based on active and waiting job count.
 * Based on {@link #setUseAverage(boolean)} parameter, this implementation will
 * either use average job count values or current (default is to use averages).
 * <p>
 * The load of a node is simply calculated by adding active and waiting job counts.
 * <p>
 * Below is an example of how CPU load probe would be configured in Ignite
 * Spring configuration file:
 * <pre name="code" class="xml">
 * &lt;property name="loadBalancingSpi"&gt;
 *     &lt;bean class="org.apache.ignite.spi.loadBalancing.adaptive.GridAdaptiveLoadBalancingSpi"&gt;
 *         &lt;property name="loadProbe"&gt;
 *             &lt;bean class="org.apache.ignite.spi.loadBalancing.adaptive.GridAdaptiveJobCountLoadProbe"&gt;
 *                 &lt;property name="useAverage" value="true"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 */
public class AdaptiveJobCountLoadProbe implements AdaptiveLoadProbe {
    /** Flag indicating whether to use average CPU load vs. current. */
    private boolean useAvg = true;

    /**
     * Initializes active job probe.
     */
    public AdaptiveJobCountLoadProbe() {
        // No-op.
    }

    /**
     * Creates new active job prove specifying whether to use average
     * job counts vs. current.
     *
     * @param useAvg Flag indicating whether to use average job counts vs. current.
     */
    public AdaptiveJobCountLoadProbe(boolean useAvg) {
        this.useAvg = useAvg;
    }

    /**
     * Gets flag indicating whether to use average job counts vs. current.
     *
     * @return Flag indicating whether to use average job counts vs. current.
     */
    public boolean isUseAverage() {
        return useAvg;
    }

    /**
     * Sets flag indicating whether to use average job counts vs. current.
     *
     * @param useAvg Flag indicating whether to use average job counts vs. current.
     */
    public void setUseAverage(boolean useAvg) {
        this.useAvg = useAvg;
    }


    /** {@inheritDoc} */
    @Override public double getLoad(ClusterNode node, int jobsSentSinceLastUpdate) {
        ClusterMetrics metrics = node.metrics();

        if (useAvg) {
            double load = metrics.getAverageActiveJobs() + metrics.getAverageWaitingJobs();

            if (load > 0)
                return load;
        }

        double load = metrics.getCurrentActiveJobs() + metrics.getCurrentWaitingJobs();

        return load < 0 ? 0 : load;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AdaptiveJobCountLoadProbe.class, this);
    }
}