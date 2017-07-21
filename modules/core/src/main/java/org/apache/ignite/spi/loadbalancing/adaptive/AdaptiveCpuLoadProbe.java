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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Implementation of node load probing based on CPU load.
 * <p>
 * Based on {@link #setUseAverage(boolean)}
 * parameter, this implementation will either use average CPU load
 * values or current (default is to use averages).
 * <p>
 * Based on {@link #setUseProcessors(boolean)} parameter, this implementation
 * will either take number of processors on the node into account or not.
 * Since CPU load on multi-processor boxes shows medium load of multiple CPU's it
 * usually means that the remaining capacity is proportional to the number of
 * CPU's (or cores) on the node. This configuration parameter indicates
 * whether to divide each node's CPU load by the number of processors on that node
 * (default is {@code true}).
 * <p>
 * Also note that in some environments every processor may not be adding 100% of
 * processing power. For example, if you are using multi-core CPU's, then addition of
 * every core would probably result in about 75% of extra CPU power. To account
 * for that, you should set {@link #setProcessorCoefficient(double)} parameter to
 * {@code 0.75} .
 * <p>
 * Below is an example of how CPU load probe would be configured in Ignite
 * Spring configuration file:
 * <pre name="code" class="xml">
 * &lt;property name="loadBalancingSpi"&gt;
 *     &lt;bean class="org.apache.ignite.spi.loadBalancing.adaptive.GridAdaptiveLoadBalancingSpi"&gt;
 *         &lt;property name="loadProbe"&gt;
 *             &lt;bean class="org.apache.ignite.spi.loadBalancing.adaptive.GridAdaptiveCpuLoadProbe"&gt;
 *                 &lt;property name="useAverage" value="true"/&gt;
 *                 &lt;property name="useProcessors" value="true"/&gt;
 *                 &lt;property name="processorCoefficient" value="0.9"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * This implementation is used by default by {@link AdaptiveLoadBalancingSpi} SPI.
 */
public class AdaptiveCpuLoadProbe implements AdaptiveLoadProbe {
    /** Flag indicating whether to use average CPU load vs. current. */
    private boolean useAvg = true;

    /**
     * Flag indicating whether to divide each node's CPU load
     * by the number of processors on that node.
     */
    private boolean useProcs = true;

    /**
     * Coefficient of every CPU processor. By default it is {@code 1}, but
     * in some environments every processor may not be adding 100% of processing
     * power. For example, if you are using multi-core CPU's, then addition of
     * every core would probably result in about 75% of extra CPU power, and hence
     * you would set this coefficient to {@code 0.75} .
     */
    private double procCoefficient = 1;

    /**
     * Initializes CPU load probe to use CPU load average by default.
     */
    public AdaptiveCpuLoadProbe() {
        // No-op.
    }

    /**
     * Specifies whether to use average CPU load vs. current and whether or
     * not to take number of processors into account.
     * <p>
     * Since CPU load on multi-processor boxes shows medium load of multiple CPU's it
     * usually means that the remaining capacity is proportional to the number of
     * CPU's (or cores) on the node.
     *
     * @param useAvg Flag indicating whether to use average CPU load vs. current
     *      (default is {@code true}).
     * @param useProcs Flag indicating whether to divide each node's CPU load
     *      by the number of processors on that node (default is {@code true}).
     */
    public AdaptiveCpuLoadProbe(boolean useAvg, boolean useProcs) {
        this.useAvg = useAvg;
        this.useProcs = useProcs;
    }

    /**
     * Specifies whether to use average CPU load vs. current and whether or
     * not to take number of processors into account. It also allows to
     * specify the coefficient of addition power every CPU adds.
     * <p>
     * Since CPU load on multi-processor boxes shows medium load of multiple CPU's it
     * usually means that the remaining capacity is proportional to the number of
     * CPU's (or cores) on the node.
     * <p>
     * Also, in some environments every processor may not be adding 100% of processing
     * power. For example, if you are using multi-core CPU's, then addition of
     * every core would probably result in about 75% of extra CPU power, and hence
     * you would set this coefficient to {@code 0.75} .
     *
     * @param useAvg Flag indicating whether to use average CPU load vs. current
     *      (default is {@code true}).
     * @param useProcs Flag indicating whether to divide each node's CPU load
     *      by the number of processors on that node (default is {@code true}).
     * @param procCoefficient Coefficient of every CPU processor (default value is {@code 1}).
     */
    public AdaptiveCpuLoadProbe(boolean useAvg, boolean useProcs, double procCoefficient) {
        this.useAvg = useAvg;
        this.useProcs = useProcs;
        this.procCoefficient = procCoefficient;
    }

    /**
     * Gets flag indicating whether to use average CPU load vs. current.
     *
     * @return Flag indicating whether to use average CPU load vs. current.
     */
    public boolean isUseAverage() {
        return useAvg;
    }

    /**
     * Sets flag indicating whether to use average CPU load vs. current.
     * If not explicitly set, then default value is {@code true}.
     *
     * @param useAvg Flag indicating whether to use average CPU load vs. current.
     */
    public void setUseAverage(boolean useAvg) {
        this.useAvg = useAvg;
    }

    /**
     * Gets flag indicating whether to use average CPU load vs. current
     * (default is {@code true}).
     * <p>
     * Since CPU load on multi-processor boxes shows medium load of multiple CPU's it
     * usually means that the remaining capacity is proportional to the number of
     * CPU's (or cores) on the node.
     *
     * @return Flag indicating whether to divide each node's CPU load
     *      by the number of processors on that node (default is {@code true}).
     */
    public boolean isUseProcessors() {
        return useProcs;
    }

    /**
     * Sets flag indicating whether to use average CPU load vs. current
     * (default is {@code true}).
     * <p>
     * Since CPU load on multi-processor boxes shows medium load of multiple CPU's it
     * usually means that the remaining capacity is proportional to the number of
     * CPU's (or cores) on the node.
     * <p>
     * If not explicitly set, then default value is {@code true}.
     *
     * @param useProcs Flag indicating whether to divide each node's CPU load
     *      by the number of processors on that node (default is {@code true}).
     */
    public void setUseProcessors(boolean useProcs) {
        this.useProcs = useProcs;
    }

    /**
     * Gets coefficient of every CPU processor. By default it is {@code 1}, but
     * in some environments every processor may not be adding 100% of processing
     * power. For example, if you are using multi-core CPU's, then addition of
     * every core would probably result in about 75% of extra CPU power, and hence
     * you would set this coefficient to {@code 0.75} .
     * <p>
     * This value is ignored if {@link #isUseProcessors()} is set to {@code false}.
     *
     * @return Coefficient of every CPU processor.
     */
    public double getProcessorCoefficient() {
        return procCoefficient;
    }

    /**
     * Sets coefficient of every CPU processor. By default it is {@code 1}, but
     * in some environments every processor may not be adding 100% of processing
     * power. For example, if you are using multi-core CPU's, then addition of
     * every core would probably result in about 75% of extra CPU power, and hence
     * you would set this coefficient to {@code 0.75} .
     * <p>
     * This value is ignored if {@link #isUseProcessors()} is set to {@code false}.
     *
     * @param procCoefficient Coefficient of every CPU processor.
     */
    public void setProcessorCoefficient(double procCoefficient) {
        A.ensure(procCoefficient > 0, "procCoefficient > 0");

        this.procCoefficient = procCoefficient;
    }

    /** {@inheritDoc} */
    @Override public double getLoad(ClusterNode node, int jobsSentSinceLastUpdate) {
        ClusterMetrics metrics = node.metrics();

        double k = 1.0d;

        if (useProcs) {
            int procs = metrics.getTotalCpus();

            if (procs > 1)
                k = procs * procCoefficient;
        }

        double load = (useAvg ? metrics.getAverageCpuLoad() : metrics.getCurrentCpuLoad()) / k;

        return load < 0 ? 0 : load;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AdaptiveCpuLoadProbe.class, this);
    }
}