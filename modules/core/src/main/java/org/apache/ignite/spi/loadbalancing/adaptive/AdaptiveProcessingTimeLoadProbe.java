/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.loadbalancing.adaptive;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Implementation of node load probing based on total job processing time.
 * Based on {@link #setUseAverage(boolean)}
 * parameter, this implementation will either use average job execution
 * time values or current (default is to use averages). The algorithm
 * returns a sum of job wait time and job execution time.
 * <p>
 * Below is an example of how CPU load probe would be configured in GridGain
 * Spring configuration file:
 * <pre name="code" class="xml">
 * &lt;property name="loadBalancingSpi"&gt;
 *     &lt;bean class="org.gridgain.grid.spi.loadBalancing.adaptive.GridAdaptiveLoadBalancingSpi"&gt;
 *         &lt;property name="loadProbe"&gt;
 *             &lt;bean class="org.gridgain.grid.spi.loadBalancing.adaptive.GridAdaptiveProcessingTimeLoadProbe"&gt;
 *                 &lt;property name="useAverage" value="true"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 */
public class AdaptiveProcessingTimeLoadProbe implements AdaptiveLoadProbe {
    /** Flag indicating whether to use average execution time vs. current. */
    private boolean useAvg = true;

    /**
     * Initializes execution time load probe to use
     * execution time average by default.
     */
    public AdaptiveProcessingTimeLoadProbe() {
        // No-op.
    }

    /**
     * Specifies whether to use average execution time vs. current.
     *
     * @param useAvg Flag indicating whether to use average execution time vs. current.
     */
    public AdaptiveProcessingTimeLoadProbe(boolean useAvg) {
        this.useAvg = useAvg;
    }

    /**
     * Gets flag indicating whether to use average execution time vs. current.
     *
     * @return Flag indicating whether to use average execution time vs. current.
     */
    public boolean isUseAverage() {
        return useAvg;
    }

    /**
     * Sets flag indicating whether to use average execution time vs. current.
     *
     * @param useAvg Flag indicating whether to use average execution time vs. current.
     */
    public void setUseAverage(boolean useAvg) {
        this.useAvg = useAvg;
    }


    /** {@inheritDoc} */
    @Override public double getLoad(ClusterNode node, int jobsSentSinceLastUpdate) {
        ClusterNodeMetrics metrics = node.metrics();

        if (useAvg) {
            double load = metrics.getAverageJobExecuteTime() + metrics.getAverageJobWaitTime();

            // If load is greater than 0, then we can use average times.
            // Otherwise, we will proceed to using current times.
            if (load > 0)
                return load;
        }

        double load = metrics.getCurrentJobExecuteTime() + metrics.getCurrentJobWaitTime();

        return load < 0 ? 0 : load;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AdaptiveProcessingTimeLoadProbe.class, this);
    }
}
