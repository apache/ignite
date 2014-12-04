/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.adaptive;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Implementation of node load probing based on active and waiting job count.
 * Based on {@link #setUseAverage(boolean)} parameter, this implementation will
 * either use average job count values or current (default is to use averages).
 * <p>
 * The load of a node is simply calculated by adding active and waiting job counts.
 * <p>
 * Below is an example of how CPU load probe would be configured in GridGain
 * Spring configuration file:
 * <pre name="code" class="xml">
 * &lt;property name="loadBalancingSpi"&gt;
 *     &lt;bean class="org.gridgain.grid.spi.loadBalancing.adaptive.GridAdaptiveLoadBalancingSpi"&gt;
 *         &lt;property name="loadProbe"&gt;
 *             &lt;bean class="org.gridgain.grid.spi.loadBalancing.adaptive.GridAdaptiveJobCountLoadProbe"&gt;
 *                 &lt;property name="useAverage" value="true"/&gt;
 *             &lt;/bean&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 */
public class GridAdaptiveJobCountLoadProbe implements GridAdaptiveLoadProbe {
    /** Flag indicating whether to use average CPU load vs. current. */
    private boolean useAvg = true;

    /**
     * Initializes active job probe.
     */
    public GridAdaptiveJobCountLoadProbe() {
        // No-op.
    }

    /**
     * Creates new active job prove specifying whether to use average
     * job counts vs. current.
     *
     * @param useAvg Flag indicating whether to use average job counts vs. current.
     */
    public GridAdaptiveJobCountLoadProbe(boolean useAvg) {
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
        GridNodeMetrics metrics = node.metrics();

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
        return S.toString(GridAdaptiveJobCountLoadProbe.class, this);
    }
}
