/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.always;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Failover SPI that always reroutes a failed job to another node.
 * Note, that at first an attempt will be made to reroute the failed job
 * to a node that was not part of initial split for a better chance of
 * success. If no such nodes are available, then an attempt will be made to
 * reroute the failed job to the nodes in the initial split minus the node
 * the job is failed on. If none of the above attempts succeeded, then the
 * job will not be failed over and {@code null} will be returned.
 * <p>
 * <h1 class="header">Configuration</h1>
 * This SPI is default failover SPI and does not have to be explicitly
 * configured unless configuration parameters need to be changed.
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has following optional configuration parameters:
 * <ul>
 *      <li>
 *          Maximum failover attempts for a single job (see {@link #setMaximumFailoverAttempts(int)}).
 *          If maximum failover attempts is reached, then job will not be failed-over and,
 *          hence, will fail.
 *      </li>
 * </ul>
 * Here is a Java example how to configure grid with {@code GridAlwaysFailoverSpi} failover SPI.
 * <pre name="code" class="java">
 * GridAlwaysFailoverSpi spi = new GridAlwaysFailoverSpi();
 *
 * // Override maximum failover attempts.
 * spi.setMaximumFailoverAttempts(5);
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default failover SPI.
 * cfg.setFailoverSpiSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * Here is an example of how to configure {@code GridAlwaysFailoverSpi} from Spring XML configuration file.
 * <pre name="code" class="xml">
 * &lt;property name="failoverSpi"&gt;
 *     &lt;bean class="org.gridgain.grid.spi.failover.always.GridAlwaysFailoverSpi"&gt;
 *         &lt;property name="maximumFailoverAttempts" value="5"/&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see GridFailoverSpi
 */
@GridSpiMultipleInstancesSupport(true)
@GridSpiConsistencyChecked(optional = true)
public class GridAlwaysFailoverSpi extends IgniteSpiAdapter implements GridFailoverSpi, GridAlwaysFailoverSpiMBean {
    /** Maximum number of attempts to execute a failed job on another node (default is {@code 5}). */
    public static final int DFLT_MAX_FAILOVER_ATTEMPTS = 5;

    /**
     * Name of job context attribute containing all nodes a job failed on.
     *
     * @see org.apache.ignite.compute.ComputeJobContext
     */
    public static final String FAILED_NODE_LIST_ATTR = "gg:failover:failednodelist";

    /** Maximum attempts attribute key should be the same on all nodes. */
    public static final String MAX_FAILOVER_ATTEMPT_ATTR = "gg:failover:maxattempts";

    /** Injected grid logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** Maximum number of attempts to execute a failed job on another node. */
    private int maxFailoverAttempts = DFLT_MAX_FAILOVER_ATTEMPTS;

    /** Number of jobs that were failed over. */
    private int totalFailoverJobs;

    /** {@inheritDoc} */
    @Override public int getMaximumFailoverAttempts() {
        return maxFailoverAttempts;
    }

    /**
     * Sets maximum number of attempts to execute a failed job on another node.
     * If not specified, {@link #DFLT_MAX_FAILOVER_ATTEMPTS} value will be used.
     *
     * @param maxFailoverAttempts Maximum number of attempts to execute a failed job on another node.
     */
    @GridSpiConfiguration(optional = true)
    public void setMaximumFailoverAttempts(int maxFailoverAttempts) {
        this.maxFailoverAttempts = maxFailoverAttempts;
    }

    /** {@inheritDoc} */
    @Override public int getTotalFailoverJobsCount() {
        return totalFailoverJobs;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws GridSpiException {
        return F.<String, Object>asMap(createSpiAttributeName(MAX_FAILOVER_ATTEMPT_ATTR), maxFailoverAttempts);
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws GridSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        assertParameter(maxFailoverAttempts >= 0, "maxFailoverAttempts >= 0");

        if (log.isDebugEnabled())
            log.debug(configInfo("maximumFailoverAttempts", maxFailoverAttempts));

        registerMBean(gridName, this, GridAlwaysFailoverSpiMBean.class);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public ClusterNode failover(GridFailoverContext ctx, List<ClusterNode> top) {
        assert ctx != null;
        assert top != null;

        if (log.isDebugEnabled())
            log.debug("Received failed job result: " + ctx.getJobResult());

        if (top.isEmpty()) {
            U.warn(log, "Received empty topology for failover and is forced to fail.");

            // Nowhere to failover to.
            return null;
        }

        Collection<UUID> failedNodes = ctx.getJobResult().getJobContext().getAttribute(FAILED_NODE_LIST_ATTR);

        if (failedNodes == null)
            failedNodes = U.newHashSet(1);

        Integer failoverCnt = failedNodes.size();

        if (failoverCnt >= maxFailoverAttempts) {
            U.warn(log, "Job failover failed because number of maximum failover attempts is exceeded [failedJob=" +
                ctx.getJobResult().getJob() + ", maxFailoverAttempts=" + maxFailoverAttempts + ']');

            return null;
        }

        failedNodes.add(ctx.getJobResult().getNode().id());

        // Copy.
        List<ClusterNode> newTop = new ArrayList<>(top.size());

        for (ClusterNode node : top)
            if (!failedNodes.contains(node.id()))
                newTop.add(node);

        if (newTop.isEmpty()) {
            U.warn(log, "Received topology with only nodes that job had failed on (forced to fail) [failedNodes=" +
                failedNodes + ']');

            // Nowhere to failover to.
            return null;
        }

        try {
            ClusterNode node = ctx.getBalancedNode(newTop);

            if (node == null)
                U.warn(log, "Load balancer returned null node for topology: " + newTop);
            else {
                // Increment failover count.
                ctx.getJobResult().getJobContext().setAttribute(FAILED_NODE_LIST_ATTR, failedNodes);

                totalFailoverJobs++;
            }

            if (node != null)
                U.warn(log, "Failed over job to a new node [newNode=" + node.id() +
                    ", oldNode=" + ctx.getJobResult().getNode().id() +
                    ", sesId=" + ctx.getTaskSession().getId() +
                    ", job=" + ctx.getJobResult().getJob() +
                    ", jobCtx=" + ctx.getJobResult().getJobContext() +
                    ", task=" + ctx.getTaskSession().getTaskName() + ']');

            return node;
        }
        catch (GridException e) {
            U.error(log, "Failed to get next balanced node for failover: " + ctx, e);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override protected List<String> getConsistentAttributeNames() {
        return Collections.singletonList(createSpiAttributeName(MAX_FAILOVER_ATTEMPT_ATTR));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAlwaysFailoverSpi.class, this);
    }
}
