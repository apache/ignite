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

package org.apache.ignite.spi.failover.always;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.failover.*;

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
 * Here is a Java example how to configure grid with {@link AlwaysFailoverSpi} failover SPI.
 * <pre name="code" class="java">
 * AlwaysFailoverSpi spi = new AlwaysFailoverSpi();
 *
 * // Override maximum failover attempts.
 * spi.setMaximumFailoverAttempts(5);
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default failover SPI.
 * cfg.setFailoverSpiSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * Here is an example of how to configure {@code AlwaysFailoverSpi} from Spring XML configuration file.
 * <pre name="code" class="xml">
 * &lt;property name="failoverSpi"&gt;
 *     &lt;bean class="org.apache.ignite.spi.failover.always.AlwaysFailoverSpi"&gt;
 *         &lt;property name="maximumFailoverAttempts" value="5"/&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.incubator.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see org.apache.ignite.spi.failover.FailoverSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
@IgniteSpiConsistencyChecked(optional = true)
public class AlwaysFailoverSpi extends IgniteSpiAdapter implements FailoverSpi, AlwaysFailoverSpiMBean {
    /** Maximum number of attempts to execute a failed job on another node (default is {@code 5}). */
    public static final int DFLT_MAX_FAILOVER_ATTEMPTS = 5;

    /**
     * Name of job context attribute containing all nodes a job failed on.
     *
     * @see org.apache.ignite.compute.ComputeJobContext
     */
    public static final String FAILED_NODE_LIST_ATTR = "gg:failover:failednodelist";

    /**
     * Name of job context attribute containing number of affinity call attempts.
     */
    public static final String AFFINITY_CALL_ATTEMPT = "ignite:failover:affinitycallattempt";

    /** Maximum attempts attribute key should be the same on all nodes. */
    public static final String MAX_FAILOVER_ATTEMPT_ATTR = "gg:failover:maxattempts";

    /** Injected grid logger. */
    @LoggerResource
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
    @IgniteSpiConfiguration(optional = true)
    public void setMaximumFailoverAttempts(int maxFailoverAttempts) {
        this.maxFailoverAttempts = maxFailoverAttempts;
    }

    /** {@inheritDoc} */
    @Override public int getTotalFailoverJobsCount() {
        return totalFailoverJobs;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws IgniteSpiException {
        return F.<String, Object>asMap(createSpiAttributeName(MAX_FAILOVER_ATTEMPT_ATTR), maxFailoverAttempts);
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        assertParameter(maxFailoverAttempts >= 0, "maxFailoverAttempts >= 0");

        if (log.isDebugEnabled())
            log.debug(configInfo("maximumFailoverAttempts", maxFailoverAttempts));

        registerMBean(gridName, this, AlwaysFailoverSpiMBean.class);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public ClusterNode failover(FailoverContext ctx, List<ClusterNode> top) {
        assert ctx != null;
        assert top != null;

        if (log.isDebugEnabled())
            log.debug("Received failed job result: " + ctx.getJobResult());

        if (top.isEmpty()) {
            U.warn(log, "Received empty topology for failover and is forced to fail.");

            // Nowhere to failover to.
            return null;
        }

        if (ctx.affinityKey() != null) {
            Integer affCallAttempt = ctx.getJobResult().getJobContext().getAttribute(AFFINITY_CALL_ATTEMPT);

            if (affCallAttempt == null)
                affCallAttempt = 1;

            if (maxFailoverAttempts <= affCallAttempt) {
                U.warn(log, "Job failover failed because number of maximum failover attempts for affinity call" +
                    " is exceeded [failedJob=" + ctx.getJobResult().getJob() + ", maxFailoverAttempts=" +
                    maxFailoverAttempts + ']');

                return null;
            }
            else {
                ctx.getJobResult().getJobContext().setAttribute(AFFINITY_CALL_ATTEMPT, affCallAttempt + 1);

                return ignite.affinity(ctx.affinityCacheName()).mapKeyToNode(ctx.affinityKey());
            }
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
        catch (IgniteException e) {
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
        return S.toString(AlwaysFailoverSpi.class, this);
    }
}
