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

package org.apache.ignite.spi.loadbalancing.roundrobin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.loadbalancing.LoadBalancingSpi;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

/**
 * This SPI iterates through nodes in round-robin fashion and pick the next
 * sequential node. Two modes of operation are supported: per-task and global
 * (see {@link #setPerTask(boolean)} configuration). Global mode is used be default.
 * <p>
 * When configured in per-task mode, implementation will pick a random node at the
 * beginning of every task execution and then sequentially iterate through all
 * nodes in topology starting from the picked node. For cases when split size
 * is equal to the number of nodes, this mode guarantees that all nodes will
 * participate in the split.
 * <p>
 * When configured in global mode, a single sequential queue of nodes is maintained for
 * all tasks and the next node in the queue is picked every time. In this mode (unlike in
 * {@code per-task} mode) it is possible that even if split size may be equal to the
 * number of nodes, some jobs within the same task will be assigned to the same node if
 * multiple tasks are executing concurrently.
 * <h1 class="header">Coding Example</h1>
 * If you are using {@link org.apache.ignite.compute.ComputeTaskSplitAdapter} then load balancing logic
 * is transparent to your code and is handled automatically by the adapter.
 * Here is an example of how your task will look:
 * <pre name="code" class="java">
 * public class MyFooBarTask extends ComputeTaskSplitAdapter&lt;Object, Object&gt; {
 *    &#64;Override
 *    protected Collection&lt;? extends ComputeJob&gt; split(int gridSize, Object arg) throws IgniteCheckedException {
 *        List&lt;MyFooBarJob&gt; jobs = new ArrayList&lt;MyFooBarJob&gt;(gridSize);
 *
 *        for (int i = 0; i &lt; gridSize; i++) {
 *            jobs.add(new MyFooBarJob(arg));
 *        }
 *
 *        // Node assignment via load balancer
 *        // happens automatically.
 *        return jobs;
 *    }
 *    ...
 * }
 * </pre>
 * If you need more fine-grained control over how some jobs within task get mapped to a node
 * and use affinity load balancing for some other jobs within task, then you should use
 * {@link org.apache.ignite.compute.ComputeTaskAdapter}. Here is an example of how your task will look. Note that in this
 * case we manually inject load balancer and use it to pick the best node. Doing it in
 * such way would allow user to map some jobs manually and for others use load balancer.
 * <pre name="code" class="java">
 * public class MyFooBarTask extends ComputeTaskAdapter&lt;String, String&gt; {
 *    // Inject load balancer.
 *    &#64;LoadBalancerResource
 *    ComputeLoadBalancer balancer;
 *
 *    // Map jobs to grid nodes.
 *    public Map&lt;? extends ComputeJob, ClusterNode&gt; map(List&lt;ClusterNode&gt; subgrid, String arg) throws IgniteCheckedException {
 *        Map&lt;MyFooBarJob, ClusterNode&gt; jobs = new HashMap&lt;MyFooBarJob, ClusterNode&gt;(subgrid.size());
 *
 *        // In more complex cases, you can actually do
 *        // more complicated assignments of jobs to nodes.
 *        for (int i = 0; i &lt; subgrid.size(); i++) {
 *            // Pick the next best balanced node for the job.
 *            jobs.put(new MyFooBarJob(arg), balancer.getBalancedNode())
 *        }
 *
 *        return jobs;
 *    }
 *
 *    // Aggregate results into one compound result.
 *    public String reduce(List&lt;ComputeJobResult&gt; results) throws IgniteCheckedException {
 *        // For the purpose of this example we simply
 *        // concatenate string representation of every
 *        // job result
 *        StringBuilder buf = new StringBuilder();
 *
 *        for (ComputeJobResult res : results) {
 *            // Append string representation of result
 *            // returned by every job.
 *            buf.append(res.getData().string());
 *        }
 *
 *        return buf.string();
 *    }
 * }
 * </pre>
 * <p>
 * <h1 class="header">Configuration</h1>
 * In order to use this load balancer, you should configure your grid instance
 * to use {@link RoundRobinLoadBalancingSpi} either from Spring XML file or
 * directly. The following configuration parameters are supported:
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>
 *      Flag that indicates whether to use {@code per-task} or global
 *      round-robin modes described above (see {@link #setPerTask(boolean)}).
 * </li>
 * </ul>
 * Below is Java configuration example:
 * <pre name="code" class="java">
 * RoundRobinLoadBalancingSpi spi = new RoundRobinLoadBalancingSpi();
 *
 * // Configure SPI to use global round-robin mode.
 * spi.setPerTask(false);
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default load balancing SPI.
 * cfg.setLoadBalancingSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * Here is how you can configure {@link RoundRobinLoadBalancingSpi} using Spring XML configuration:
 * <pre name="code" class="xml">
 * &lt;property name="loadBalancingSpi"&gt;
 *     &lt;bean class="org.apache.ignite.spi.loadBalancing.roundrobin.RoundRobinLoadBalancingSpi"&gt;
 *         &lt;!-- Set to global round-robin mode. --&gt;
 *         &lt;property name="perTask" value="false"/&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
@IgniteSpiMultipleInstancesSupport(true)
public class RoundRobinLoadBalancingSpi extends IgniteSpiAdapter implements LoadBalancingSpi,
    RoundRobinLoadBalancingSpiMBean {
    /** Grid logger. */
    @LoggerResource
    private IgniteLogger log;

    /** */
    private RoundRobinGlobalLoadBalancer balancer;

    /** */
    private boolean isPerTask;

    /** */
    private final Map<IgniteUuid, RoundRobinPerTaskLoadBalancer> perTaskBalancers =
        new ConcurrentHashMap8<>();

    /** Event listener. */
    private final GridLocalEventListener lsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            if (evt.type() == EVT_TASK_FAILED ||
                evt.type() == EVT_TASK_FINISHED)
                perTaskBalancers.remove(((TaskEvent)evt).taskSessionId());
            else if (evt.type() == EVT_JOB_MAPPED) {
                RoundRobinPerTaskLoadBalancer balancer =
                    perTaskBalancers.get(((JobEvent)evt).taskSessionId());

                if (balancer != null)
                    balancer.onMapped();
            }
        }
    };

    /** {@inheritDoc} */
    @Override public boolean isPerTask() {
        return isPerTask;
    }

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
     * Default is {@code false}.
     *
     * @param isPerTask Configuration parameter indicating whether a new round robin order should
     *      be created for every task. Default is {@code false}.
     */
    @IgniteSpiConfiguration(optional = true)
    public void setPerTask(boolean isPerTask) {
        this.isPerTask = isPerTask;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        startStopwatch();

        if (log.isDebugEnabled())
            log.debug(configInfo("isPerTask", isPerTask));

        registerMBean(gridName, this, RoundRobinLoadBalancingSpiMBean.class);

        balancer = new RoundRobinGlobalLoadBalancer(log);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        balancer = null;

        perTaskBalancers.clear();

        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onContextInitialized0(IgniteSpiContext spiCtx) throws IgniteSpiException {
        if (!isPerTask)
            balancer.onContextInitialized(spiCtx);
        else {
            if (!getSpiContext().isEventRecordable(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED))
                throw new IgniteSpiException("Required event types are disabled: " +
                    U.gridEventName(EVT_TASK_FAILED) + ", " +
                    U.gridEventName(EVT_TASK_FINISHED) + ", " +
                    U.gridEventName(EVT_JOB_MAPPED));

            getSpiContext().addLocalEventListener(lsnr, EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);
        }
    }

    /** {@inheritDoc} */
    @Override protected void onContextDestroyed0() {
        if (!isPerTask) {
            if (balancer != null)
                balancer.onContextDestroyed();
        }
        else {
            IgniteSpiContext spiCtx = getSpiContext();

            if (spiCtx != null)
                spiCtx.removeLocalEventListener(lsnr);
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterNode getBalancedNode(ComputeTaskSession ses, List<ClusterNode> top, ComputeJob job) {
        A.notNull(ses, "ses", top, "top");

        if (isPerTask) {
            // Note that every session operates from single thread which
            // allows us to use concurrent map and avoid synchronization.
            RoundRobinPerTaskLoadBalancer taskBalancer = perTaskBalancers.get(ses.getId());

            if (taskBalancer == null)
                perTaskBalancers.put(ses.getId(), taskBalancer = new RoundRobinPerTaskLoadBalancer());

            return taskBalancer.getBalancedNode(top);
        }

        return balancer.getBalancedNode(top);
    }

    /**
     * THIS METHOD IS USED ONLY FOR TESTING.
     *
     * @param ses Task session.
     * @return Internal list of nodes.
     */
    List<UUID> getNodeIds(ComputeTaskSession ses) {
        if (isPerTask) {
            RoundRobinPerTaskLoadBalancer balancer = perTaskBalancers.get(ses.getId());

            if (balancer == null)
                return Collections.emptyList();

            List<UUID> ids = new ArrayList<>();

            for (ClusterNode node : balancer.getNodes()) {
                ids.add(node.id());
            }

            return ids;
        }

        return balancer.getNodeIds();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RoundRobinLoadBalancingSpi.class, this);
    }
}
