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

package org.apache.ignite.agent.processor;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.agent.dto.cluster.BaselineInfo;
import org.apache.ignite.agent.dto.cluster.ClusterInfo;
import org.apache.ignite.agent.dto.topology.TopologySnapshot;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterDest;
import static org.apache.ignite.agent.StompDestinationsUtils.buildClusterTopologyDest;
import static org.apache.ignite.agent.utils.AgentUtils.fromNullableCollection;
import static org.apache.ignite.agent.utils.AgentUtils.getClusterFeatures;
import static org.apache.ignite.events.EventType.EVTS_CLUSTER_ACTIVATION;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Cluster processor.
 */
public class ClusterInfoProcessor extends GridProcessorAdapter {
    /** Discovery event on restart agent. */
    private static final int[] EVTS_DISCOVERY = new int[] {EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT};

    /** TODO GG-21449: this code emulates EVT_BASELINE_CHANGED */
    private volatile Set<String> curBaseline;

    /** Baseline parameters. */
    private volatile BaselineInfo curBaselineParameters;

    /** Cluster. */
    private IgniteClusterEx cluster;

    /** Manager. */
    private WebSocketManager mgr;

    /** Executor service. */
    private ScheduledExecutorService baselineExecSrvc =
        Executors.newScheduledThreadPool(1, new CustomizableThreadFactory("mgmt-console-baseline-watcher-"));

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public ClusterInfoProcessor(GridKernalContext ctx, WebSocketManager mgr) {
        super(ctx);
        this.mgr = mgr;
        cluster = ctx.grid().cluster();

        GridEventStorageManager evtMgr = ctx.event();

        // Listener for topology changes.
        evtMgr.addDiscoveryEventListener(this::sendTopologyUpdate, EVTS_DISCOVERY);

        // Listen for activation/deactivation.
        evtMgr.enableEvents(EVTS_CLUSTER_ACTIVATION);
        evtMgr.addLocalEventListener(this::sendClusterInfo, EVTS_CLUSTER_ACTIVATION);

        // TODO GG-21449: this code emulates EVT_BASELINE_CHANGED and EVT_BASELINE_AUTO_*
        baselineExecSrvc.scheduleWithFixedDelay(() -> {
            Stream<BaselineNode> stream = fromNullableCollection(ctx.grid().cluster().currentBaselineTopology());

            Set<String> baseline = stream
                .map(BaselineNode::consistentId)
                .map(Object::toString)
                .collect(Collectors.toSet());

            if (curBaseline == null)
                curBaseline = baseline;
            else if (!curBaseline.equals(baseline)) {
                curBaseline = baseline;

                sendTopologyUpdate(null, ctx.discovery().discoCache());
            }

            BaselineInfo baselineParameters = new BaselineInfo(
                cluster.isBaselineAutoAdjustEnabled(),
                cluster.baselineAutoAdjustTimeout()
            );

            if (curBaselineParameters == null)
                curBaselineParameters = baselineParameters;
            else if (!curBaselineParameters.equals(baselineParameters)) {
                curBaselineParameters = baselineParameters;

                sendClusterInfo(null);
            }
        }, 2, 5, TimeUnit.SECONDS);
    }

    /**
     * Send initial states.
     */
    public void sendInitialState() {
        sendClusterInfo(null);
        sendTopologyUpdate(null, ctx.discovery().discoCache());
    }

    /**
     * Send full topology to Management Console.
     */
    void sendTopologyUpdate(DiscoveryEvent evt, DiscoCache discoCache) {
        if (log.isDebugEnabled())
            log.debug("Sending full topology to Management Console");

        Object crdId = cluster.localNode().consistentId();

        mgr.send(
            buildClusterTopologyDest(cluster.id()),
            TopologySnapshot.topology(cluster.topologyVersion(), crdId, cluster.nodes(), cluster.currentBaselineTopology())
        );

        if (evt != null)
            sendClusterInfo(null);
    }

    /**
     * Send cluster info.
     */
    void sendClusterInfo(Event evt) {
        if (log.isDebugEnabled())
            log.debug("Sending cluster info to Management Console");

        ClusterInfo clusterInfo = new ClusterInfo(cluster.id(), cluster.tag())
            .setActive(cluster.active())
            .setPersistenceEnabled(CU.isPersistenceEnabled(ctx.config()))
            .setBaselineParameters(
                new BaselineInfo(
                    cluster.isBaselineAutoAdjustEnabled(),
                    cluster.baselineAutoAdjustTimeout()
                )
            )
            .setFeatures(getClusterFeatures(ctx, ctx.cluster().get().nodes()));

        mgr.send(buildClusterDest(cluster.id()), clusterInfo);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        ctx.event().removeDiscoveryEventListener(this::sendTopologyUpdate, EVTS_DISCOVERY);

        ctx.event().removeLocalEventListener(this::sendClusterInfo, EVTS_CLUSTER_ACTIVATION);

        U.shutdownNow(getClass(), baselineExecSrvc, log);
    }
}
