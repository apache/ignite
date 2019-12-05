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

package org.apache.ignite.agent.processor.metrics;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.agent.dto.metric.MetricRequest;
import org.apache.ignite.agent.dto.metric.MetricResponse;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;

import static org.apache.ignite.agent.StompDestinationsUtils.buildMetricsDest;
import static org.apache.ignite.internal.GridTopic.TOPIC_METRICS;
import static org.apache.ignite.internal.IgniteFeatures.MANAGEMENT_CONSOLE;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Metric processor.
 */
public class MetricsProcessor extends GridProcessorAdapter {
    /** Manager. */
    private WebSocketManager mgr;

    /** Listener. */
    private final GridMessageListener lsnr = this::processMetricResponse;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public MetricsProcessor(GridKernalContext ctx, WebSocketManager mgr) {
        super(ctx);

        this.mgr = mgr;

        // Listener for collecting metrics event.
        ctx.io().addMessageListener(TOPIC_METRICS, lsnr);
    }

    /**
     * Process node metrics message.
     *
     * @param nodeId ID of node that sent the message. Note that may have already
     *      left topology by the time this message is received.
     * @param msg Message received.
     * @param plc Message policy (pool).
     */
    private void processMetricResponse(UUID nodeId, Object msg, byte plc) {
        if (msg instanceof MetricResponse) {
            MetricResponse res = (MetricResponse)msg;

            if (log.isDebugEnabled())
                log.debug("Send message to Management Console: " + msg);

            try {
                mgr.send(buildMetricsDest(), res.body());
            }
            catch (Throwable e) {
                log.error("Failed to send metrics to Management Console", e);
            }
        }
    }

    /**
     * Pull metrics from cluster.
     */
    public void broadcastPullMetrics() {
        Collection<ClusterNode> nodes = ctx.grid().cluster()
            .forServers()
            .forPredicate(n -> nodeSupports(ctx, n, MANAGEMENT_CONSOLE))
            .nodes();

        try {
            if (log.isDebugEnabled())
                log.debug("Broadcasting pull metrics request");

            MetricRequest req = new MetricRequest(-1);

            ctx.io().sendToGridTopic(nodes, TOPIC_METRICS, req, SYSTEM_POOL);
        }
        catch (Throwable e) {
            log.error("Failed to broadcast pull metrics request", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        ctx.io().removeMessageListener(TOPIC_METRICS, lsnr);
    }
}
