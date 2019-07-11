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

package org.apache.ignite.console.agent.handlers;

import java.io.Closeable;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.console.agent.AgentUtils.nid8;
import static org.apache.ignite.console.agent.AgentUtils.send;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.CLUSTER_TOPOLOGY;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientResponse.STATUS_FAILED;
import static org.apache.ignite.lang.IgniteProductVersion.fromString;

/**
 * API to transfer topology from Ignite cluster to Web Console.
 */
public class ClustersWatcher implements Closeable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(ClustersWatcher.class));

    /** */
    private static final IgniteProductVersion IGNITE_2_0 = fromString("2.0.0");

    /** */
    private static final IgniteProductVersion IGNITE_2_1 = fromString("2.1.0");

    /** */
    private static final IgniteProductVersion IGNITE_2_3 = fromString("2.3.0");

    /** Unique Visor key to get events last order. */
    private static final String EVT_LAST_ORDER_KEY = "WEB_AGENT_" + UUID.randomUUID().toString();

    /** Unique Visor key to get events throttle counter. */
    private static final String EVT_THROTTLE_CNTR_KEY = "WEB_AGENT_" + UUID.randomUUID().toString();

    /** Topology refresh frequency. */
    private static final long REFRESH_FREQ = 3000L;

    /** */
    private static final String EXPIRED_SES_ERROR_MSG = "Failed to handle request - unknown session token (maybe expired session)";

    /** Agent configuration. */
    private AgentConfiguration cfg;

    /** Cluster handler. */
    private ClusterHandler clusterHnd;
    
    /** Demo cluster handler. */
    private DemoClusterHandler demoClusterHnd;

    /** Latest topology snapshot. */
    private TopologySnapshot latestTop;

    /** Executor pool. */
    private static final ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

    /** Refresh task. */
    private ScheduledFuture<?> refreshTask;

    /** Session token. */
    private String sesTok;

    /**
     * @param cfg Agent configuration.
     * @param clusterHnd Cluster handler.
     */
    ClustersWatcher(AgentConfiguration cfg, ClusterHandler clusterHnd, DemoClusterHandler demoClusterHnd) {
        this.cfg = cfg;
        this.clusterHnd = clusterHnd;
        this.demoClusterHnd = demoClusterHnd;
    }

    /**
     * Send event to websocket.
     *
     * @param ses Websocket session.
     * @param tops Topologies.
     */
    private void sendTopology(Session ses, List<TopologySnapshot> tops) {
        try {
            send(ses, new WebSocketResponse(CLUSTER_TOPOLOGY, tops), 10L, TimeUnit.SECONDS);
        }
        catch (Throwable e) {
            log.error("Failed to send topology to server");
        }
    }

    /**
     * Start watch cluster.
     */
    void startWatchTask(Session ses) {
        if (refreshTask != null && !refreshTask.isCancelled()) {
            log.warning("Detected that watch task already running");

            refreshTask.cancel(true);
        }

        refreshTask = pool.scheduleWithFixedDelay(() -> {
           List<TopologySnapshot> tops = new ArrayList<>(F.asList(demoClusterHnd.topologySnapshot()));

            try {
                RestResult res = topology();

                if (res.getStatus() != STATUS_SUCCESS)
                    throw new IllegalStateException(res.getError());

                List<GridClientNodeBean> nodes = fromJson(
                    res.getData(),
                    new TypeReference<List<GridClientNodeBean>>() {}
                );

                TopologySnapshot newTop = new TopologySnapshot(nodes);

                if (newTop.differentCluster(latestTop))
                    log.info("Connection successfully established to cluster with nodes: " + nid8(newTop.nids()));
                else if (!Objects.equals(latestTop.nids(), newTop.nids()))
                    log.info("Cluster topology changed, new topology: " + nid8(newTop.nids()));

                boolean active = active(fromString(newTop.getClusterVersion()), F.first(newTop.nids()));

                newTop.setDemo(false);
                newTop.setActive(active);
                newTop.setSecured(!F.isEmpty(res.getSessionToken()));

                latestTop = newTop;

                tops.add(newTop);

                sendTopology(ses, tops);
            }
            catch (Throwable e) {
                onFailedClusterRequest(e);

                latestTop = null;

                sendTopology(ses, tops);
            }
        }, 0L, REFRESH_FREQ, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop cluster watch.
     */
    void stop() {
        if (refreshTask != null) {
            refreshTask.cancel(true);

            refreshTask = null;

            log.info("Topology watch process was suspended");
        }
    }

    /**
     * Execute REST command under agent user.
     *
     * @param params Command params.
     * @return Command result.
     * @throws Exception If failed to execute.
     */
    public RestResult restCommand(JsonObject params) throws Throwable {
        if (!F.isEmpty(sesTok))
            params.put("sessionToken", sesTok);
        else if (!F.isEmpty(cfg.nodeLogin()) && !F.isEmpty(cfg.nodePassword())) {
            params.put("user", cfg.nodeLogin());
            params.put("password", cfg.nodePassword());
        }

        RestResult res = clusterHnd.restCommand(params);

        switch (res.getStatus()) {
            case STATUS_SUCCESS:
                sesTok = res.getSessionToken();

                return res;

            case STATUS_FAILED:
                if (res.getError().startsWith(EXPIRED_SES_ERROR_MSG)) {
                    sesTok = null;

                    params.remove("sessionToken");

                    return restCommand(params);
                }

            default:
                return res;
        }
    }

    /**
     * Collect topology.
     *
     * @return REST result.
     * @throws Exception If failed to collect cluster topology.
     */
    private RestResult topology() throws Throwable {
        JsonObject params = new JsonObject()
            .add("cmd", "top")
            .add("attr", true)
            .add("mtr", false)
            .add("caches", false);

        return restCommand(params);
    }

    /**
     * @param ver Cluster version.
     * @param nid Node ID.
     * @return Cluster active state.
     * @throws Exception If failed to collect cluster active state.
     */
    private boolean active(IgniteProductVersion ver, UUID nid) throws Throwable {
        // 1.x clusters are always active.
        if (ver.compareTo(IGNITE_2_0) < 0)
            return true;

        JsonObject params = new JsonObject();

        boolean v23 = ver.compareTo(IGNITE_2_3) >= 0;

        if (v23)
            params.put("cmd", "currentState");
        else {
            params.put("cmd", "exe");
            params.put("name", "org.apache.ignite.internal.visor.compute.VisorGatewayTask");
            params.put("p1", nid);
            params.put("p2", "org.apache.ignite.internal.visor.node.VisorNodeDataCollectorTask");
            params.put("p3", "org.apache.ignite.internal.visor.node.VisorNodeDataCollectorTaskArg");
            params.put("p4", false);
            params.put("p5", EVT_LAST_ORDER_KEY);
            params.put("p6", EVT_THROTTLE_CNTR_KEY);

            if (ver.compareTo(IGNITE_2_1) >= 0)
                params.put("p7", false);
            else {
                params.put("p7", 10);
                params.put("p8", false);
            }
        }

        RestResult res = restCommand(params);

        if (res.getStatus() == STATUS_SUCCESS)
            return v23 ? Boolean.valueOf(res.getData()) : res.getData().contains("\"active\":true");

        return false;
    }

    /**
     * Callback on disconnect from cluster.
     */
    private void onFailedClusterRequest(Throwable e) {
        String msg = latestTop == null ? "Failed to establish connection to node" : "Connection to cluster was lost";

        boolean failed = X.hasCause(e, IllegalStateException.class);

        if (X.hasCause(e, ConnectException.class))
            LT.info(log, msg);
        else if (failed && "Failed to handle request - session token not found or invalid".equals(e.getMessage()))
            LT.error(log, null, "Failed to establish connection to secured cluster - missing credentials. Please pass '--node-login' and '--node-password' options");
        else if (failed && e.getMessage().startsWith("Failed to authenticate remote client (invalid credentials?):"))
            LT.error(log, null, "Failed to establish connection to secured cluster - invalid credentials. Please check '--node-login' and '--node-password' options");
        else if (failed)
            LT.error(log, null, msg + ". " + e.getMessage());
        else
            LT.error(log, e, msg);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (refreshTask != null)
            refreshTask.cancel(true);

        pool.shutdownNow();
    }
}
