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

package org.apache.ignite.console.agent.handlers;

import io.socket.client.Socket;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CLUSTER_NAME;
import static org.apache.ignite.console.agent.AgentUtils.toJSON;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_CLIENT_MODE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IPS;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientResponse.STATUS_FAILED;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.sortAddresses;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.splitAddresses;

/**
 * API to transfer topology from Ignite cluster available by node-uri.
 */
public class ClusterListener implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(ClusterListener.class));

    /** */
    private static final IgniteProductVersion IGNITE_2_1 = IgniteProductVersion.fromString("2.1.0");

    /** */
    private static final IgniteProductVersion IGNITE_2_3 = IgniteProductVersion.fromString("2.3.0");

    /** Unique Visor key to get events last order. */
    private static final String EVT_LAST_ORDER_KEY = "WEB_AGENT_" + UUID.randomUUID().toString();

    /** Unique Visor key to get events throttle counter. */
    private static final String EVT_THROTTLE_CNTR_KEY = "WEB_AGENT_" + UUID.randomUUID().toString();

    /** */
    private static final String EVENT_CLUSTER_CONNECTED = "cluster:connected";

    /** */
    private static final String EVENT_CLUSTER_TOPOLOGY = "cluster:topology";

    /** */
    private static final String EVENT_CLUSTER_DISCONNECTED = "cluster:disconnected";

    /** Default timeout. */
    private static final long DFLT_TIMEOUT = 3000L;

    /** JSON object mapper. */
    private static final ObjectMapper MAPPER = new GridJettyObjectMapper();

    /** Latest topology snapshot. */
    private TopologySnapshot top;

    /** */
    private final WatchTask watchTask = new WatchTask();

    /** */
    private static final IgniteClosure<UUID, String> ID2ID8 = new IgniteClosure<UUID, String>() {
        @Override public String apply(UUID nid) {
            return U.id8(nid).toUpperCase();
        }

        @Override public String toString() {
            return "Node ID to ID8 transformer closure.";
        }
    };

    /** */
    private AgentConfiguration cfg;

    /** */
    private Socket client;

    /** */
    private RestExecutor restExecutor;

    /** */
    private static final ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

    /** */
    private ScheduledFuture<?> refreshTask;

    /**
     * @param client Client.
     * @param restExecutor Client.
     */
    public ClusterListener(AgentConfiguration cfg, Socket client, RestExecutor restExecutor) {
        this.cfg = cfg;
        this.client = client;
        this.restExecutor = restExecutor;
    }

    /**
     * Callback on cluster connect.
     *
     * @param nids Cluster nodes IDs.
     */
    private void clusterConnect(Collection<UUID> nids) {
        log.info("Connection successfully established to cluster with nodes: " + F.viewReadOnly(nids, ID2ID8));

        client.emit(EVENT_CLUSTER_CONNECTED, toJSON(nids));
    }

    /**
     * Callback on disconnect from cluster.
     */
    private void clusterDisconnect() {
        if (top == null)
            return;

        top = null;

        log.info("Connection to cluster was lost");

        client.emit(EVENT_CLUSTER_DISCONNECTED);
    }

    /**
     * Stop refresh task.
     */
    private void safeStopRefresh() {
        if (refreshTask != null)
            refreshTask.cancel(true);
    }

    /**
     * Start watch cluster.
     */
    public void watch() {
        safeStopRefresh();

        refreshTask = pool.scheduleWithFixedDelay(watchTask, 0L, DFLT_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        refreshTask.cancel(true);

        pool.shutdownNow();
    }

    /** */
    private static class TopologySnapshot {
        /** */
        private String clusterName;

        /** */
        private Collection<UUID> nids;

        /** */
        private Map<UUID, String> addrs;

        /** */
        private Map<UUID, Boolean> clients;

        /** */
        private String clusterVerStr;

        /** */
        private IgniteProductVersion clusterVer;

        /** */
        private boolean active;

        /** */
        private boolean secured;

        /**
         * Helper method to get attribute.
         *
         * @param attrs Map with attributes.
         * @param name Attribute name.
         * @return Attribute value.
         */
        private static <T> T attribute(Map<String, Object> attrs, String name) {
            return (T)attrs.get(name);
        }

        /**
         * @param nodes Nodes.
         */
        TopologySnapshot(Collection<GridClientNodeBean> nodes) {
            int sz = nodes.size();

            nids = new ArrayList<>(sz);
            addrs = U.newHashMap(sz);
            clients = U.newHashMap(sz);
            active = false;
            secured = false;

            for (GridClientNodeBean node : nodes) {
                UUID nid = node.getNodeId();

                nids.add(nid);

                Map<String, Object> attrs = node.getAttributes();

                if (F.isEmpty(clusterName))
                    clusterName = attribute(attrs, IGNITE_CLUSTER_NAME);

                Boolean client = attribute(attrs, ATTR_CLIENT_MODE);

                clients.put(nid, client);

                Collection<String> nodeAddrs = client
                    ? splitAddresses(attribute(attrs, ATTR_IPS))
                    : node.getTcpAddresses();

                String firstIP = F.first(sortAddresses(nodeAddrs));

                addrs.put(nid, firstIP);

                String nodeVerStr = attribute(attrs, ATTR_BUILD_VER);

                IgniteProductVersion nodeVer = IgniteProductVersion.fromString(nodeVerStr);

                if (clusterVer == null || clusterVer.compareTo(nodeVer) > 0) {
                    clusterVer = nodeVer;
                    clusterVerStr = nodeVerStr;
                }
            }
        }

        /**
         * @return Cluster name.
         */
        public String getClusterName() {
            return clusterName;
        }

        /**
         * @return Cluster version.
         */
        public String getClusterVersion() {
            return clusterVerStr;
        }

        /**
         * @return Cluster active flag.
         */
        public boolean isActive() {
            return active;
        }

        /**
         * @param active New cluster active state.
         */
        public void setActive(boolean active) {
            this.active = active;
        }

        /**
         * @return {@code true} If cluster has configured security.
         */
        public boolean isSecured() {
            return secured;
        }

        /**
         * @param secured Configured security flag.
         */
        public void setSecured(boolean secured) {
            this.secured = secured;
        }

        /**
         * @return Cluster nodes IDs.
         */
        public Collection<UUID> getNids() {
            return nids;
        }

        /**
         * @return Cluster nodes with IPs.
         */
        public Map<UUID, String> getAddresses() {
            return addrs;
        }

        /**
         * @return Cluster nodes with client mode flag.
         */
        public Map<UUID, Boolean> getClients() {
            return clients;
        }

        /**
         * @return Cluster version.
         */
        public IgniteProductVersion clusterVersion() {
            return clusterVer;
        }

        /**
         * @return Collection of short UUIDs.
         */
        Collection<String> nid8() {
            return F.viewReadOnly(nids, ID2ID8);
        }

        /**
         * @param prev Previous topology.
         * @return {@code true} in case if current topology is a new cluster.
         */
        boolean differentCluster(TopologySnapshot prev) {
            return prev == null || F.isEmpty(prev.nids) || Collections.disjoint(nids, prev.nids);
        }
    }

    /** */
    private class WatchTask implements Runnable {
        /** */
        private static final String EXPIRED_SES_ERROR_MSG = "Failed to handle request - unknown session token (maybe expired session)";

        /** */
        private String sesTok;

        /**
         * Execute REST command under agent user.
         *
         * @param params Command params.
         * @return Command result.
         * @throws IOException If failed to execute.
         */
        private RestResult restCommand(Map<String, Object> params) throws IOException {
            if (!F.isEmpty(sesTok))
                params.put("sessionToken", sesTok);
            else if (!F.isEmpty(cfg.nodeLogin()) && !F.isEmpty(cfg.nodePassword())) {
                params.put("user", cfg.nodeLogin());
                params.put("password", cfg.nodePassword());
            }

            RestResult res = restExecutor.sendRequest(cfg.nodeURIs(), params, null);

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
         * @param full Full.
         */
        private RestResult topology(boolean full) throws IOException {
            Map<String, Object> params = U.newHashMap(3);

            params.put("cmd", "top");
            params.put("attr", true);
            params.put("mtr", full);

            return restCommand(params);
        }

        /**
         * @param ver Cluster version.
         * @param nid Node ID.
         * @return Cluster active state.
         * @throws IOException If failed to collect cluster active state.
         */
        public boolean active(IgniteProductVersion ver, UUID nid) throws IOException {
            Map<String, Object> params = U.newHashMap(10);

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

            switch (res.getStatus()) {
                case STATUS_SUCCESS:
                    if (v23)
                        return Boolean.valueOf(res.getData());

                    return res.getData().contains("\"active\":true");

                default:
                    throw new IOException(res.getError());
            }
        }


        /** {@inheritDoc} */
        @Override public void run() {
            try {
                RestResult res = topology(false);

                switch (res.getStatus()) {
                    case STATUS_SUCCESS:
                        List<GridClientNodeBean> nodes = MAPPER.readValue(res.getData(),
                            new TypeReference<List<GridClientNodeBean>>() {});

                        TopologySnapshot newTop = new TopologySnapshot(nodes);

                        if (newTop.differentCluster(top))
                            log.info("Connection successfully established to cluster with nodes: " + newTop.nid8());

                        boolean active = active(newTop.clusterVersion(), F.first(newTop.getNids()));

                        newTop.setActive(active);
                        newTop.setSecured(!F.isEmpty(res.getSessionToken()));

                        top = newTop;

                        client.emit(EVENT_CLUSTER_TOPOLOGY, toJSON(top));

                        break;

                    default:
                        LT.warn(log, res.getError());

                        clusterDisconnect();
                }
            }
            catch (ConnectException ignored) {
                clusterDisconnect();
            }
            catch (Exception e) {
                log.error("WatchTask failed", e);

                clusterDisconnect();
            }
        }
    }
}
