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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.socket.client.Socket;
import io.socket.emitter.Emitter;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.console.agent.AgentUtils.toJSON;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * API to retranslate topology from Ignite cluster available by node-uri.
 */
public class ClusterListener {
    /** */
    private static final Logger log = LoggerFactory.getLogger(ClusterListener.class);

    /** */
    private static final String EVENT_CLUSTER_CONNECTED = "cluster:connected";

    /** */
    private static final String EVENT_CLUSTER_TOPOLOGY = "cluster:topology";
    
    /** */
    private static final String EVENT_CLUSTER_DISCONNECTED = "cluster:disconnected";

    /** Default timeout. */
    private static final long DFLT_TIMEOUT = 3000L;

    /** JSON object mapper. */
    private static final ObjectMapper mapper = new GridJettyObjectMapper();

    /** Latest topology snapshot. */
    private TopologySnapshot top;

    /** */
    private final WatchTask watchTask = new WatchTask();

    /** */
    private final BroadcastTask broadcastTask = new BroadcastTask();

    /** */
    private static final IgniteClosure<GridClientNodeBean, UUID> NODE2ID = new IgniteClosure<GridClientNodeBean, UUID>() {
        @Override public UUID apply(GridClientNodeBean n) {
            return n.getNodeId();
        }

        @Override public String toString() {
            return "Node bean to node ID transformer closure.";
        }
    };

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
    private static final ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

    /** */
    private ScheduledFuture<?> refreshTask;

    /** */
    private Socket client;

    /** */
    private RestExecutor restExecutor;

    /**
     * @param client Client.
     * @param restExecutor Client.
     */
    public ClusterListener(Socket client, RestExecutor restExecutor) {
        this.client = client;
        this.restExecutor = restExecutor;
    }

    /**
     * Callback on cluster connect.
     *
     * @param nids Cluster nodes IDs.
     */
    private void clusterConnect(Collection<UUID> nids) {
        log.info("Connection successfully established to cluster with nodes: {}", F.viewReadOnly(nids, ID2ID8));

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

    /**
     * Start broadcast topology to server-side.
     */
    public Emitter.Listener start() {
        return new Emitter.Listener() {
            @Override public void call(Object... args) {
                safeStopRefresh();

                final long timeout = args.length > 1  && args[1] instanceof Long ? (long)args[1] : DFLT_TIMEOUT;

                refreshTask = pool.scheduleWithFixedDelay(broadcastTask, 0L, timeout, TimeUnit.MILLISECONDS);
            }
        };
    }

    /**
     * Stop broadcast topology to server-side.
     */
    public Emitter.Listener stop() {
        return new Emitter.Listener() {
            @Override public void call(Object... args) {
                refreshTask.cancel(true);

                watch();
            }
        };
    }

    /** */
    private class TopologySnapshot {
        /** */
        private Collection<UUID> nids;

        /** */
        private String clusterVersion;

        /**
         * @param nodes Nodes.
         */
        TopologySnapshot(Collection<GridClientNodeBean> nodes) {
            nids = F.viewReadOnly(nodes, NODE2ID);

            Collection<IgniteProductVersion> vers = F.transform(nodes,
                new IgniteClosure<GridClientNodeBean, IgniteProductVersion>() {
                    @Override public IgniteProductVersion apply(GridClientNodeBean bean) {
                        return IgniteProductVersion.fromString((String)bean.getAttributes().get(ATTR_BUILD_VER));
                    }
                });

            clusterVersion = Collections.min(vers).toString();
        }

        /**  */
        Collection<String> nid8() {
            return F.viewReadOnly(nids, ID2ID8);
        }

        /**  */
        boolean isSameCluster(TopologySnapshot snapshot) {
            if (snapshot == null || F.isEmpty(snapshot.nids))
                return false;

            return Collections.disjoint(nids, snapshot.nids);
        }
    }

    /** */
    private class WatchTask implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            try {
                RestResult res = restExecutor.topology(false, false);

                switch (res.getStatus()) {
                    case STATUS_SUCCESS:
                        List<GridClientNodeBean> nodes = mapper.readValue(res.getData(),
                            new TypeReference<List<GridClientNodeBean>>() {});

                        TopologySnapshot newTop = new TopologySnapshot(nodes);

                        if (newTop.isSameCluster(top))
                            log.info("Connection successfully established to cluster with nodes: {}", newTop.nid8());

                        top = newTop;

                        client.emit(EVENT_CLUSTER_TOPOLOGY, toJSON(top));

                        break;

                    default:
                        log.warn(res.getError());

                        clusterDisconnect();
                }
            }
            catch (IOException ignore) {
                clusterDisconnect();
            }
        }
    }
    
    /** */
    private class BroadcastTask implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            try {
                RestResult res = restExecutor.topology(false, true);

                switch (res.getStatus()) {
                    case STATUS_SUCCESS:
                        List<GridClientNodeBean> nodes = mapper.readValue(res.getData(),
                            new TypeReference<List<GridClientNodeBean>>() {});

                        TopologySnapshot newTop = new TopologySnapshot(nodes);

                        if (top == null || top.isSameCluster(newTop)) {
                            clusterDisconnect();

                            log.info("Connection successfully established to cluster with nodes: {}", newTop.nid8());

                            watch();
                        }

                        top = newTop;

                        client.emit(EVENT_CLUSTER_TOPOLOGY, res.getData());

                        break;

                    default:
                        log.warn(res.getError());

                        clusterDisconnect();
                }
            }
            catch (IOException ignore) {
                clusterDisconnect();

                watch();
            }
        }
    }
}
