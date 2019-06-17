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

import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.util.typedef.F;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CLUSTER_NAME;
import static org.apache.ignite.console.demo.AgentClusterDemo.SRV_NODE_NAME;
import static org.apache.ignite.console.websocket.TopologySnapshot.IGNITE_CLUSTER_ID;
import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.sortAddresses;

/**
 * API to transfer topology from demo cluster to Web Console.
 */
public class DemoClusterHandler extends AbstractClusterHandler{
    /** Demo cluster ID. */
    static final String DEMO_CLUSTER_ID = UUID.randomUUID().toString();

    /**
     * @param cfg Config.
     */
    DemoClusterHandler(AgentConfiguration cfg) {
        super(cfg, null);

        System.setProperty(IGNITE_CLUSTER_ID, DEMO_CLUSTER_ID);
        System.setProperty(IGNITE_CLUSTER_NAME, "demo-cluster");
    }

    /** {@inheritDoc} */
    @Override public RestResult restCommand(JsonObject params) throws Throwable {
        if (AgentClusterDemo.getDemoUrl() == null) {
            if (cfg.disableDemo())
                return RestResult.fail(404, "Demo mode disabled by administrator.");

            AgentClusterDemo.tryStart().await();

            if (AgentClusterDemo.getDemoUrl() == null)
                return RestResult.fail(404, "Failed to send request because of embedded node for demo mode is not started yet.");
        }

        return restExecutor.sendRequest(AgentClusterDemo.getDemoUrl(), params);
    }

    /**
     * @return Topology snapshot for demo cluster.
     */
    TopologySnapshot topologySnapshot() {
        if (cfg.disableDemo())
            return null;

        TopologySnapshot top = new TopologySnapshot();

        top.setId(System.getProperty(IGNITE_CLUSTER_ID));
        top.setName(System.getProperty(IGNITE_CLUSTER_NAME));
        top.setClusterVersion(VER_STR);
        top.setSecured(false);
        top.setDemo(true);

        if (AgentClusterDemo.getDemoUrl() != null) {
            Ignite ignite = Ignition.ignite(SRV_NODE_NAME + 0);

            top.setActive(ignite.cluster().active());
            top.setNodes(
                ignite.cluster().nodes().stream()
                    .collect(toMap(
                        ClusterNode::id,
                        n -> new TopologySnapshot.NodeBean(n.isClient(), F.first(sortAddresses(n.addresses())))
                    ))
            );
        }
        else {
            top.setActive(false);
            top.setNodes(Collections.emptyMap());
        }

        return top;
    }
}
