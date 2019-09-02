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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.client.message.GridClientNodeBean;
import org.apache.ignite.internal.processors.rest.handlers.top.GridTopologyCommandHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestTopologyRequest;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.IgniteVersionUtils.VER_STR;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.TOPOLOGY;

/**
 * API to transfer topology from demo cluster to Web Console.
 */
public class DemoClusterHandler extends AbstractClusterHandler{
    /** Demo cluster ID. */
    static final String DEMO_CLUSTER_ID = UUID.randomUUID().toString();

    /** Demo cluster name. */
    private static final String DEMO_CLUSTER_NAME = "demo-cluster";

    /**
     * @param cfg Config.
     */
    DemoClusterHandler(AgentConfiguration cfg) {
        super(cfg, null);
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

        TopologySnapshot top;

        if (AgentClusterDemo.getDemoUrl() != null) {
            IgniteEx ignite = (IgniteEx)F.first(Ignition.allGrids());

            Collection<GridClientNodeBean> nodes = collectNodes(ignite.context());

            top = new TopologySnapshot(nodes);

            top.setActive(ignite.cluster().active());
        }
        else {
            top = new TopologySnapshot();
            
            top.setClusterVersion(VER_STR);
        }

        top.setId(DEMO_CLUSTER_ID);
        top.setName(DEMO_CLUSTER_NAME);
        top.setDemo(true);

        return top;
    }

    /**
     * @param ctx Context.
     */
    private Collection<GridClientNodeBean> collectNodes(GridKernalContext ctx) {
        try {
            GridTopologyCommandHandler hnd = new GridTopologyCommandHandler(ctx);

            GridRestTopologyRequest req = new GridRestTopologyRequest();

            req.command(TOPOLOGY);
            req.includeAttributes(true);

            GridRestResponse res = hnd.handleAsync(req).getUninterruptibly();

            return (Collection<GridClientNodeBean>)res.getResponse();
        }
        catch (IgniteCheckedException e) {
            return Collections.emptyList();
        }
    }
}
