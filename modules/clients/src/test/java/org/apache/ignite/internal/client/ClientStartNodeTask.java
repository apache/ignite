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

package org.apache.ignite.internal.client;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import static org.apache.ignite.compute.ComputeJobResultPolicy.FAILOVER;
import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;

/**
 * Start node task, applicable arguments:
 * <ul>
 *     <li>tcp</li>
 *     <li>http</li>
 *     <li>tcp+ssl</li>
 *     <li>http+ssl</li>
 * </ul>
 */
public class ClientStartNodeTask extends TaskSingleJobSplitAdapter<String, Integer> {
    /**
     * Available node's configurations.
     */
    private static final Map<String, String> NODE_CFG = new HashMap<String, String>() {{
        put("tcp", "modules/clients/src/test/resources/spring-server-node.xml");
        put("http", "modules/clients/src/test/resources/spring-server-node.xml");
        put("tcp+ssl", "modules/clients/src/test/resources/spring-server-ssl-node.xml");
        put("http+ssl", "modules/clients/src/test/resources/spring-server-ssl-node.xml");
    }};

    /** */
    @LoggerResource
    private transient IgniteLogger log;

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** {@inheritDoc} */
    @Override protected Object executeJob(int gridSize, String type) {
        log.info(">>> Starting new grid node [currGridSize=" + gridSize + ", arg=" + type + "]");

        if (type == null)
            throw new IllegalArgumentException("Node type to start should be specified.");

        IgniteConfiguration cfg = getConfig(type);

        // Generate unique for this VM grid name.
        String gridName = cfg.getGridName() + " (" + UUID.randomUUID() + ")";

        // Update grid name (required to be unique).
        cfg.setGridName(gridName);

        // Start new node in current VM.
        Ignite g =  G.start(cfg);

        log.info(">>> Grid started [nodeId=" + g.cluster().localNode().id() + ", name='" + g.name() + "']");

        return true;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        if (res.getException() != null)
            return FAILOVER;

        return WAIT;
    }

    /**
     * Load grid configuration for specified node type.
     *
     * @param type Node type to load configuration for.
     * @return Grid configuration for specified node type.
     */
    static IgniteConfiguration getConfig(String type) {
        String path = NODE_CFG.get(type);

        if (path == null)
            throw new IllegalArgumentException("Unsupported node type: " + type);

        URL url = U.resolveIgniteUrl(path);

        BeanFactory ctx = new FileSystemXmlApplicationContext(url.toString());

        return (IgniteConfiguration)ctx.getBean("grid.cfg");
    }

    /**
     * Example for start/stop node tasks.
     *
     * @param args Not used.
     */
    public static void main(String[] args) {
        String nodeType = "tcp+ssl";

        // Start initial node = 1
        try (Ignite g = G.start(NODE_CFG.get(nodeType))) {
            // Change topology.
            changeTopology(g, 4, 1, nodeType);
            changeTopology(g, 1, 4, nodeType);

            // Stop node by id = 0
            g.compute().execute(ClientStopNodeTask.class, g.cluster().localNode().id().toString());

            // Wait for node stops.
            //U.sleep(1000);

            assert G.allGrids().isEmpty();
        }
        catch (Exception e) {
            System.err.println("Uncaught exception: " + e.getMessage());

            e.printStackTrace(System.err);
        }
    }

    /**
     * Change topology.
     *
     * @param parent Grid to execute tasks on.
     * @param add New nodes count.
     * @param rmv Remove nodes count.
     * @param type Type of nodes to manipulate.
     */
    private static void changeTopology(Ignite parent, int add, int rmv, String type) {
        Collection<ComputeTaskFuture<?>> tasks = new ArrayList<>();

        IgniteCompute comp = parent.compute().withAsync();

        // Start nodes in parallel.
        while (add-- > 0) {
            comp.execute(ClientStartNodeTask.class, type);

            tasks.add(comp.future());
        }

        for (ComputeTaskFuture<?> task : tasks)
            task.get();

        // Stop nodes in sequence.
        while (rmv-- > 0)
            parent.compute().execute(ClientStopNodeTask.class, type);

        // Wait for node stops.
        //U.sleep(1000);

        Collection<String> gridNames = new ArrayList<>();

        for (Ignite g : G.allGrids())
            gridNames.add(g.name());

        parent.log().info(">>> Available grids: " + gridNames);
    }
}