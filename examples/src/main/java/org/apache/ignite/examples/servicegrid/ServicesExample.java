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

package org.apache.ignite.examples.servicegrid;

import org.apache.ignite.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;

import java.util.*;

/**
 * Example that demonstrates how to deploy distributed services in Ignite.
 * Distributed services are especially useful when deploying singletons on the ignite,
 * be that cluster-singleton, or per-node-singleton, etc...
 * <p>
 * To start remote nodes, you must run {@link ExampleNodeStartup} in another JVM
 * which will start node with {@code examples/config/example-ignite.xml} configuration.
 * <p>
 * NOTE:<br/>
 * Starting {@code ignite.sh} directly will not work, as distributed services
 * cannot be peer-deployed and classes must be on the classpath for every node.
 */
public class ServicesExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // Mark this node as client node.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            // Deploy services only on server nodes.
            IgniteServices svcs = ignite.services(ignite.cluster().forServers());

            try {
                // Deploy cluster singleton.
                svcs.deployClusterSingleton("myClusterSingletonService", new SimpleMapServiceImpl());

                // Deploy node singleton.
                svcs.deployNodeSingleton("myNodeSingletonService", new SimpleMapServiceImpl());

                // Deploy 2 instances, regardless of number nodes.
                svcs.deployMultiple("myMultiService",
                    new SimpleMapServiceImpl(),
                    2 /*total number*/,
                    0 /*0 for unlimited*/);

                // Example for using a service proxy
                // to access a remotely deployed service.
                serviceProxyExample(ignite);

                // Example for auto-injecting service proxy
                // into remote closure execution.
                serviceInjectionExample(ignite);
            }
            finally {
                // Undeploy all services.
                ignite.services().cancelAll();
            }
        }
    }

    /**
     * Simple example to demonstrate service proxy invocation of a remotely deployed service.
     *
     * @param ignite Ignite instance.
     * @throws Exception If failed.
     */
    private static void serviceProxyExample(Ignite ignite) throws Exception {
        System.out.println(">>>");
        System.out.println(">>> Starting service proxy example.");
        System.out.println(">>>");

        // Get a sticky proxy for node-singleton map service.
        SimpleMapService<Integer, String> mapSvc = ignite.services().serviceProxy("myNodeSingletonService",
            SimpleMapService.class,
            true);

        int cnt = 10;

        // Each service invocation will go over a proxy to some remote node.
        // Since service proxy is sticky, we will always be contacting the same remote node.
        for (int i = 0; i < cnt; i++)
            mapSvc.put(i, Integer.toString(i));

        // Get size from remotely deployed service instance.
        int mapSize = mapSvc.size();

        System.out.println("Map service size: " + mapSize);

        if (mapSize != cnt)
            throw new Exception("Invalid map size [expected=" + cnt + ", actual=" + mapSize + ']');
    }

    /**
     * Simple example to demonstrate how to inject service proxy into distributed closures.
     *
     * @param ignite Ignite instance.
     * @throws Exception If failed.
     */
    private static void serviceInjectionExample(Ignite ignite) throws Exception {
        System.out.println(">>>");
        System.out.println(">>> Starting service injection example.");
        System.out.println(">>>");

        // Get a sticky proxy for cluster-singleton map service.
        SimpleMapService<Integer, String> mapSvc = ignite.services().serviceProxy("myClusterSingletonService",
            SimpleMapService.class,
            true);

        int cnt = 10;

        // Each service invocation will go over a proxy to the remote cluster-singleton instance.
        for (int i = 0; i < cnt; i++)
            mapSvc.put(i, Integer.toString(i));

        // Broadcast closure to every node.
        final Collection<Integer> mapSizes = ignite.compute().broadcast(new SimpleClosure());

        System.out.println("Closure execution result: " + mapSizes);

        // Since we invoked the same cluster-singleton service instance
        // from all the remote closure executions, they should all return
        // the same size equal to 'cnt' value.
        for (int mapSize : mapSizes)
            if (mapSize != cnt)
                throw new Exception("Invalid map size [expected=" + cnt + ", actual=" + mapSize + ']');
    }

    /**
     * Simple closure to demonstrate auto-injection of the service proxy.
     */
    private static class SimpleClosure implements IgniteCallable<Integer> {
        // Auto-inject service proxy.
        @ServiceResource(serviceName = "myClusterSingletonService", proxyInterface = SimpleMapService.class)
        private transient SimpleMapService mapSvc;

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            int mapSize = mapSvc.size();

            System.out.println("Executing closure [mapSize=" + mapSize + ']');

            return mapSize;
        }
    }
}
