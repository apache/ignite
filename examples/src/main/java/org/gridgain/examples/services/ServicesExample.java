/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.services;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.service.*;

import java.util.*;

/**
 * Example that demonstrates how to deploy distributed services in GridGain.
 * Distributed services are especially useful when deploying singletons on the grid,
 * be that cluster-singleton, or per-node-singleton, etc...
 * <p>
 * To start remote nodes, you must run {@link ComputeNodeStartup} in another JVM
 * which will start GridGain node with {@code examples/config/example-compute.xml} configuration.
 * <p>
 * NOTE:<br/>
 * Starting {@code ggstart.sh} directly will not work, as distributed services
 * cannot be peer-deployed and classes must be on the classpath for every node.
 */
public class ServicesExample {
    public static void main(String[] args) throws Exception {
        try (Grid grid = GridGain.start("examples/config/example-compute.xml")) {
            GridProjection rmts = grid.forRemotes();

            if (rmts.nodes().isEmpty()) {
                System.err.println(">>>");
                System.err.println(">>> Must start at least one remote node using " +
                    ComputeNodeStartup.class.getSimpleName() + '.');
                System.err.println(">>>");

                return;
            }

            GridServices svcs = rmts.services();

            // Deploy cluster singleton.
            svcs.deployClusterSingleton("myClusterSingletonService", new SimpleMapServiceImpl()).get();

            // Deploy node singleton.
            svcs.deployNodeSingleton("myNodeSingletonService", new SimpleMapServiceImpl()).get();

            // Deploy 2 instances, regardless of number nodes.
            svcs.deployMultiple("myMultiService", new SimpleMapServiceImpl(), 2 /*total number*/, 0 /*0 for unlimited*/).get();

            // Example for using a service proxy
            // to access a remotely deployed service.
            serviceProxyExample(grid);

            // Example for auto-injecting service proxy
            // into remote closure execution.
            serviceInjectionExample(grid);
        }
    }

    /**
     * Simple example to demonstrate service proxy invocation of a remotely deployed service.
     *
     * @param grid Grid instance.
     * @throws Exception If failed.
     */
    private static void serviceProxyExample(Grid grid) throws Exception {
        System.out.println(">>>");
        System.out.println(">>> Starting service proxy example.");
        System.out.println(">>>");

        // Get a sticky proxy for node-singleton map service.
        SimpleMapService<Integer, String> mapSvc = grid.services().serviceProxy("myNodeSingletonService", SimpleMapService.class, true);

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
     * @param grid Grid instance.
     * @throws Exception If failed.
     */
    private static void serviceInjectionExample(Grid grid) throws Exception {
        System.out.println(">>>");
        System.out.println(">>> Starting service injection example.");
        System.out.println(">>>");

        // Get a sticky proxy for cluster-singleton map service.
        SimpleMapService<Integer, String> mapSvc = grid.services().serviceProxy("myClusterSingletonService", SimpleMapService.class, true);

        int cnt = 10;

        // Each service invocation will go over a proxy to the remote cluster-singleton instance.
        for (int i = 0; i < cnt; i++)
            mapSvc.put(i, Integer.toString(i));

        // Broadcast closure to every node.
        final Collection<Integer> mapSizes = grid.compute().broadcast(new SimpleClosure()).get();

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
    private static class SimpleClosure implements GridCallable<Integer> {
        // Auto-inject service proxy.
        @GridServiceResource(serviceName = "myClusterSingletonService", proxyInterface = SimpleMapService.class)
        private SimpleMapService mapSvc;

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            int mapSize = mapSvc.size();

            System.out.println("Executing closure [mapSize=" + mapSize + ']');

            return mapSize;
        }
    }
}
