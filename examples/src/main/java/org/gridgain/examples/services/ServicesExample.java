// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.services;

import org.gridgain.grid.*;
import org.gridgain.grid.service.*;

/**
 * Example that demonstrates how to deploy distributed services in GridGain.
 * Distributed services are especially useful when deploying singletons on the grid,
 * be that cluster-singleton, or per-node-singleton, etc...
 * <p>
 * To start remote nodes, you must run {@link ComputeNodeStartup} in another JVM
 * which will start GridGain node with {@code examples/config/example-compute.xml} configuration.
 *
 * @author @java.author
 * @version @java.version
 */
public class ServicesExample {
    public static void main(String[] args) throws GridException {
        try (Grid grid = GridGain.start("examples/config/example-compute.xml")) {
            GridServices svcs = grid.services();

            // Deploy cluster singleton.
            svcs.deployClusterSingleton("myClusterSingletonService", new SimpleService()).get();

            // Deploy node singleton.
            svcs.deployNodeSingleton("myNodeSingletonService", new SimpleService()).get();

            // Deploy 2 instances, regardless of number nodes.
            svcs.deployMultiple("myMultiService", new SimpleService(), 2 /*total number*/, 0 /*0 for unlimited*/).get();
        }
    }
}
