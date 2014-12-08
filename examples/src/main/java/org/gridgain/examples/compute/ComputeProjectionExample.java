/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.compute;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;

/**
 * Demonstrates new functional APIs.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeProjectionExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-compute.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(ignite.cluster(), 2))
                return;

            System.out.println();
            System.out.println("Compute projection example started.");

            IgniteCluster cluster = ignite.cluster();

            // Say hello to all nodes in the grid, including local node.
            // Note, that Grid itself also implements GridProjection.
            sayHello(ignite, cluster);

            // Say hello to all remote nodes.
            sayHello(ignite, cluster.forRemotes());

            // Pick random node out of remote nodes.
            ClusterGroup randomNode = cluster.forRemotes().forRandom();

            // Say hello to a random node.
            sayHello(ignite, randomNode);

            // Say hello to all nodes residing on the same host with random node.
            sayHello(ignite, cluster.forHost(randomNode.node()));

            // Say hello to all nodes that have current CPU load less than 50%.
            sayHello(ignite, cluster.forPredicate(new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode n) {
                    return n.metrics().getCurrentCpuLoad() < 0.5;
                }
            }));
        }
    }

    /**
     * Print 'Hello' message on remote grid nodes.
     *
     * @param ignite Grid.
     * @param prj Grid projection.
     * @throws GridException If failed.
     */
    private static void sayHello(Ignite ignite, final ClusterGroup prj) throws GridException {
        // Print out hello message on all projection nodes.
        ignite.compute(prj).broadcast(
            new IgniteRunnable() {
                @Override public void run() {
                    // Print ID of remote node on remote node.
                    System.out.println(">>> Hello Node: " + prj.ignite().cluster().localNode().id());
                }
            }
        );
    }
}
