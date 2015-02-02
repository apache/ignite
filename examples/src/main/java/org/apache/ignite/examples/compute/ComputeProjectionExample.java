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

package org.apache.ignite.examples.compute;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.lang.*;

/**
 * Demonstrates new functional APIs.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeProjectionExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-compute.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(ignite.cluster(), 2))
                return;

            System.out.println();
            System.out.println("Compute projection example started.");

            IgniteCluster cluster = ignite.cluster();

            // Say hello to all nodes in the cluster, including local node.
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
     * Print 'Hello' message on remote nodes.
     *
     * @param ignite Ignite.
     * @param prj Projection.
     * @throws IgniteCheckedException If failed.
     */
    private static void sayHello(Ignite ignite, final ClusterGroup prj) throws IgniteCheckedException {
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
