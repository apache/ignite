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

package org.apache.ignite.examples.cluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;

/**
 * Demonstrates new functional APIs.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public class ClusterGroupExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(ignite.cluster(), 2))
                return;

            System.out.println();
            System.out.println("Compute example started.");

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
            sayHello(ignite, cluster.forPredicate(n -> n.metrics().getCurrentCpuLoad() < 0.5));
        }
    }

    /**
     * Print 'Hello' message on remote nodes.
     *
     * @param ignite Ignite.
     * @param grp Cluster group.
     * @throws IgniteException If failed.
     */
    private static void sayHello(Ignite ignite, final ClusterGroup grp) throws IgniteException {
        // Print out hello message on all cluster nodes.
        ignite.compute(grp).broadcast(
            () -> System.out.println(">>> Hello Node: " + grp.ignite().cluster().localNode().id()));
    }
}