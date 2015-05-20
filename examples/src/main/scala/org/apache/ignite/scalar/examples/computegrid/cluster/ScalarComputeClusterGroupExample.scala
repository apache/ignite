/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.computegrid.cluster

import org.apache.ignite.cluster.{ClusterGroup, ClusterNode}
import org.apache.ignite.examples.{ExampleNodeStartup, ExamplesUtils}
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.IgnitePredicate
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{Ignite, IgniteException}

/**
 * Demonstrates new functional APIs.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start node
 * with `examples/config/example-ignite.xml` configuration.
 */
object ScalarComputeClusterGroupExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        val cluster = cluster$

        if (ExamplesUtils.checkMinTopologySize(cluster, 2)) {
            println()
            println("Compute example started.")


            // Say hello to all nodes in the cluster, including local node.
            sayHello(ignite$, cluster)

            // Say hello to all remote nodes.
            sayHello(ignite$, cluster.forRemotes)

            // Pick random node out of remote nodes.
            val randomNode = cluster.forRemotes.forRandom

            // Say hello to a random node.
            sayHello(ignite$, randomNode)

            // Say hello to all nodes residing on the same host with random node.
            sayHello(ignite$, cluster.forHost(randomNode.node))

            // Say hello to all nodes that have current CPU load less than 50%.
            sayHello(ignite$, cluster.forPredicate(new IgnitePredicate[ClusterNode] {
                @impl def apply(n: ClusterNode): Boolean = {
                    n.metrics.getCurrentCpuLoad < 0.5
                }
            }))
        }
    }

    /**
     * Print 'Hello' message on remote nodes.
     *
     * @param ignite Ignite.
     * @param grp Cluster group.
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def sayHello(ignite: Ignite, grp: ClusterGroup) {
        ignite.compute(grp).broadcast(toRunnable(() => {
            println(">>> Hello Node: " + grp.ignite.cluster.localNode.id)
        }))
    }
}
