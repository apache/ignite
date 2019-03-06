/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.scalar.examples

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit._
import javax.swing.{JComponent, JLabel, JOptionPane}

import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.scalar.scalar
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder

/**
 * This example demonstrates how you can easily startup multiple nodes
 * in the same JVM with Scala. All started nodes use default configuration
 * with only difference of the ignite cluster name which has to be different for
 * every node so they can be differentiated within JVM.
 * <p/>
 * Starting multiple nodes in the same JVM is especially useful during
 * testing and debugging as it allows you to create a full ignite cluster within
 * a test case, simulate various scenarios, and watch how jobs and data
 * behave within a ignite cluster.
 */
object ScalarJvmCloudExample {
    /** Names of nodes to start. */
    val NODES = List("scalar-node-0", "scalar-node-1", "scalar-node-2", "scalar-node-3", "scalar-node-4")

    def main(args: Array[String]) {
        try {
            // Shared IP finder for in-VM node discovery.
            val ipFinder = new TcpDiscoveryVmIpFinder(true)

            val pool = Executors.newFixedThreadPool(NODES.size)

            // Concurrently startup all nodes.
            NODES.foreach(name => pool.execute(new Runnable {
                @impl def run() {
                    // All defaults.
                    val cfg = new IgniteConfiguration

                    cfg.setGridName(name)

                    // Configure in-VM TCP discovery so we don't
                    // interfere with other ignites running on the same network.
                    val discoSpi = new TcpDiscoverySpi

                    discoSpi.setIpFinder(ipFinder)

                    cfg.setDiscoverySpi(discoSpi)

                    // Start node
                    scalar.start(cfg)

                    ()
                }
            }))

            pool.shutdown()

            pool.awaitTermination(Long.MaxValue, MILLISECONDS)

            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                Array[JComponent](
                    new JLabel("Ignite JVM cloud started."),
                    new JLabel("Number of nodes in the cluster: " + scalar.ignite$(NODES(1)).get.cluster().nodes().size()),
                    new JLabel("Click OK to stop.")
                ),
                "Ignite",
                JOptionPane.INFORMATION_MESSAGE)

        }
        // Stop all nodes
        finally
            NODES.foreach(node => scalar.stop(node, true))
    }
}
