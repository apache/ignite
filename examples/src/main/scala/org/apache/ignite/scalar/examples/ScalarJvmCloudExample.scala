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
