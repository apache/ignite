package org.gridgain.scalar.examples

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit._
import javax.swing.{JComponent, JLabel, JOptionPane}
import org.apache.ignite.configuration.IgniteConfiguration
import org.gridgain.grid.spi.discovery.tcp.TcpDiscoverySpi
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder
import org.gridgain.grid.util.scala.impl
import org.gridgain.scalar.scalar

/**
 * This example demonstrates how you can easily startup multiple nodes
 * in the same JVM with Scala. All started nodes use default configuration
 * with only difference of the grid name which has to be different for
 * every node so they can be differentiated within JVM.
 * <p>
 * Starting multiple nodes in the same JVM is especially useful during
 * testing and debugging as it allows you to create a full grid within
 * a test case, simulate various scenarios, and watch how jobs and data
 * behave within a grid.
 */
object ScalarJvmCloudExample {
    /** Names of nodes to start. */
    val NODES = List("scalar-node-0", "scalar-node-1", "scalar-node-2", "scalar-node-3", "scalar-node-4")

    def main(args: Array[String]) {
        try {
            // Shared IP finder for in-VM node discovery.
            val ipFinder = new GridTcpDiscoveryVmIpFinder(true)

            val pool = Executors.newFixedThreadPool(NODES.size)

            // Concurrently startup all nodes.
            NODES.foreach(name => pool.submit(new Runnable {
                @impl def run() {
                    // All defaults.
                    val cfg = new IgniteConfiguration

                    cfg.setGridName(name)

                    // Configure in-VM TCP discovery so we don't
                    // interfere with other grids running on the same network.
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
                    new JLabel("GridGain JVM cloud started."),
                    new JLabel("Number of nodes in the grid: " + scalar.grid$(NODES(1)).get.cluster().nodes().size()),
                    new JLabel("Click OK to stop.")
                ),
                "GridGain",
                JOptionPane.INFORMATION_MESSAGE)

        }
        // Stop all nodes
        finally
            NODES.foreach(node => scalar.stop(node, true))
    }
}
