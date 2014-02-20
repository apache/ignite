// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.multinodejvm;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;

import javax.swing.*;
import java.util.concurrent.*;

/**
 * This example demonstrate how you can easily startup multiple nodes
 * in the same JVM. All started nodes use default configuration with
 * only difference of the grid name which has to be different for
 * every node so they can be differentiated within JVM.
 * <p>
 * Starting multiple nodes in the same JVM is especially useful during
 * testing and debugging as it allows you to create a full grid within
 * a test case, simulate various scenarios, and watch how jobs and data
 * behave within a grid.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridMultiNodeJvmExample {
    /** Number of nodes to start. */
    private static final int NODE_COUNT = 5;

    /**
     * Starts multiple nodes in the same JVM.
     *
     * @param args Parameters.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try {
            ExecutorService exe = new ThreadPoolExecutor(NODE_COUNT, NODE_COUNT, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

            // Shared IP finder for in-VM node discovery.
            final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

            for (int i = 0; i < NODE_COUNT; i++) {
                final String nodeName = "jvm-node-" + i;

                // Start nodes concurrently (it's faster).
                exe.submit(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        // All defaults.
                        GridConfiguration cfg = new GridConfiguration();

                        cfg.setGridName(nodeName);

                        // Configure in-VM TCP discovery so we don't
                        // interfere with other grids running on the same network.
                        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

                        discoSpi.setIpFinder(ipFinder);

                        cfg.setDiscoverySpi(discoSpi);

                        GridGain.start(cfg);

                        return null;
                    }
                });
            }

            exe.shutdown();

            exe.awaitTermination(20, TimeUnit.SECONDS);

            // Get first node.
            Grid g = GridGain.grid("jvm-node-0");

            // Print out number of nodes in topology.
            System.out.println("Number of nodes in the grid: " + g.nodes().size());

            // Wait until Ok is pressed.
            JOptionPane.showMessageDialog(
                null,
                new JComponent[] {
                    new JLabel("GridGain JVM cloud started."),
                    new JLabel("Press OK to stop all nodes.")
                },
                "GridGain",
                JOptionPane.INFORMATION_MESSAGE);
        }
        finally {
            GridGain.stopAll(true);
        }
    }
}
