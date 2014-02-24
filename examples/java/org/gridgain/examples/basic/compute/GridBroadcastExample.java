// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.basic.compute;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;

import java.util.*;

/**
 * Demonstrates new functional APIs.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridBroadcastExample {
    /**
     * Executes broadcasting message example with closures.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid grid = GridGain.start("examples/config/example-default.xml")) {
            // Print hello message on all nodes.
            hello(grid);

            // Gather system info from all nodes.
            gatherSystemInfo(grid);
        }
    }

    /**
     * Print 'Hello' message on all grid nodes.
     *
     * @param g Grid instance.
     * @throws GridException If failed.
     */
    private static void hello(Grid g) throws GridException {
        // Print out hello message on all nodes.
        g.compute().broadcast(
            new GridRunnable() {
                @Override public void run() {
                    System.out.println();
                    System.out.println(">>> Hello Node! :)");
                }
            }
        ).get();

        System.out.println(">>> Check all nodes for hello message output.");
    }

    /**
     * Gather system info from all nodes and print it out.
     *
     * @param g Grid instance.
     * @throws GridException if failed.
     */
    private static void gatherSystemInfo(Grid g) throws GridException {
        // Gather system info from all nodes.
        Collection<String> res = g.compute().broadcast(
            new GridCallable<String>() {
                // Automatically inject grid instance.
                @GridInstanceResource
                private Grid grid;

                public String call() {
                    System.out.println("Executing task on node: " + grid.localNode().id());

                    return "Node ID: " + grid.localNode().id() + "\n" +
                        "OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " " +
                        System.getProperty("os.arch") + "\n" +
                        "User: " + System.getProperty("user.name") + "\n" +
                        "JRE: " + System.getProperty("java.runtime.name") + " " +
                        System.getProperty("java.runtime.version");
                }
        }).get();

        // Print result.
        System.out.println("Nodes system information:");
        System.out.println();

        for (String r : res) {
            System.out.println(r);
            System.out.println();
        }
    }
}
