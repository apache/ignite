// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.closure;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Demonstrates the replacement of functional APIs with traditional task-based
 * execution. This example should be used together with corresponding functional
 * example code to see the difference in coding approach.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 * @see GridClosureExample3
 */
public class GridTaskExample3 {
    /**
     * Executes broadcasting message example without using closures and functional API.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-default.xml") : GridGain.start(args[0])) {
            // Executes task.
            g.compute().execute(new GridMessageBroadcastTask(g.nodes()), ">>>>>\n>>>>> Hello Node! :)\n>>>>>").get();

            // Prints.
            System.out.println(">>>>> Check all nodes for numbers output.");
        }
    }

    /**
     * This class defines grid task for this example.
     *
     * @author @java.author
     * @version @java.version
     */
    private static class GridMessageBroadcastTask extends GridComputeTaskNoReduceAdapter<String> {
        /** Execution nodes. */
        private Iterable<GridNode> nodes;

        /**
         * Creates task for message broadcast.
         *
         * @param nodes Nodes for execution.
         */
        GridMessageBroadcastTask(Iterable<GridNode> nodes) {
            this.nodes = nodes;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> gridNodes,
            @Nullable final String msg) throws GridException {
            // For each node create a job for message output.
            Map<GridComputeJob, GridNode> map = new HashMap<>();

            for (GridNode node : nodes) {
                if (gridNodes.contains(node)) {
                    map.put(new GridComputeJobOneWayAdapter() {
                        @Override public void oneWay() {
                            System.out.println(msg);
                        }
                    }, node);
                }
            }

            return map;
        }
    }
}
