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
 * @see GridClosureExample4
 */
public class GridTaskExample4 {
    /**
     * Executes information gathering example without using closures and functional API.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-default.xml") : GridGain.start(args[0])) {
            // Executes task.
            String res = g.compute().execute(new GridNodeInformationGatheringTask(g.nodes()), null, 0).get();

            // Prints result.
            System.out.println("Nodes system information:");
            System.out.println(res);
        }
    }

    /**
     * This class defines grid task for this example.
     * This particular implementation create job for each checked
     * node for information gathering about it.
     *
     * @author @java.author
     * @version @java.version
     */
    private static class GridNodeInformationGatheringTask extends GridComputeTaskAdapter<Void, String> {
        /** Execution nodes. */
        private Iterable<GridNode> nodes;

        /**
         * Creates task for gathering information.
         *
         * @param nodes Nodes for execution.
         */
        GridNodeInformationGatheringTask(Iterable<GridNode> nodes) {
            this.nodes = nodes;
        }

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> gridNodes,
            @Nullable Void msg) throws GridException {
            // For each checked node create a job for gathering system information.
            Map<GridComputeJob, GridNode> map = new HashMap<>();

            for (GridNode node : nodes) {
                if (gridNodes.contains(node)) {
                    map.put(new GridComputeJobAdapter() {
                        @Override public Object execute() {
                            StringBuilder buf = new StringBuilder();

                            buf.append("OS: ").append(System.getProperty("os.name"))
                                .append(" ").append(System.getProperty("os.version"))
                                .append(" ").append(System.getProperty("os.arch"))
                                .append("\nUser: ").append(System.getProperty("user.name"))
                                .append("\nJRE: ").append(System.getProperty("java.runtime.name"))
                                .append(" ").append(System.getProperty("java.runtime.version"));

                            return buf.toString();
                        }
                    }, node);
                }
            }

            return map;
        }

        /** {@inheritDoc} */
        @Nullable
        @Override public String reduce(List<GridComputeJobResult> results) throws GridException {
            // Reduce results to one string.
            String res = null;

            if (!results.isEmpty()) {
                StringBuilder buf = new StringBuilder();

                for (GridComputeJobResult result : results) {
                    buf.append("\n").append(result.<String>getData()).append("\n");
                }

                res = buf.toString();
            }

            return res;
        }
    }
}
