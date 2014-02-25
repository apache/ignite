package org.gridgain.examples.compute;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Demonstrates a simple use of GridGain grid with
 * {@link org.gridgain.grid.compute.GridComputeTaskAdapter}.
 * <p>
 * String "Hello Grid Enabled World!" is passed as an argument to
 * {@link GridCompute#execute(String, Object)} method.
 * This method also takes as an argument a task instance, which splits the
 * string into words and wraps each word into a child job, which prints
 * the word to standard output and returns the word length. Those jobs
 * are then distributed among the running nodes. The {@code reduce(...)}
 * method then receives all job results and sums them up. The result
 * of task execution is the number of non-space characters in the
 * sentence that is passed in. All nodes should also print out the words
 * that were processed on them.
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you should (but don't have to) start remote grid instances.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/example-default.xml</pre>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-default.xml} configuration.
 * <p>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * some of the nodes will participate in task execution (check node
 * output).
 *
 * @author @java.author
 * @version @java.version
 */
public class ComputeTaskMapExample {
    /**
     * Execute {@code HelloWorld} example with {@link org.gridgain.grid.compute.GridComputeTaskSplitAdapter}.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-default.xml")) {
            // Execute task on the grid and wait for its completion.
            int cnt = g.compute().execute(CharacterCountTask.class, "Hello Grid Enabled World!").get();

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + cnt + "'.");
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
        }
    }

    /**
     * Task to count non-white-space characters in a phrase.
     */
    private static class CharacterCountTask extends GridComputeTaskAdapter<String, Integer> {
        /**
         * Splits the received string to words, creates a child job for each word, and sends
         * these jobs to other nodes for processing. Each such job simply prints out the received word.
         *
         * @param subgrid Nodes available for this task execution.
         * @param arg String to split into words for processing.
         * @return Map of jobs to nodes.
         */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, String arg) {
            String[] words = arg.split(" ");

            Map<GridComputeJob, GridNode> map = new HashMap<>(words.length);

            Iterator<GridNode> it = subgrid.iterator();

            for (final String word : arg.split(" ")) {
                // If we used all nodes, restart the iterator.
                if (!it.hasNext())
                    it = subgrid.iterator();

                GridNode node = it.next();

                map.put(new GridComputeJobAdapter() {
                    @Nullable @Override public Object execute() {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from grid job.");

                        // Return number of letters in the word.
                        return word.length();
                    }
                }, node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer reduce(List<GridComputeJobResult> results) {
            int sum = 0;

            for (GridComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }
}
