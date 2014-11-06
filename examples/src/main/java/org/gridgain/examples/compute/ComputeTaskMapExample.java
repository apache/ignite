package org.gridgain.examples.compute;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Demonstrates a simple use of GridGain grid with
 * {@link GridComputeTaskAdapter}.
 * <p>
 * Phrase passed as task argument is split into words on map stage and distributed among grid nodes.
 * Each node computes word length and returns result to master node where total phrase length is
 * calculated on reduce stage.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeTaskMapExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println("Compute task map example started.");

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

            Map<GridComputeJob, GridNode> map = new HashMap<>();

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
