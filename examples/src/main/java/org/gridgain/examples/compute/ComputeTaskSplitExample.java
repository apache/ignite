package org.gridgain.examples.compute;

import org.apache.ignite.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Demonstrates a simple use of GridGain grid with {@link GridComputeTaskSplitAdapter}.
 * <p>
 * Phrase passed as task argument is split into jobs each taking one word. Then jobs are distributed among
 * grid nodes. Each node computes word length and returns result to master node where total phrase length
 * is calculated on reduce stage.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeTaskSplitExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = GridGain.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println("Compute task split example started.");

            // Execute task on the grid and wait for its completion.
            int cnt = g.compute().execute(CharacterCountTask.class, "Hello Grid Enabled World!");

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + cnt + "'.");
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
        }
    }

    /**
     * Task to count non-white-space characters in a phrase.
     */
    private static class CharacterCountTask extends GridComputeTaskSplitAdapter<String, Integer> {
        /**
         * Splits the received string to words, creates a child job for each word, and sends
         * these jobs to other nodes for processing. Each such job simply prints out the received word.
         *
         * @param gridSize Number of available grid nodes. Note that returned number of
         *      jobs can be less, equal or greater than this grid size.
         * @param arg Task execution argument. Can be {@code null}.
         * @return The list of child jobs.
         */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, String arg) {
            Collection<GridComputeJob> jobs = new LinkedList<>();

            for (final String word : arg.split(" ")) {
                jobs.add(new GridComputeJobAdapter() {
                    @Nullable @Override public Object execute() {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from grid job.");

                        // Return number of letters in the word.
                        return word.length();
                    }
                });
            }

            return jobs;
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
