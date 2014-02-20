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
 */
public class GridTaskExample2 {
    /**
     * Executes GCD and LCM calculation without closures and functional API.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-default.xml") : GridGain.start(args[0])) {
            int bound = 100;
            int size = 10;

            // Initialises collection of pair random numbers.
            Collection<int[]> pairs = new ArrayList<>(size);

            // Fills collection.
            for (int i = 0; i < size; i++) {
                pairs.add(new int[] {
                    GridNumberUtils.getRand(bound), GridNumberUtils.getRand(bound)
                });
            }

            // Executes task.
            g.compute().execute(new GridNumberCalculationTask(), pairs, 0).get();

            // Prints.
            System.out.println(">>>>> Check all nodes for number and their GCD and LCM output.");
        }
    }

    /**
     * This class defines grid task for this example.
     * This particular implementation creates a collection of jobs for each number pair.
     * Each job will calculate and print GCD and LCM for each pair.
     *
     * @author @java.author
     * @version @java.version
     */
    private static class GridNumberCalculationTask extends GridComputeTaskNoReduceSplitAdapter<Collection<int[]>> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Collection<int[]> pairs)
            throws GridException {
            // Creates collection of jobs.
            Collection<GridComputeJob> jobs = new ArrayList<>(pairs.size());

            // Fills collection with calculation jobs.
            for(final int[] pair : pairs) {
                jobs.add(new GridComputeJobOneWayAdapter() {
                    @Override public void oneWay() {
                        int gcd = GridNumberUtils.getGCD(pair[0], pair[1]);
                        int lcm = GridNumberUtils.getLCM(pair[0], pair[1]);

                        System.out.printf(">>>>> Numbers: %d and %d. GCD: %d. LCM: %d.%n", pair[0], pair[1], gcd, lcm);
                    }
                });
            }

            return jobs;
        }
    }
}
