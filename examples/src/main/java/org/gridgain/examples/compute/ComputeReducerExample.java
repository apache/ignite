/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.compute;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Demonstrates a simple use of GridGain grid with reduce closure.
 * <p>
 * Phrase is split into words and distributed across Grid nodes where length of each word is
 * calculated. Then total phrase length is calculated using reducer.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeReducerExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println("Compute reducer example started.");

            Integer sum = g.compute().apply(
                new GridClosure<String, Integer>() {
                    @Override public Integer apply(String word) {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from grid job.");

                        // Return number of letters in the word.
                        return word.length();
                    }
                },

                // Job parameters. GridGain will create as many jobs as there are parameters.
                Arrays.asList("Count characters using reducer".split(" ")),

                // Reducer to process results as they come.
                new GridReducer<Integer, Integer>() {
                    private AtomicInteger sum = new AtomicInteger();

                    // Callback for every job result.
                    @Override public boolean collect(Integer len) {
                        sum.addAndGet(len);

                        // Return true to continue waiting until all results are received.
                        return true;
                    }

                    // Reduce all results into one.
                    @Override public Integer reduce() {
                        return sum.get();
                    }
                }
            ).get();

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + sum + "'.");
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
        }
    }
}
