/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.compute;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;

import java.util.*;

/**
 * Demonstrates using of {@link org.apache.ignite.lang.IgniteCallable} job execution on the grid.
 * <p>
 * This example takes a sentence composed of multiple words and counts number of non-space
 * characters in the sentence by having each compute job count characters in each individual
 * word.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeCallableExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = Ignition.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println(">>> Compute callable example started.");

            Collection<IgniteCallable<Integer>> calls = new ArrayList<>();

            // Iterate through all words in the sentence and create callable jobs.
            for (final String word : "Count characters using callable".split(" ")) {
                calls.add(new IgniteCallable<Integer>() {
                    @Override public Integer call() throws Exception {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from grid job.");

                        return word.length();
                    }
                });
            }

            // Execute collection of callables on the grid.
            Collection<Integer> res = g.compute().call(calls);

            int sum = 0;

            // Add up individual word lengths received from remote nodes.
            for (int len : res)
                sum += len;

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + sum + "'.");
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
        }
    }
}
