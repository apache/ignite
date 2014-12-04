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

import java.util.concurrent.*;

/**
 * Simple example to demonstrate usage of grid-enabled executor service provided by GridGain.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public final class ComputeExecutorServiceExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println(">>> Compute executor service example started.");

            // Get grid-enabled executor service.
            ExecutorService exec = g.executorService();

            // Iterate through all words in the sentence and create callable jobs.
            for (final String word : "Print words using runnable".split(" ")) {
                // Execute runnable on some node.
                exec.submit(new GridRunnable() {
                    @Override public void run() {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from grid job.");
                    }
                });
            }

            exec.shutdown();

            // Wait for all jobs to complete (0 means no limit).
            exec.awaitTermination(0, TimeUnit.MILLISECONDS);

            System.out.println();
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
        }
    }
}
