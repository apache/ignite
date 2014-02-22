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

import java.util.concurrent.*;

/**
 * Simple example to demonstrate usage of grid-enabled executor service provided by GridGain.
 * <p>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridExecutorExample {
    /**
     * Execute {@code Executor} example on the grid.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws Exception If example execution failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("examples/config/example-default.xml")) {
            // Get grid-enabled executor service.
            ExecutorService exec = g.compute().executor();

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
