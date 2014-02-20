// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.metrics;

import org.gridgain.grid.*;

/**
 * Example demonstrates how to use node metrics data. In this example we start up
 * one or more remote nodes and then run benchmark on nodes with more than one
 * processor and idle time percentage of node idle time more 50%.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridMetricsExample {
    /**
     * Executes example on the grid.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-default.xml")) {
            // Execute task.
            g.compute().execute(GridMetricsTask.class, null, 0).get();

            System.out.println(">>>");
            System.out.println(">>> Finished execution of GridMetricsExample.");
            System.out.println(">>> Check log output for every node.");
            System.out.println(">>>");
        }
    }
}
