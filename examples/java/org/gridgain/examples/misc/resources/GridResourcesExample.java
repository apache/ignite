// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.resources;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;

/**
 * This example shows how to use injected user resources inside grid jobs.
 * Jobs use context for insertion data in storage. Only one context object
 * will be created and injected in all split jobs.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridResourcesExample {
    /**
     * Execute {@code Resources} example on the grid.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start(args.length == 0 ? "examples/config/example-default.xml" : args[0])) {
            // Execute task.
            GridComputeTaskFuture<Integer> fut = g.compute().execute(GridResourcesTask.class,
                "Grid Computing Made Simple with GridGain");

            // Wait for task completion.
            int phraseLen = fut.get();

            System.out.println(">>>");
            System.out.println(">>> Finished executing Grid \"Grid Computing Made Simple with GridGain\" " +
                "example with custom task.");
            System.out.println(">>> Total number of characters in the phrase is '" + phraseLen + "'.");
            System.out.println(">>> You should see print out of words from phrase" +
                " \"Grid Computing Made Simple with GridGain\" on different nodes.");
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
            System.out.println(">>>");
        }
    }
}
