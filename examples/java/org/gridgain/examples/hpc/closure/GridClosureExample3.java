// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.closure;

import org.gridgain.grid.*;

/**
 * Demonstrates new functional APIs.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 * @see GridTaskExample3
 */
public class GridClosureExample3 {
    /**
     * Executes broadcasting message example with closures.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start(args.length == 0 ? "examples/config/example-default.xml" : args[0])) {
            // Broadcasts message to all nodes.
            g.compute().broadcast(new Runnable() {
                    @Override public void run() {
                        System.out.println(">>>>>");
                        System.out.println(">>>>> Hello Node! :)");
                        System.out.println(">>>>>");
                    }
                }
            ).get();

            // Prints.
            System.out.println(">>>>> Check all nodes for hello message output.");
        }
    }
}
