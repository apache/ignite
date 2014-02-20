// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.closure;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Demonstrates new functional APIs.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 * @see GridTaskExample4
 */
public class GridClosureExample4 {
    /**
     * Executes information gathering example with closures.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start(args.length == 0 ? "examples/config/example-default.xml" : args[0])) {
            // Broadcast closure to all nodes for gathering their system information.
            String res = g.compute().reduce(GridClosureCallMode.BROADCAST,
                Collections.<GridOutClosure<String>>singleton(
                    new GridOutClosure<String>() {
                        @Override public String apply() {
                            StringBuilder buf = new StringBuilder();

                            buf.append("OS: ").append(System.getProperty("os.name"))
                                .append(" ").append(System.getProperty("os.version"))
                                .append(" ").append(System.getProperty("os.arch"))
                                .append("\nUser: ").append(System.getProperty("user.name"))
                                .append("\nJRE: ").append(System.getProperty("java.runtime.name"))
                                .append(" ").append(System.getProperty("java.runtime.version"));

                            return buf.toString();
                        }
                    }
                ),
                new GridReducer<String, String>() {
                    private StringBuilder buf = new StringBuilder();

                    @Override public boolean collect(String s) {
                        buf.append("\n").append(s).append("\n");

                        return true;
                    }

                    @Override public String apply() {
                        return buf.toString();
                    }
                }
            ).get();

            // Print result.
            System.out.println("Nodes system information:");
            System.out.println(res);
        }
    }
}
