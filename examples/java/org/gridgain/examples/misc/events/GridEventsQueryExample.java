// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.events;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Example of querying events. In this example we execute a dummy
 * task to generate events, and then query them.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridEventsQueryExample {
    /**
     * Executes an example task on the grid in order to generate events and then lists
     * all events that have occurred from task execution.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start(args.length == 0 ? "examples/config/example-default.xml" : args[0])) {
            // Executes example task to generate events.
            g.compute().execute(GridEventsExampleTask.class, null, 0).get();

            // Retrieve and filter nodes events.
            List<GridEvent> evts = g.events().queryRemote(
                // Always true predicate.
                new GridPredicate<GridEvent>() {
                    @Override public boolean apply(GridEvent e) {
                        return true;
                    }
                },
                0
            ).get();

            for (GridEvent evt : evts)
                System.out.println(">>> Found grid event: " + evt);

            System.out.println(">>>");
            System.out.println(">>> Finished executing Grid Events Query Example.");
            System.out.println(">>> Check local node output.");
            System.out.println(">>>");
        }
    }
}
