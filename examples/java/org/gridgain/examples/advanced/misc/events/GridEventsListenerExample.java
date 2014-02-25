// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.misc.events;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Example for listening to local grid events. In this example we register
 * a local grid event listener and then demonstrate how it gets notified
 * of all events that occur from a dummy task execution.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public final class GridEventsListenerExample {
    /**
     * Registers a listener to local grid events and prints all events that
     * occurred from a sample task execution.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-default.xml") : GridGain.start(args[0])) {
            // Define event listener
            GridLocalEventListener lsnr = new GridLocalEventListener() {
                @Override public void onEvent(GridEvent evt) {
                    System.out.println(">>> Grid event occurred: " + evt);
                }
            };

            // Register event listener for all task execution local events.
            g.events().localListen(lsnr, EVTS_TASK_EXECUTION);

            // Executes example task to generate events.
            g.compute().execute(GridEventsExampleTask.class, null).get();

            // Remove local event listener.
            g.events().removeLocalListener(lsnr);

            System.out.println(">>>");
            System.out.println(">>> Finished executing Grid Event Listener Example.");
            System.out.println(">>> Check local node output.");
            System.out.println(">>>");
        }
    }
}
