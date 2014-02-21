// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.schedule;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Demonstrates a cron-based {@link Runnable} execution scheduling.
 * Test runnable object broadcasts a phrase to all grid nodes every minute
 * three times with initial scheduling delay equal to five seconds.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridScheduleRunnableExample {
    /**
     * Executes scheduling example.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = args.length == 0 ? GridGain.start("examples/config/example-default.xml") : GridGain.start(args[0])) {
            // Schedule output message every minute.
            g.scheduler().scheduleLocal(
                new GridRunnable() {
                    @Override public void run() {
                        try {
                            g.compute().run(new Runnable() {
                                @Override public void run() {
                                    System.out.println("Howdy! :)");
                                }
                            }).get();
                        }
                        catch (GridException e) {
                            throw new GridRuntimeException(e);
                        }
                    }
                },
                "{5, 3} * * * * *" // Cron expression.
            );

            // Sleep.
            U.sleep(1000 * 60 * 3);

            // Prints.
            System.out.println(">>>>> Check all nodes for hello message output.");
        }
    }
}
