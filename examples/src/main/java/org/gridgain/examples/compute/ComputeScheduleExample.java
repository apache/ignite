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
import org.gridgain.grid.scheduler.*;

import java.util.concurrent.*;

/**
 * Demonstrates a cron-based {@link Runnable} execution scheduling.
 * Test runnable object broadcasts a phrase to all grid nodes every minute
 * three times with initial scheduling delay equal to five seconds.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeScheduleExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = GridGain.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println("Compute schedule example started.");

            // Schedule output message every minute.
            GridSchedulerFuture<?> fut = g.scheduler().scheduleLocal(
                new Callable<Integer>() {
                    private int invocations;

                    @Override public Integer call() {
                        invocations++;

                        try {
                            g.compute().broadcast(
                                new GridRunnable() {
                                    @Override public void run() {
                                        System.out.println();
                                        System.out.println("Howdy! :) ");
                                    }
                                }
                            );
                        }
                        catch (GridException e) {
                            throw new GridRuntimeException(e);
                        }

                        return invocations;
                    }
                },
                "{5, 3} * * * * *" // Cron expression.
            );

            while (!fut.isDone())
                System.out.println(">>> Invocation #: " + fut.get());

            System.out.println();
            System.out.println(">>> Schedule future is done and has been unscheduled.");
            System.out.println(">>> Check all nodes for hello message output.");
        }
    }
}
