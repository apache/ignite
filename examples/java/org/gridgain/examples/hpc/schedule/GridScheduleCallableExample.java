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
import org.gridgain.grid.scheduler.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Demonstrates a cron-based {@link Callable} execution scheduling.
 * The example schedules task that returns result. To trace the execution result it uses method
 * {@link GridSchedulerFuture#get()} blocking current thread and waiting for result of the next execution.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridScheduleCallableExample {
    /**
     * Executes scheduling example.
     *
     * @param args Command line arguments, none required but if provided
     *             first one should point to the Spring XML configuration file. See
     *             {@code "examples/config/"} for configuration file examples.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("examples/config/example-default.xml")) {
            // Schedule callable that returns incremented value each time.
            GridSchedulerFuture<Integer> fut = g.scheduler().scheduleLocal(
                new GridCallable<Integer>() {
                    private int cnt;

                    @Override public Integer call() {
                        return ++cnt;
                    }
                },
                "{1, 3} * * * * *" // Cron expression.
            );

            System.out.println(">>> Started scheduling callable execution at " + new Date() + ". " +
                "Wait for 3 minutes and check the output.");

            System.out.println(">>> First execution result: " + fut.get() + ", time: " + new Date());
            System.out.println(">>> Second execution result: " + fut.get() + ", time: " + new Date());
            System.out.println(">>> Third execution result: " + fut.get() + ", time: " + new Date());

            System.out.println(">>> Execution scheduling stopped after 3 executions.");

            // Prints.
            System.out.println(">>> Check local node for output.");
        }
    }
}
