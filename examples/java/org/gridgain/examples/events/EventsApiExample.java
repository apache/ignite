/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.events;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.examples.compute.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 * <p>
 * Remote nodes should always be started with configuration: {@code 'ggstart.sh examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start
 * GridGain node with {@code examples/config/example-compute.xml} configuration.
 */
public class EventsApiExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid grid = GridGain.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println(">>> Events API example started.");

            // Listen to events happening on local node.
            localListen();

            // Listen to events happening on all grid nodes.
            remoteListen();

            // Wait for a while while callback is notified about remaining puts.
            Thread.sleep(1000);
        }
    }

    /**
     * Listen to events that happen only on local node.
     *
     * @throws GridException If failed.
     */
    private static void localListen() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        GridPredicate<GridEvent> p = new GridPredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                GridTaskEvent taskEvt = (GridTaskEvent)evt;

                System.out.println();
                System.out.println("Got event notification [evt=" + evt.name() + ", taskName=" + taskEvt.taskName() + ']');

                latch.countDown();

                return true;
            }
        };

        Grid g = GridGain.grid();

        // Register event listener for all local task execution events.
        g.events().localListen(p, EVTS_TASK_EXECUTION);

        // Generate task events.
        g.compute().withName("example-local-event-task").run(new GridRunnable() {
            @Override public void run() {
                System.out.println();
                System.out.println("Executing sample job.");
            }
        }).get();

        // Wait for the listener to be notified.
        latch.await();

        // Unregister event listener.
        g.events().stopLocalListen(p, EVTS_TASK_EXECUTION);
    }

    /**
     * Listen to events coming from all grid nodes.
     *
     * @throws GridException If failed.
     */
    private static void remoteListen() throws GridException {
        Grid g = GridGain.grid();

        // Register remote event listeners on all nodes.
        GridFuture<?> fut = g.events().remoteListen(
            // This optional local callback is called for each event notification
            // that passed remote predicate filter.
            new GridBiPredicate<UUID, GridEvent>() {
                @Override public boolean apply(UUID nodeId, GridEvent evt) {
                    GridTaskEvent taskEvt = (GridTaskEvent) evt;

                    System.out.println();
                    System.out.println("Received event [evt=" + evt.name() + ", taskName=" + taskEvt.taskName());

                    return true; // Return true to continue listening.
                }
            },
            // Remote filter which only accepts events for tasks that have names ending with digit greater than 4.
            new GridPredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    GridTaskEvent taskEvt = (GridTaskEvent) evt;

                    int lastDigit = Integer.parseInt(taskEvt.taskName().substring(taskEvt.taskName().length() - 1));

                    return lastDigit > 4;
                }
            },
            // Types of events for which listeners are registered.
            EVTS_TASK_EXECUTION);

        // Wait until event listeners are subscribed on all nodes.
        fut.get();

        int keyCnt = 20;

        // Generate task events.
        for (int i = 0; i < keyCnt; i++) {
            final int i0 = i;

            g.compute().withName("example-remote-event-task" + i0).run(new GridRunnable() {
                @Override public void run() {
                    System.out.println();
                    System.out.println("Executing sample job " + i0 + ".");
                }
            }).get();
        }
    }
}
