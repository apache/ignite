/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.events;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;

import java.util.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 * Note that grid events are disabled by default and must be specifically enabled,
 * just like in {@code examples/config/example-compute.xml} file.
 * <p>
 * Remote nodes should always be started with configuration: {@code 'ggstart.sh examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start
 * GridGain node with {@code examples/config/example-compute.xml} configuration.
 */
public class EventsExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-compute.xml")) {
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
        System.out.println();
        System.out.println(">>> Local event listener example.");

        Ignite g = Ignition.grid();

        IgnitePredicate<IgniteTaskEvent> lsnr = new IgnitePredicate<IgniteTaskEvent>() {
            @Override public boolean apply(IgniteTaskEvent evt) {
                System.out.println("Received task event [evt=" + evt.name() + ", taskName=" + evt.taskName() + ']');

                return true; // Return true to continue listening.
            }
        };

        // Register event listener for all local task execution events.
        g.events().localListen(lsnr, EVTS_TASK_EXECUTION);

        // Generate task events.
        g.compute().withName("example-event-task").run(new IgniteRunnable() {
            @Override public void run() {
                System.out.println("Executing sample job.");
            }
        });

        // Unsubscribe local task event listener.
        g.events().stopLocalListen(lsnr);
    }

    /**
     * Listen to events coming from all grid nodes.
     *
     * @throws GridException If failed.
     */
    private static void remoteListen() throws GridException {
        System.out.println();
        System.out.println(">>> Remote event listener example.");

        // This optional local callback is called for each event notification
        // that passed remote predicate listener.
        IgniteBiPredicate<UUID, IgniteTaskEvent> locLsnr = new IgniteBiPredicate<UUID, IgniteTaskEvent>() {
            @Override public boolean apply(UUID nodeId, IgniteTaskEvent evt) {
                // Remote filter only accepts tasks whose name being with "good-task" prefix.
                assert evt.taskName().startsWith("good-task");

                System.out.println("Received task event [evt=" + evt.name() + ", taskName=" + evt.taskName());

                return true; // Return true to continue listening.
            }
        };

        // Remote filter which only accepts tasks whose name begins with "good-task" prefix.
        IgnitePredicate<IgniteTaskEvent> rmtLsnr = new IgnitePredicate<IgniteTaskEvent>() {
            @Override public boolean apply(IgniteTaskEvent evt) {
                return evt.taskName().startsWith("good-task");
            }
        };

        Ignite g = Ignition.grid();

        // Register event listeners on all nodes to listen for task events.
        g.events().remoteListen(locLsnr, rmtLsnr, EVTS_TASK_EXECUTION);

        // Generate task events.
        for (int i = 0; i < 10; i++) {
            g.compute().withName(i < 5 ? "good-task-" + i : "bad-task-" + i).run(new IgniteRunnable() {
                // Auto-inject task session.
                @IgniteTaskSessionResource
                private ComputeTaskSession ses;

                @Override public void run() {
                    System.out.println("Executing sample job for task: " + ses.getTaskName());
                }
            });
        }
    }
}
