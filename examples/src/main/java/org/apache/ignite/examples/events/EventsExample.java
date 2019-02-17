/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.events;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.TaskSessionResource;

import static org.apache.ignite.events.EventType.EVTS_TASK_EXECUTION;

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 * Note that ignite events are disabled by default and must be specifically enabled,
 * just like in {@code examples/config/example-ignite.xml} file.
 * <p>
 * Remote nodes should always be started with configuration: {@code 'ignite.sh examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start
 * node with {@code examples/config/example-ignite.xml} configuration.
 */
public class EventsExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Events API example started.");

            // Listen to events happening on local node.
            localListen();

            // Listen to events happening on all cluster nodes.
            remoteListen();

            // Wait for a while while callback is notified about remaining puts.
            Thread.sleep(1000);
        }
    }

    /**
     * Listen to events that happen only on local node.
     *
     * @throws IgniteException If failed.
     */
    private static void localListen() throws IgniteException {
        System.out.println();
        System.out.println(">>> Local event listener example.");

        Ignite ignite = Ignition.ignite();

        IgnitePredicate<TaskEvent> lsnr = evt -> {
            System.out.println("Received task event [evt=" + evt.name() + ", taskName=" + evt.taskName() + ']');

            return true; // Return true to continue listening.
        };

        // Register event listener for all local task execution events.
        ignite.events().localListen(lsnr, EVTS_TASK_EXECUTION);

        // Generate task events.
        ignite.compute().withName("example-event-task").run(() -> System.out.println("Executing sample job."));

        // Unsubscribe local task event listener.
        ignite.events().stopLocalListen(lsnr);
    }

    /**
     * Listen to events coming from all cluster nodes.
     *
     * @throws IgniteException If failed.
     */
    private static void remoteListen() throws IgniteException {
        System.out.println();
        System.out.println(">>> Remote event listener example.");

        // This optional local callback is called for each event notification
        // that passed remote predicate listener.
        IgniteBiPredicate<UUID, TaskEvent> locLsnr = (nodeId, evt) -> {
            // Remote filter only accepts tasks whose name being with "good-task" prefix.
            assert evt.taskName().startsWith("good-task");

            System.out.println("Received task event [evt=" + evt.name() + ", taskName=" + evt.taskName());

            return true; // Return true to continue listening.
        };

        // Remote filter which only accepts tasks whose name begins with "good-task" prefix.
        IgnitePredicate<TaskEvent> rmtLsnr = evt -> evt.taskName().startsWith("good-task");

        Ignite ignite = Ignition.ignite();

        // Register event listeners on all nodes to listen for task events.
        ignite.events().remoteListen(locLsnr, rmtLsnr, EVTS_TASK_EXECUTION);

        // Generate task events.
        for (int i = 0; i < 10; i++) {
            ignite.compute().withName(i < 5 ? "good-task-" + i : "bad-task-" + i).run(new IgniteRunnable() {
                // Auto-inject task session.
                @TaskSessionResource
                private ComputeTaskSession ses;

                @Override public void run() {
                    System.out.println("Executing sample job for task: " + ses.getTaskName());
                }
            });
        }
    }
}