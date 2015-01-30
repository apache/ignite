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

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;

import java.util.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Demonstrates event consume API that allows to register event listeners on remote nodes.
 * Note that grid events are disabled by default and must be specifically enabled,
 * just like in {@code examples/config/example-compute.xml} file.
 * <p>
 * Remote nodes should always be started with configuration: {@code 'ignite.sh examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start
 * GridGain node with {@code examples/config/example-compute.xml} configuration.
 */
public class EventsExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
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
     * @throws IgniteCheckedException If failed.
     */
    private static void localListen() throws Exception {
        System.out.println();
        System.out.println(">>> Local event listener example.");

        Ignite g = Ignition.ignite();

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
     * @throws IgniteCheckedException If failed.
     */
    private static void remoteListen() throws IgniteCheckedException {
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

        Ignite g = Ignition.ignite();

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
