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

package org.apache.ignite.examples.misc.schedule;

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.scheduler.SchedulerFuture;

/**
 * Demonstrates a cron-based {@link Runnable} execution scheduling.
 * Test runnable object broadcasts a phrase to all cluster nodes every minute
 * three times with initial scheduling delay equal to five seconds.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public class ComputeScheduleExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println("Compute schedule example started.");

            // Schedule output message every minute.
            SchedulerFuture<?> fut = ignite.scheduler().scheduleLocal(
                new Callable<Integer>() {
                    private int invocations;

                    @Override public Integer call() {
                        invocations++;

                        ignite.compute().broadcast(
                            new IgniteRunnable() {
                                @Override public void run() {
                                    System.out.println();
                                    System.out.println("Howdy! :)");
                                }
                            }
                        );

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