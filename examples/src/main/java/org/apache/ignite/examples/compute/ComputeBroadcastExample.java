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

package org.apache.ignite.examples.compute;

import org.apache.ignite.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;

import java.util.*;

/**
 * Demonstrates broadcasting computations within grid projection.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeBroadcastExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println(">>> Compute broadcast example started.");

            // Print hello message on all nodes.
            hello(ignite);

            // Gather system info from all nodes.
            gatherSystemInfo(ignite);
       }
    }

    /**
     * Print 'Hello' message on all grid nodes.
     *
     * @param g Grid instance.
     * @throws IgniteCheckedException If failed.
     */
    private static void hello(Ignite g) throws IgniteCheckedException {
        // Print out hello message on all nodes.
        g.compute().broadcast(
            new IgniteRunnable() {
                @Override public void run() {
                    System.out.println();
                    System.out.println(">>> Hello Node! :)");
                }
            }
        );

        System.out.println();
        System.out.println(">>> Check all nodes for hello message output.");
    }

    /**
     * Gather system info from all nodes and print it out.
     *
     * @param g Grid instance.
     * @throws IgniteCheckedException if failed.
     */
    private static void gatherSystemInfo(Ignite g) throws IgniteCheckedException {
        // Gather system info from all nodes.
        Collection<String> res = g.compute().broadcast(
            new IgniteCallable<String>() {
                // Automatically inject grid instance.
                @IgniteInstanceResource
                private Ignite grid;

                @Override public String call() {
                    System.out.println();
                    System.out.println("Executing task on node: " + grid.cluster().localNode().id());

                    return "Node ID: " + grid.cluster().localNode().id() + "\n" +
                        "OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " " +
                        System.getProperty("os.arch") + "\n" +
                        "User: " + System.getProperty("user.name") + "\n" +
                        "JRE: " + System.getProperty("java.runtime.name") + " " +
                        System.getProperty("java.runtime.version");
                }
        });

        // Print result.
        System.out.println();
        System.out.println("Nodes system information:");
        System.out.println();

        for (String r : res) {
            System.out.println(r);
            System.out.println();
        }
    }
}
