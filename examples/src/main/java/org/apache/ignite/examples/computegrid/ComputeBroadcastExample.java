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

package org.apache.ignite.examples.computegrid;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * Demonstrates broadcasting computations within cluster.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public class ComputeBroadcastExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Compute broadcast example started.");

            // Print hello message on all nodes.
            hello(ignite);

            // Gather system info from all nodes.
            gatherSystemInfo(ignite);
       }
    }

    /**
     * Print 'Hello' message on all nodes.
     *
     * @param ignite Ignite instance.
     * @throws IgniteException If failed.
     */
    private static void hello(Ignite ignite) throws IgniteException {
        // Print out hello message on all nodes.
        ignite.compute().broadcast(() -> {
            System.out.println();
            System.out.println(">>> Hello Node! :)");
        });

        System.out.println();
        System.out.println(">>> Check all nodes for hello message output.");
    }

    /**
     * Gather system info from all nodes and print it out.
     *
     * @param ignite Ignite instance.
     * @throws IgniteException if failed.
     */
    private static void gatherSystemInfo(Ignite ignite) throws IgniteException {
        // Gather system info from all nodes.
        Collection<String> res = ignite.compute().broadcast(() -> {
            System.out.println();
            System.out.println("Executing task on node: " + ignite.cluster().localNode().id());

            return "Node ID: " + ignite.cluster().localNode().id() + "\n" +
                "OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " " +
                System.getProperty("os.arch") + "\n" +
                "User: " + System.getProperty("user.name") + "\n" +
                "JRE: " + System.getProperty("java.runtime.name") + " " +
                System.getProperty("java.runtime.version");
        });

        // Print result.
        System.out.println();
        System.out.println("Nodes system information:");
        System.out.println();

        res.forEach(r -> {
            System.out.println(r);
            System.out.println();
        });
    }
}