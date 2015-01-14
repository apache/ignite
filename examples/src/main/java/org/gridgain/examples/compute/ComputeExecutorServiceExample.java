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

package org.gridgain.examples.compute;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;

import java.util.concurrent.*;

/**
 * Simple example to demonstrate usage of grid-enabled executor service provided by GridGain.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public final class ComputeExecutorServiceExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public static void main(String[] args) throws Exception {
        try (Ignite g = Ignition.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println(">>> Compute executor service example started.");

            // Get grid-enabled executor service.
            ExecutorService exec = g.executorService();

            // Iterate through all words in the sentence and create callable jobs.
            for (final String word : "Print words using runnable".split(" ")) {
                // Execute runnable on some node.
                exec.submit(new IgniteRunnable() {
                    @Override public void run() {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from grid job.");
                    }
                });
            }

            exec.shutdown();

            // Wait for all jobs to complete (0 means no limit).
            exec.awaitTermination(0, TimeUnit.MILLISECONDS);

            System.out.println();
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
        }
    }
}
