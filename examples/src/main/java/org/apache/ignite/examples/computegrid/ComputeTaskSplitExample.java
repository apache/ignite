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
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.jetbrains.annotations.Nullable;

/**
 * Demonstrates a simple use of Ignite with {@link ComputeTaskSplitAdapter}.
 * <p>
 * Phrase passed as task argument is split into jobs each taking one word. Then jobs are distributed among
 * cluster nodes. Each node computes word length and returns result to master node where total phrase length
 * is calculated on reduce stage.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public class ComputeTaskSplitExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println("Compute task split example started.");

            // Execute task on the cluster and wait for its completion.
            int cnt = ignite.compute().execute(CharacterCountTask.class, "Hello Ignite Enabled World!");

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + cnt + "'.");
            System.out.println(">>> Check all nodes for output (this node is also part of the cluster).");
        }
    }

    /**
     * Task to count non-white-space characters in a phrase.
     */
    private static class CharacterCountTask extends ComputeTaskSplitAdapter<String, Integer> {
        /**
         * Splits the received string to words, creates a child job for each word, and sends
         * these jobs to other nodes for processing. Each such job simply prints out the received word.
         *
         * @param clusterSize Number of available cluster nodes. Note that returned number of
         *      jobs can be less, equal or greater than this cluster size.
         * @param arg Task execution argument. Can be {@code null}.
         * @return The list of child jobs.
         */
        @Override protected Collection<? extends ComputeJob> split(int clusterSize, String arg) {
            Collection<ComputeJob> jobs = new LinkedList<>();

            for (final String word : arg.split(" ")) {
                jobs.add(new ComputeJobAdapter() {
                    @Nullable @Override public Object execute() {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from ignite job.");

                        // Return number of letters in the word.
                        return word.length();
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer reduce(List<ComputeJobResult> results) {
            int sum = 0;

            for (ComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }
}