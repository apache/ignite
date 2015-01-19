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
import org.apache.ignite.compute.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Demonstrates a simple use of GridGain grid with {@link org.apache.ignite.compute.ComputeTaskSplitAdapter}.
 * <p>
 * Phrase passed as task argument is split into jobs each taking one word. Then jobs are distributed among
 * grid nodes. Each node computes word length and returns result to master node where total phrase length
 * is calculated on reduce stage.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeTaskSplitExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite g = Ignition.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println("Compute task split example started.");

            // Execute task on the grid and wait for its completion.
            int cnt = g.compute().execute(CharacterCountTask.class, "Hello Grid Enabled World!");

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + cnt + "'.");
            System.out.println(">>> Check all nodes for output (this node is also part of the grid).");
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
         * @param gridSize Number of available grid nodes. Note that returned number of
         *      jobs can be less, equal or greater than this grid size.
         * @param arg Task execution argument. Can be {@code null}.
         * @return The list of child jobs.
         */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
            Collection<ComputeJob> jobs = new LinkedList<>();

            for (final String word : arg.split(" ")) {
                jobs.add(new ComputeJobAdapter() {
                    @Nullable @Override public Object execute() {
                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' on this node from grid job.");

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
