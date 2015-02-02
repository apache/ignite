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
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.resources.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Demonstrates usage of continuous mapper. With continuous mapper
 * it is possible to continue mapping jobs asynchronously even after
 * initial {@link org.apache.ignite.compute.ComputeTask#map(List, Object)} method completes.
 * <p>
 * String "Hello Continuous Mapper" is passed as an argument for execution
 * of {@link GridContinuousMapperTask}. As an outcome, participating
 * nodes will print out a single word from the passed in string and return
 * number of characters in that word. However, to demonstrate continuous
 * mapping, next word will be mapped to a node only after the result from
 * previous word has been received.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class ComputeContinuousMapperExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        System.out.println();
        System.out.println(">>> Compute continuous mapper example started.");

        try (Ignite ignite = Ignition.start("examples/config/example-compute.xml")) {
            int phraseLen = ignite.compute().execute(GridContinuousMapperTask.class, "Hello Continuous Mapper");

            System.out.println();
            System.out.println(">>> Total number of characters in the phrase is '" + phraseLen + "'.");
        }
    }

    /**
     * This task demonstrates how continuous mapper is used. The passed in phrase
     * is split into multiple words and next word is sent out for processing only
     * when the result for the previous word was received.
     * <p>
     * Note that annotation {@link org.apache.ignite.compute.ComputeTaskNoResultCache} is optional and tells Ignite
     * not to accumulate results from individual jobs. In this example we increment
     * total character count directly in {@link #result(org.apache.ignite.compute.ComputeJobResult, List)} method,
     * and therefore don't need to accumulate them be be processed at reduction step.
     */
    @ComputeTaskNoResultCache
    private static class GridContinuousMapperTask extends ComputeTaskAdapter<String, Integer> {
        /** This field will be injected with task continuous mapper. */
        @IgniteTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** Word queue. */
        private final Queue<String> words = new ConcurrentLinkedQueue<>();

        /** Total character count. */
        private final AtomicInteger totalChrCnt = new AtomicInteger(0);

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> grid, String phrase)
            throws IgniteCheckedException {
            if (phrase == null || phrase.isEmpty())
                throw new IgniteCheckedException("Phrase is empty.");

            // Populate word queue.
            Collections.addAll(words, phrase.split(" "));

            // Sends first word.
            sendWord();

            // Since we have sent at least one job, we are allowed to return
            // 'null' from map method.
            return null;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
            throws IgniteCheckedException {
            // If there is an error, fail-over to another node.
            if (res.getException() != null)
                return super.result(res, rcvd);

            // Add result to total character count.
            totalChrCnt.addAndGet(res.<Integer>getData());

            sendWord();

            // If next word was sent, keep waiting, otherwise work queue is empty and we reduce.
            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return totalChrCnt.get();
        }

        /**
         * Sends next queued word to the next node implicitly selected by load balancer.
         *
         * @throws IgniteCheckedException If sending of a word failed.
         */
        private void sendWord() throws IgniteCheckedException {
            // Remove first word from the queue.
            String word = words.poll();

            if (word != null) {
                // Map next word.
                mapper.send(new ComputeJobAdapter(word) {
                    @Override public Object execute() {
                        String word = argument(0);

                        System.out.println();
                        System.out.println(">>> Printing '" + word + "' from grid job at time: " + new Date());

                        int cnt = word.length();

                        // Sleep for some time so it will be visually noticeable that
                        // jobs are executed sequentially.
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException ignored) {
                            // No-op.
                        }

                        return cnt;
                    }
                });
            }
        }
    }
}
