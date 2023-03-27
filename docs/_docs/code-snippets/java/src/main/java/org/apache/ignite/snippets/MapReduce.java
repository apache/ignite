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
package org.apache.ignite.snippets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.TaskSessionResource;

public class MapReduce {

    private static class CharacterCountTask extends ComputeTaskSplitAdapter<String, Integer> {
        // 1. Splits the received string into words
        // 2. Creates a child job for each word
        // 3. Sends the jobs to other nodes for processing.
        @Override
        public List<ComputeJob> split(int gridSize, String arg) {
            String[] words = arg.split(" ");

            List<ComputeJob> jobs = new ArrayList<>(words.length);

            for (final String word : words) {
                jobs.add(new ComputeJobAdapter() {
                    @Override
                    public Object execute() {
                        System.out.println(">>> This compute job calculates the length of the word '" + word + "'.");

                        // Return the number of letters in the word.
                        return word.length();
                    }
                });
            }

            return jobs;
        }

        @Override
        public Integer reduce(List<ComputeJobResult> results) {
            int sum = 0;

            for (ComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }

    void executeComputeTask() {

        // tag::execute-compute-task[]
        Ignite ignite = Ignition.start();

        IgniteCompute compute = ignite.compute();

        int count = compute.execute(new CharacterCountTask(), "Hello Grid Enabled World!");

        // end::execute-compute-task[]
    }

    // tag::session[]
    @ComputeTaskSessionFullSupport
    private static class TaskSessionAttributesTask extends ComputeTaskSplitAdapter<Object, Object> {

        @Override
        protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJob> jobs = new LinkedList<>();

            // Generate jobs by number of nodes in the grid.
            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter(arg) {
                    // Auto-injected task session.
                    @TaskSessionResource
                    private ComputeTaskSession ses;

                    // Auto-injected job context.
                    @JobContextResource
                    private ComputeJobContext jobCtx;

                    @Override
                    public Object execute() {
                        // Perform STEP1.
                        // ...

                        // Tell other jobs that STEP1 is complete.
                        ses.setAttribute(jobCtx.getJobId(), "STEP1");

                        // Wait for other jobs to complete STEP1.
                        for (ComputeJobSibling sibling : ses.getJobSiblings())
                            try {
                                ses.waitForAttribute(sibling.getJobId(), "STEP1", 0);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        // Move on to STEP2.
                        // ...

                        // tag::exclude[]
                        /*
                        // end::exclude[]
                        return ... 

                        // tag::exclude[]
                        */
                        return new Object();
                        // end::exclude[]
                    }
                });
            }
            return jobs;
        }

        @Override
        public Object reduce(List<ComputeJobResult> results) {
            // No-op.
            return null;
        }
        
        //tag::exclude[]
        //tag::failover[]
        @Override
        public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            IgniteException err = res.getException();

            if (err != null)
                return ComputeJobResultPolicy.FAILOVER;

            // If there is no exception, wait for all job results.
            return ComputeJobResultPolicy.WAIT;
        }
        //end::failover[]
        //end::exclude[]
    }

    // end::session[]
    
    

}
