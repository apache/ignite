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
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;

public class JobScheduling {

    void fifo() {
        // tag::fifo[]
        FifoQueueCollisionSpi colSpi = new FifoQueueCollisionSpi();

        // Execute jobs sequentially, one at a time,
        // by setting parallel job number to 1.
        colSpi.setParallelJobsNumber(1);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default collision SPI.
        cfg.setCollisionSpi(colSpi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);

        // end::fifo[]
        ignite.close();
    }

    void priority() {
        // tag::priority[]
        PriorityQueueCollisionSpi colSpi = new PriorityQueueCollisionSpi();

        // Change the parallel job number if needed.
        // Default is number of cores times 2.
        colSpi.setParallelJobsNumber(5);

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override default collision SPI.
        cfg.setCollisionSpi(colSpi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);

        // end::priority[]
        ignite.close();
    }
    
    //tag::task-priority[]
    public class MyUrgentTask extends ComputeTaskSplitAdapter<Object, Object> {
        // Auto-injected task session.
        @TaskSessionResource
        private ComputeTaskSession taskSes = null;

        @Override
        protected Collection<ComputeJob> split(int gridSize, Object arg) {
            // Set high task priority.
            taskSes.setAttribute("grid.task.priority", 10);

            List<ComputeJob> jobs = new ArrayList<>(gridSize);

            for (int i = 1; i <= gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {

                    @Override
                    public Object execute() throws IgniteException {

                        //your implementation goes here

                        return null;
                    }
                });
            }

            // These jobs will be executed with higher priority.
            return jobs;
        }

        @Override
        public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    //end::task-priority[]

    
    public static void main(String[] args) {
       JobScheduling  js = new JobScheduling();
       js.fifo();
       js.priority();
    }
}
