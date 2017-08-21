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

package org.apache.ignite.examples.datastructures;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * This example demonstrates cache based semaphore.
 * <p>
 * Remote nodes should always be started with special configuration
 * file which enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node with {@code
 * examples/config/example-ignite.xml} configuration.
 */
public class IgniteSemaphoreExample {
    /** Number of items for each producer/consumer to produce/consume. */
    private static final int OPS_COUNT = 100;

    /** Synchronization semaphore name. */
    private static final String SEM_NAME = IgniteSemaphoreExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            int nodeCount = ignite.cluster().forServers().nodes().size();

            // Number of producers should be equal to number of consumers.
            // This value should not exceed overall number of public thread pools in a cluster,
            // otherwise blocking consumer jobs can occupy all the threads leading to starvation.
            int jobCount = ignite.configuration().getPublicThreadPoolSize() * nodeCount / 2;

            System.out.println();
            System.out.println(">>> Cache atomic semaphore example started.");

            // Initialize semaphore.
            IgniteSemaphore syncSemaphore = ignite.semaphore(SEM_NAME, 0, false, true);

            // Make name of semaphore.
            final String semaphoreName = UUID.randomUUID().toString();

            // Initialize semaphore.
            IgniteSemaphore semaphore = ignite.semaphore(semaphoreName, 0, false, true);

            // Start consumers on all cluster nodes.
            for (int i = 0; i < jobCount; i++)
                ignite.compute().runAsync(new Consumer(semaphoreName));

            // Start producers on all cluster nodes.
            for (int i = 0; i < jobCount; i++)
                ignite.compute().runAsync(new Producer(semaphoreName));

            System.out.println("Master node is waiting for all other nodes to finish...");

            // Wait for everyone to finish.
            syncSemaphore.acquire(2 * jobCount);
        }

        System.out.flush();
        System.out.println();
        System.out.println("Finished semaphore example...");
        System.out.println("Check all nodes for output (this node is also part of the cluster).");
    }

    /**
     * Closure which simply waits on the latch on all nodes.
     */
    private abstract static class SemaphoreExampleClosure implements IgniteRunnable {
        /** Semaphore name. */
        protected final String semaphoreName;

        /**
         * @param semaphoreName Semaphore name.
         */
        SemaphoreExampleClosure(String semaphoreName) {
            this.semaphoreName = semaphoreName;
        }
    }

    /**
     * Closure which simply signals the semaphore.
     */
    private static class Producer extends SemaphoreExampleClosure {
        /**
         * @param semaphoreName Semaphore name.
         */
        public Producer(String semaphoreName) {
            super(semaphoreName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteSemaphore semaphore = Ignition.ignite().semaphore(semaphoreName, 0, true, true);

            for (int i = 0; i < OPS_COUNT; i++) {
                System.out.println("Producer [nodeId=" + Ignition.ignite().cluster().localNode().id() +
                    ", available=" + semaphore.availablePermits() + ']');

                // Signals others that shared resource is available.
                semaphore.release();
            }

            System.out.println("Producer finished [nodeId=" + Ignition.ignite().cluster().localNode().id() + ']');

            // Gets the syncing semaphore
            IgniteSemaphore sem = Ignition.ignite().semaphore(SEM_NAME, 0, true, true);

            // Signals the master thread
            sem.release();
        }
    }

    /**
     * Closure which simply waits on semaphore.
     */
    private static class Consumer extends SemaphoreExampleClosure {
        /**
         * @param semaphoreName Semaphore name.
         */
        public Consumer(String semaphoreName) {
            super(semaphoreName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteSemaphore sem = Ignition.ignite().semaphore(semaphoreName, 0, true, true);

            for (int i = 0; i < OPS_COUNT; i++) {
                // Block if no permits are available.
                sem.acquire();

                System.out.println("Consumer [nodeId=" + Ignition.ignite().cluster().localNode().id() +
                    ", available=" + sem.availablePermits() + ']');
            }

            System.out.println("Consumer finished [nodeId=" + Ignition.ignite().cluster().localNode().id() + ']');

            // Gets the syncing semaphore
            IgniteSemaphore sync = Ignition.ignite().semaphore(SEM_NAME, 0, true, true);

            // Signals the master thread.
            sync.release();
        }
    }
}
