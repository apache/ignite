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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * This example demonstrates cache based reentrant lock.
 * <p>
 * Remote nodes should always be started with special configuration
 * file which enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node with {@code
 * examples/config/example-ignite.xml} configuration.
 */
public class IgniteLockExample {
    /** Number of items for each producer/consumer to produce/consume. */
    private static final int OPS_COUNT = 100;

    /** Number of producers. */
    private static final int NUM_PRODUCERS = 5;

    /** Number of consumers. */
    private static final int NUM_CONSUMERS = 5;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Name of the global resource. */
    private static final String QUEUE_ID = "queue";

    /** Name of the synchronization variable. */
    private static final String SYNC_NAME = "done";

    /** Name of the condition object. */
    private static final String NOT_FULL = "notFull";

    /** Name of the condition object. */
    private static final String NOT_EMPTY = "notEmpty";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache atomic reentrant lock example started.");

            // Make name of reentrant lock.
            final String reentrantLockName = UUID.randomUUID().toString();

            // Initialize lock.
            IgniteLock lock = ignite.reentrantLock(reentrantLockName, true, false, true);

            // Init distributed cache.
            IgniteCache<String, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

            // Init shared variable.
            cache.put(QUEUE_ID, 0);

            // Shared variable indicating number of jobs left to be completed.
            cache.put(SYNC_NAME, NUM_PRODUCERS + NUM_CONSUMERS);

            // Start consumers on all cluster nodes.
            for (int i = 0; i < NUM_CONSUMERS; i++)
                ignite.compute().runAsync(new Consumer(reentrantLockName));

            // Start producers on all cluster nodes.
            for (int i = 0; i < NUM_PRODUCERS; i++)
                ignite.compute().runAsync(new Producer(reentrantLockName));

            System.out.println("Master node is waiting for all other nodes to finish...");

            // Wait for everyone to finish.
            try {
                lock.lock();

                IgniteCondition notDone = lock.getOrCreateCondition(SYNC_NAME);

                int count = cache.get(SYNC_NAME);

                while(count > 0) {
                    notDone.await();

                    count = cache.get(SYNC_NAME);
                }
            }
            finally {
                lock.unlock();
            }
        }

        System.out.flush();
        System.out.println();
        System.out.println("Finished reentrant lock example...");
        System.out.println("Check all nodes for output (this node is also part of the cluster).");
    }

    /**
     * Closure which simply acquires reentrant lock.
     */
    private abstract static class ReentrantLockExampleClosure implements IgniteRunnable {
        /** Semaphore name. */
        protected final String reentrantLockName;

        /**
         * @param reentrantLockName Reentrant lock name.
         */
        ReentrantLockExampleClosure(String reentrantLockName) {
            this.reentrantLockName = reentrantLockName;
        }
    }

    /**
     * Closure which simulates producer.
     */
    private static class Producer extends ReentrantLockExampleClosure {
        /**
         * @param reentrantLockName Reentrant lock name.
         */
        public Producer(String reentrantLockName) {
            super(reentrantLockName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            System.out.println("Producer started. ");

            IgniteLock lock = Ignition.ignite().reentrantLock(reentrantLockName, true, false, true);

            // Condition to wait on when queue is full.
            IgniteCondition notFull = lock.getOrCreateCondition(NOT_FULL);

            // Signaled to wait on when queue is empty.
            IgniteCondition notEmpty = lock.getOrCreateCondition(NOT_EMPTY);

            // Signaled when job is done.
            IgniteCondition done = lock.getOrCreateCondition(SYNC_NAME);

            IgniteCache<String, Integer> cache = Ignition.ignite().cache(CACHE_NAME);

            for (int i = 0; i < OPS_COUNT; i++) {
                try {
                    lock.lock();

                    int val = cache.get(QUEUE_ID);

                    while(val >= 100){
                        System.out.println("Queue is full. Producer [nodeId=" + Ignition.ignite().cluster().localNode().id() +
                            " paused.");

                        notFull.await();

                        val = cache.get(QUEUE_ID);
                    }

                    val++;

                    System.out.println("Producer [nodeId=" + Ignition.ignite().cluster().localNode().id() +
                        ", available=" + val + ']');

                    cache.put(QUEUE_ID, val);

                    notEmpty.signalAll();
                }
                finally {
                    lock.unlock();
                }
            }

            System.out.println("Producer finished [nodeId=" + Ignition.ignite().cluster().localNode().id() + ']');

            try {
                lock.lock();

                int count = cache.get(SYNC_NAME);

                count--;

                cache.put(SYNC_NAME, count);

                // Signals the master thread.
                done.signal();
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     * Closure which simulates consumer.
     */
    private static class Consumer extends ReentrantLockExampleClosure {
        /**
         * @param reentrantLockName ReentrantLock name.
         */
        public Consumer(String reentrantLockName) {
            super(reentrantLockName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            System.out.println("Consumer started. ");

            Ignite g = Ignition.ignite();

            IgniteLock lock = g.reentrantLock(reentrantLockName, true, false, true);

            // Condition to wait on when queue is full.
            IgniteCondition notFull = lock.getOrCreateCondition(NOT_FULL);

            // Signaled to wait on when queue is empty.
            IgniteCondition notEmpty = lock.getOrCreateCondition(NOT_EMPTY);

            // Signaled when job is done.
            IgniteCondition done = lock.getOrCreateCondition(SYNC_NAME);

            IgniteCache<String, Integer> cache = g.cache(CACHE_NAME);

            for (int i = 0; i < OPS_COUNT; i++) {
                try {
                    lock.lock();

                    int val = cache.get(QUEUE_ID);

                    while (val <= 0) {
                        System.out.println("Queue is empty. Consumer [nodeId=" +
                            Ignition.ignite().cluster().localNode().id() + " paused.");

                        notEmpty.await();

                        val = cache.get(QUEUE_ID);
                    }

                    val--;

                    System.out.println("Consumer [nodeId=" + Ignition.ignite().cluster().localNode().id() +
                        ", available=" + val + ']');

                    cache.put(QUEUE_ID, val);

                    notFull.signalAll();
                }
                finally {
                    lock.unlock();
                }
            }

            System.out.println("Consumer finished [nodeId=" + Ignition.ignite().cluster().localNode().id() + ']');

            try {
                lock.lock();

                int count = cache.get(SYNC_NAME);

                count--;

                cache.put(SYNC_NAME, count);

                // Signals the master thread.
                done.signal();
            }
            finally {
                lock.unlock();
            }
        }
    }
}
