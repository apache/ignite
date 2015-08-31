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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.lang.IgniteRunnable;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Ignite cache distributed queue example. This example demonstrates {@code FIFO} unbounded
 * cache queue.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class IgniteQueueExample {
    /** Number of retries */
    private static final int RETRIES = 20;

    /** Queue instance. */
    private static IgniteQueue<String> queue;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Ignite queue example started.");

            // Make queue name.
            String queueName = UUID.randomUUID().toString();

            queue = initializeQueue(ignite, queueName);

            readFromQueue(ignite);

            writeToQueue(ignite);

            clearAndRemoveQueue();
        }

        System.out.println("Cache queue example finished.");
    }

    /**
     * Initialize queue.
     *
     * @param ignite Ignite.
     * @param queueName Name of queue.
     * @return Queue.
     * @throws IgniteException If execution failed.
     */
    private static IgniteQueue<String> initializeQueue(Ignite ignite, String queueName) throws IgniteException {
        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(PARTITIONED);

        // Initialize new FIFO queue.
        IgniteQueue<String> queue = ignite.queue(queueName, 0, colCfg);

        // Initialize queue items.
        // We will be use blocking operation and queue size must be appropriated.
        for (int i = 0; i < ignite.cluster().nodes().size() * RETRIES * 2; i++)
            queue.put(Integer.toString(i));

        System.out.println("Queue size after initializing: " + queue.size());

        return queue;
    }

    /**
     * Read items from head and tail of queue.
     *
     * @param ignite Ignite.
     * @throws IgniteException If failed.
     */
    private static void readFromQueue(Ignite ignite) throws IgniteException {
        final String queueName = queue.name();

        // Read queue items on each node.
        ignite.compute().broadcast(new QueueClosure(queueName, false));

        System.out.println("Queue size after reading [expected=0, actual=" + queue.size() + ']');
    }

    /**
     * Write items into queue.
     *
     * @param ignite Ignite.
     * @throws IgniteException If failed.
     */
    private static void writeToQueue(Ignite ignite) throws IgniteException {
        final String queueName = queue.name();

        // Write queue items on each node.
        ignite.compute().broadcast(new QueueClosure(queueName, true));

        System.out.println("Queue size after writing [expected=" + ignite.cluster().nodes().size() * RETRIES +
            ", actual=" + queue.size() + ']');

        System.out.println("Iterate over queue.");

        // Iterate over queue.
        for (String item : queue)
            System.out.println("Queue item: " + item);
    }

    /**
     * Clear and remove queue.
     *
     * @throws IgniteException If execution failed.
     */
    private static void clearAndRemoveQueue() throws IgniteException {
        System.out.println("Queue size before clearing: " + queue.size());

        // Clear queue.
        queue.clear();

        System.out.println("Queue size after clearing: " + queue.size());

        // Remove queue.
        queue.close();

        // Try to work with removed queue.
        try {
            queue.poll();
        }
        catch (IllegalStateException expected) {
            System.out.println("Expected exception - " + expected.getMessage());
        }
    }

    /**
     * Closure to populate or poll the queue.
     */
    private static class QueueClosure implements IgniteRunnable {
        /** Queue name. */
        private final String queueName;

        /** Flag indicating whether to put or poll. */
        private final boolean put;

        /**
         * @param queueName Queue name.
         * @param put Flag indicating whether to put or poll.
         */
        QueueClosure(String queueName, boolean put) {
            this.queueName = queueName;
            this.put = put;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteQueue<String> queue = Ignition.ignite().queue(queueName, 0, null);

            if (put) {
                UUID locId = Ignition.ignite().cluster().localNode().id();

                for (int i = 0; i < RETRIES; i++) {
                    String item = locId + "_" + Integer.toString(i);

                    queue.put(item);

                    System.out.println("Queue item has been added: " + item);
                }
            }
            else {
                // Take items from queue head.
                for (int i = 0; i < RETRIES; i++)
                    System.out.println("Queue item has been read from queue head: " + queue.take());

                // Take items from queue head once again.
                for (int i = 0; i < RETRIES; i++)
                    System.out.println("Queue item has been read from queue head: " + queue.poll());
            }
        }
    }
}