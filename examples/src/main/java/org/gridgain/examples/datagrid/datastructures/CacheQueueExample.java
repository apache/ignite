/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.apache.ignite.*;
import org.gridgain.examples.datagrid.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;

import java.util.*;

/**
 * Grid cache distributed queue example. This example demonstrates {@code FIFO} unbounded
 * cache queue.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CacheQueueExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned_tx";

    /** Number of retries */
    private static final int RETRIES = 20;

    /** Queue instance. */
    private static GridCacheQueue<String> queue;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Ignite g = GridGain.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache queue example started.");

            // Make queue name.
            String queueName = UUID.randomUUID().toString();

            queue = initializeQueue(g, queueName);

            readFromQueue(g);

            writeToQueue(g);

            clearAndRemoveQueue(g);
        }

        System.out.println("Cache queue example finished.");
    }

    /**
     * Initialize queue.
     *
     * @param g Grid.
     * @param queueName Name of queue.
     * @return Queue.
     * @throws GridException If execution failed.
     */
    private static GridCacheQueue<String> initializeQueue(Ignite g, String queueName) throws GridException {
        // Initialize new FIFO queue.
        GridCacheQueue<String> queue = g.cache(CACHE_NAME).dataStructures().queue(queueName, 0, false, true);

        // Initialize queue items.
        // We will be use blocking operation and queue size must be appropriated.
        for (int i = 0; i < g.cluster().nodes().size() * RETRIES * 2; i++)
            queue.put(Integer.toString(i));

        System.out.println("Queue size after initializing: " + queue.size());

        return queue;
    }

    /**
     * Read items from head and tail of queue.
     *
     * @param g Grid.
     * @throws GridException If failed.
     */
    private static void readFromQueue(Ignite g) throws GridException {
        final String queueName = queue.name();

        // Read queue items on each node.
        g.compute().run(new QueueClosure(CACHE_NAME, queueName, false));

        System.out.println("Queue size after reading [expected=0, actual=" + queue.size() + ']');
    }

    /**
     * Write items into queue.
     *
     * @param g Grid.
     * @throws GridException If failed.
     */
    private static void writeToQueue(Ignite g) throws GridException {
        final String queueName = queue.name();

        // Write queue items on each node.
        g.compute().run(new QueueClosure(CACHE_NAME, queueName, true));

        System.out.println("Queue size after writing [expected=" + g.cluster().nodes().size() * RETRIES +
            ", actual=" + queue.size() + ']');

        System.out.println("Iterate over queue.");

        // Iterate over queue.
        for (String item : queue)
            System.out.println("Queue item: " + item);
    }

    /**
     * Clear and remove queue.
     *
     * @param g Grid.
     * @throws GridException If execution failed.
     */
    private static void clearAndRemoveQueue(Ignite g) throws GridException {
        System.out.println("Queue size before clearing: " + queue.size());

        // Clear queue.
        queue.clear();

        System.out.println("Queue size after clearing: " + queue.size());

        // Remove queue from cache.
        g.cache(CACHE_NAME).dataStructures().removeQueue(queue.name());

        // Try to work with removed queue.
        try {
            queue.poll();
        }
        catch (GridRuntimeException expected) {
            System.out.println("Expected exception - " + expected.getMessage());
        }
    }

    /**
     * Closure to populate or poll the queue.
     */
    private static class QueueClosure implements IgniteRunnable {
        /** Cache name. */
        private final String cacheName;

        /** Queue name. */
        private final String queueName;

        /** Flag indicating whether to put or poll. */
        private final boolean put;

        /**
         * @param cacheName Cache name.
         * @param queueName Queue name.
         * @param put Flag indicating whether to put or poll.
         */
        QueueClosure(String cacheName, String queueName, boolean put) {
            this.cacheName = cacheName;
            this.queueName = queueName;
            this.put = put;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                GridCacheQueue<String> queue = GridGain.grid().cache(cacheName).dataStructures().
                    queue(queueName, 0, false, true);

                if (put) {
                    UUID locId = GridGain.grid().cluster().localNode().id();

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
            catch (GridException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
