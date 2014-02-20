// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.GridClosureCallMode.*;
import static org.gridgain.grid.cache.datastructures.GridCacheQueueType.*;
import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Grid cache distributed queue example. This example demonstrates {@code PRIORITY}
 * unbounded cache queue.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/example-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridCacheQueuePriorityExample {
    /** Cache name. */
    //private static final String CACHE_NAME = "replicated";
    private static final String CACHE_NAME = "partitioned_tx";

    /** Number of retries */
    private static final int RETRIES = 5;

    /** Queue instance. */
    private static GridCacheQueue<SampleItem> queue;

    /**
     * Executes this example on the grid.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = GridGain.start("examples/config/example-cache.xml")) {
            print("Priority queue example started on nodes: " + g.nodes().size());

            // Make queue name.
            String queueName = UUID.randomUUID().toString();

            queue = initializeQueue(g, queueName);

            readFromQueue(g);

            writeToQueue(g);

            clearAndRemoveQueue(g);
        }

        print("Priority queue example finished.");
    }

    /**
     * Initialize queue.
     *
     * @param g Grid.
     * @param queueName Name of queue.
     * @return Queue.
     * @throws GridException If execution failed.
     */
    private static GridCacheQueue<SampleItem> initializeQueue(Grid g, String queueName) throws GridException {
        // Initialize new priority queue.
        GridCacheQueue<SampleItem> queue = g.cache(CACHE_NAME).dataStructures().
            queue(queueName, PRIORITY, 0, false, true);

        // Initialize queue items.
        // We will be use blocking operation and queue size must be appropriated.
        for (int i = 0; i < g.nodes().size() * RETRIES * 2; i++)
            queue.put(new SampleItem(i, g.localNode().id().toString()));

        print("Queue size after initializing: " + queue.size());

        return queue;
    }

    /**
     * Read items from head and tail of queue.
     *
     * @param g Grid.
     * @throws GridException If failed.
     */
    private static void readFromQueue(GridProjection g) throws GridException {
        final String queueName = queue.name();

        // Read queue items on each node.
        g.compute().run(BROADCAST, new QueueClosure(CACHE_NAME, queueName, false)).get();

        print("Queue size after reading [expected=0, actual=" + queue.size() + ']');
    }

    /**
     * Write items into queue.
     *
     * @param g Grid.
     * @throws GridException If failed.
     */
    private static void writeToQueue(GridProjection g) throws GridException {
        final String queueName = queue.name();

        // Write queue items on each node.
        g.compute().run(BROADCAST, new QueueClosure(CACHE_NAME, queueName, true)).get();

        print("Queue size after writing [expected=" + g.nodes().size() * RETRIES +
            ", actual=" + queue.size() + ']');

        print("Iterate over queue.");

        // Iterate over queue.
        for (SampleItem item : queue)
            print("Queue item: " + item);
    }

    /**
     * Clear and remove queue.
     *
     * @param g Grid.
     * @throws GridException If execution failed.
     */
    private static void clearAndRemoveQueue(Grid g) throws GridException {
        print("Queue size before clearing: " + queue.size());

        // Clear queue.
        queue.clear();

        print("Queue size after clearing: " + queue.size());

        // Remove queue from cache.
        g.cache(CACHE_NAME).dataStructures().removeQueue(queue.name());

        // Try to work with removed queue.
        try {
            queue.get();
        }
        catch (GridException expected) {
            print("Expected exception - " + expected.getMessage());
        }
    }

    /**
     * Prints out given object to standard out.
     *
     * @param o Object to print.
     */
    private static void print(Object o) {
        System.out.println(">>> " + o);
    }

    /**
     * Queue item class with priority field.
     *
     * @author @java.author
     * @version @java.version
     */
    private static class SampleItem implements Serializable {
        /** Priority field*/
        @GridCacheQueuePriority
        private final int pri;

        /** Node id */
        private final String nodeId;

        /**
         * @param pri Item priority.
         * @param nodeId Id of node where object was instantiated.
         */
        SampleItem(int pri, String nodeId) {
            this.pri = pri;
            this.nodeId = nodeId;
        }

        /**
         * @return Priority.
         */
        int priority() {
            return pri;
        }

        /**
         * @return Node id.
         */
        String nodeId() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SampleItem.class, this);
        }
    }

    /**
     * Closure to populate or poll the queue.
     */
    private static class QueueClosure extends GridRunnable {
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
                GridCacheQueue<SampleItem> queue = GridGain.grid().cache(cacheName).dataStructures().
                    queue(queueName, FIFO, 0, false, true);

                if (put) {
                    UUID locId = GridGain.grid().localNode().id();

                    for (int i = 0; i < RETRIES; i++) {
                        String item = locId + "_" + Integer.toString(i);

                        queue.put(new SampleItem(i, GridGain.grid().localNode().id().toString()));

                        print("Queue item has been added: " + item);
                    }
                }
                else {
                    // Take items from queue head.
                    for (int i = 0; i < RETRIES; i++)
                        print("Queue item has been read from queue head: " + queue.poll().toString());

                    // Take items from queue tail.
                    for (int i = 0; i < RETRIES; i++)
                        print("Queue item has been read from queue tail: " + queue.pollLast());
                }
            }
            catch (GridException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
