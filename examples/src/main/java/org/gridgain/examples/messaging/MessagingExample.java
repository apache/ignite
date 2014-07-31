/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.messaging;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Example that demonstrates how to exchange messages between nodes. Use such
 * functionality for cases when you need to communicate to other nodes outside
 * of grid task.
 * <p>
 * To run this example you must have at least one remote node started.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public final class MessagingExample {
    /** Number of messages. */
    private static final int MESSAGES_NUM = 10;

    /** Message topics. */
    private enum TOPIC { ORDERED, UNORDERED }

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("examples/config/example-compute.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(g, 2))
                return;

            System.out.println();
            System.out.println(">>> Messaging example started.");

            // Projection for remote nodes.
            GridProjection rmtPrj = g.forRemotes();

            // Register listeners on all grid nodes.
            startListening(g, rmtPrj);

            // Send unordered messages to all remote nodes.
            for (int i = 0; i < MESSAGES_NUM; i++)
                rmtPrj.message().send(TOPIC.UNORDERED, Integer.toString(i));

            System.out.println(">>> Finished sending unordered messages.");

            // Send ordered messages to all remote nodes.
            for (int i = 0; i < MESSAGES_NUM; i++)
                rmtPrj.message().sendOrdered(TOPIC.ORDERED, Integer.toString(i), 0);

            System.out.println(">>> Finished sending ordered messages.");
            System.out.println(">>> Check output on all nodes for message printouts.");

            // Listen for messages from remote nodes to make sure that they received the messages.
            localListen(rmtPrj);
        }
    }

    /**
     * Start listening to messages on all grid nodes within passed in projection.
     *
     * @param g Grid.
     * @param prj Grid projection.
     * @throws GridException If failed.
     */
    private static void startListening(final Grid g, GridProjection prj) throws GridException {
        // Add ordered message listener.
        prj.message().remoteListen(TOPIC.ORDERED, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                System.out.println("Received ordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                try {
                    g.forNodeId(nodeId).message().send(TOPIC.ORDERED, msg);
                }
                catch (GridException e) {
                    e.printStackTrace();
                }

                return true; // Return true to continue listening.
            }
        }).get();

        // Add unordered message listener.
        prj.message().remoteListen(TOPIC.UNORDERED, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                System.out.println("Received unordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                try {
                    g.forNodeId(nodeId).message().send(TOPIC.UNORDERED, msg);
                }
                catch (GridException e) {
                    e.printStackTrace();
                }

                return true; // Return true to continue listening.
            }
        }).get();
    }

    /**
     * Listen for messages from remote nodes.
     *
     * @param prj Grid projection.
     * @throws Exception If failed.
     */
    private static void localListen(GridProjection prj) throws Exception {
        int nodesNum = prj.nodes().size() * MESSAGES_NUM;

        final CountDownLatch ordLatch = new CountDownLatch(nodesNum);

        prj.message().localListen(TOPIC.ORDERED, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                ordLatch.countDown();

                return true; // Return true to continue listening.
            }
        });

        final CountDownLatch unOrdLatch = new CountDownLatch(nodesNum);

        prj.message().localListen(TOPIC.UNORDERED, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                unOrdLatch.countDown();

                return true; // Return true to continue listening.
            }
        });

        ordLatch.await();
        unOrdLatch.await();
    }
}
