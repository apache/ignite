/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.grid.*;

/**
 * Example that demonstrates how to exchange messages between nodes. Use such
 * functionality for cases when you need to communicate to other nodes outside
 * of grid task.
 * <p>
 * To run this example you must have at least one remote node started.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 */
public final class MessagingExample {
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
            if (g.nodes().size() < 2) {
                System.out.println();
                System.out.println(">>> Please start at least 2 grid nodes to run example.");
                System.out.println();

                return;
            }

            System.out.println();
            System.out.println(">>> Messaging example started.");

            // Projection for remote nodes.
            GridProjection rmtPrj = g.forRemotes();

            // Register listeners on all grid nodes.
            startListening(rmtPrj);

            // Send unordered messages to all remote nodes.
            for (int i = 0; i < 10; i++)
                rmtPrj.message().send(TOPIC.UNORDERED, Integer.toString(i));

            System.out.println(">>> Finished sending unordered messages.");

            // Send ordered messages to all remote nodes.
            for (int i = 0; i < 10; i++)
                rmtPrj.message().sendOrdered(TOPIC.ORDERED, Integer.toString(i), 0);

            System.out.println(">>> Finished sending ordered messages.");
            System.out.println(">>> Check output on all nodes for message printouts.");
        }
    }

    /**
     * Start listening to messages on all grid nodes within passed in projection.
     *
     * @param prj Grid projection.
     * @throws GridException If failed.
     */
    private static void startListening(GridProjection prj) throws GridException {
        // Add ordered message listener.
        prj.message().remoteListen(TOPIC.ORDERED, (nodeId, msg) -> {
            System.out.println("Received ordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

            return true; // Return true to continue listening.
        }).get();

        // Add unordered message listener.
        prj.message().remoteListen(TOPIC.UNORDERED, (nodeId, msg) -> {
            System.out.println("Received unordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

            return true; // Return true to continue listening.
        }).get();
    }
}
