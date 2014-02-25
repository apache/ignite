// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.messaging;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.examples.compute.*;

import java.util.*;

/**
 * Example that demonstrates how to exchange messages between nodes. Use such
 * functionality for cases when you need to communicate to other nodes outside
 * of grid task. In such cases all your message classes must be in system
 * class path as they will not be peer-loaded.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-default.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-default.xml} configuration.
 *
 * @author @java.author
 * @version @java.version
 */
public final class MessagingExample {
    /** An ordered message timeout. */
    private static final long MSG_TIMEOUT = 15000;

    /** Message topics. */
    private enum TOPIC { ORDERED, UNORDERED }

    /**
     * Shows several use-cases for ordered and unordered messages.
     *
     * @param args Command line arguments, none required but if provided
     *      first one should point to the Spring XML configuration file. See
     *      {@code "examples/config/"} for configuration file examples.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Grid g = GridGain.start("examples/config/example-default.xml")) {
            if (g.nodes().size() == 1) {
                System.out.println(">>> Need at least 2 nodes to demonstrate messaging.");

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

            System.out.println();
            System.out.println(">>> Finished sending unordered messages.");

            // Send unordered messages to all remote nodes.
            for (int i = 0; i < 10; i++)
                rmtPrj.message().send(TOPIC.ORDERED, Integer.toString(i));

            System.out.println();
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
        prj.message().remoteListen(TOPIC.ORDERED, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                System.out.println("Received ordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                return true; // Return true to continue listening.
            }
        }).get();

        // Add unordered message listener.
        prj.message().remoteListen(TOPIC.UNORDERED, new GridBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                System.out.println("Received unordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                return true; // Return true to continue listening.
            }
        }).get();
    }
}
