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
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Demonstrates simple message exchange between local and remote nodes.
 * <p>
 * To run this example you must have at least one remote node started.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class MessagingPingPongExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        // Game is played over the default grid.
        try (Grid g = GridGain.start("examples/config/example-compute.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(g.cluster(), 2))
                return;

            System.out.println();
            System.out.println(">>> Messaging ping-pong example started.");

            // Pick random remote node as a partner.
            GridProjection nodeB = g.cluster().forRemotes().forRandom();

            // Note that both nodeA and nodeB will always point to
            // same nodes regardless of whether they were implicitly
            // serialized and deserialized on another node as part of
            // anonymous closure's state during its remote execution.

            // Set up remote player.
            g.message(nodeB).remoteListen(null, new GridBiPredicate<UUID, String>() {
                /** This will be injected on node listener comes to. */
                @GridInstanceResource
                private Grid grid;

                @Override public boolean apply(UUID nodeId, String rcvMsg) {
                    System.out.println("Received message [msg=" + rcvMsg + ", sender=" + nodeId + ']');

                    try {
                        if ("PING".equals(rcvMsg)) {
                            grid.message(grid.cluster().forNodeId(nodeId)).send(null, "PONG");

                            return true; // Continue listening.
                        }

                        return false; // Unsubscribe.
                    }
                    catch (GridException e) {
                        throw new GridClosureException(e);
                    }
                }
            });

            int MAX_PLAYS = 10;

            final CountDownLatch cnt = new CountDownLatch(MAX_PLAYS);

            // Set up local player.
            g.message().localListen(null, new GridBiPredicate<UUID, String>() {
                @Override public boolean apply(UUID nodeId, String rcvMsg) {
                    System.out.println("Received message [msg=" + rcvMsg + ", sender=" + nodeId + ']');

                    try {
                        if (cnt.getCount() == 1) {
                            g.message(g.cluster().forNodeId(nodeId)).send(null, "STOP");

                            cnt.countDown();

                            return false; // Stop listening.
                        }
                        else if ("PONG".equals(rcvMsg))
                            g.message(g.cluster().forNodeId(nodeId)).send(null, "PING");
                        else
                            throw new RuntimeException("Received unexpected message: " + rcvMsg);

                        cnt.countDown();

                        return true; // Continue listening.
                    }
                    catch (GridException e) {
                        throw new GridClosureException(e);
                    }
                }
            });

            // Serve!
            g.message(nodeB).send(null, "PING");

            // Wait til the game is over.
            try {
                cnt.await();
            }
            catch (InterruptedException e) {
                System.err.println("Hm... let us finish the game!\n" + e);
            }
        }
    }
}
