/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.messaging;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.messaging.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Demonstrates messaging with {@link GridMessagingListenActor} convenience adapter.
 * <p>
 * To run this example you must have at least one remote node started.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 */
public class MessagingPingPongListenActorExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        // Game is played over the default grid.
        try (Ignite g = GridGain.start("examples/config/example-compute.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(g.cluster(), 2))
                return;

            System.out.println();
            System.out.println(">>> Messaging ping-pong listen actor example started.");

            // Pick first remote node as a partner.
            Collection<ClusterNode> rmtNodes = g.cluster().forRemotes().nodes();

            ClusterGroup nodeB = g.cluster().forNode(rmtNodes.iterator().next());

            // Note that both nodeA and nodeB will always point to
            // same nodes regardless of whether they were implicitly
            // serialized and deserialized on another node as part of
            // anonymous closure's state during its remote execution.

            // Set up remote player.
            g.message(nodeB).remoteListen(null, new GridMessagingListenActor<String>() {
                @Override public void receive(UUID nodeId, String rcvMsg) throws GridException {
                    System.out.println(rcvMsg);

                    if ("PING".equals(rcvMsg))
                        respond("PONG");
                    else if ("STOP".equals(rcvMsg))
                        stop();
                }
            });

            int MAX_PLAYS = 10;

            final CountDownLatch cnt = new CountDownLatch(MAX_PLAYS);

            // Set up local player.
            g.message().localListen(null, new GridMessagingListenActor<String>() {
                @Override protected void receive(UUID nodeId, String rcvMsg) throws GridException {
                    System.out.println(rcvMsg);

                    if (cnt.getCount() == 1)
                        stop("STOP");
                    else if ("PONG".equals(rcvMsg))
                        respond("PING");

                    cnt.countDown();
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
