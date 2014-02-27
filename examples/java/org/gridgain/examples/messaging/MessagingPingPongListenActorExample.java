// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.messaging;

import org.gridgain.grid.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.examples.compute.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Demonstrates messaging with {@link GridMessagingListenActor} convenience adapter.
 * <p>
 * <h1 class="header">Starting Remote Nodes</h1>
 * To try this example you need to start at least one remote grid instance.
 * You can start as many as you like by executing the following script:
 * <pre class="snippet">{GRIDGAIN_HOME}/bin/ggstart.{bat|sh} examples/config/example-compute.xml</pre>
 * Once remote instances are started, you can execute this example from
 * Eclipse, IntelliJ IDEA, or NetBeans (and any other Java IDE) by simply hitting run
 * button. You will see that all nodes discover each other and
 * some of the nodes will participate in task execution (check node
 * output).
 * <p>
 * Alternatively you can run {@link ComputeNodeStartup} in another JVM which will start GridGain node
 * with {@code examples/config/example-compute.xml} configuration.
 *
 * @author @java.author
 * @version @java.version
 */
public class MessagingPingPongListenActorExample {
    /**
     * This example demonstrates simple protocol-based exchange in playing a ping-pong between
     * two nodes.
     *
     * @param args Command line arguments (none required).
     * @throws GridException Thrown in case of any errors.
     */
    public static void main(String[] args) throws GridException {
        // Game is played over the default grid.
        try (Grid g = GridGain.start("examples/config/example-compute.xml")) {
            // Gets collection of remote nodes.
            Collection<GridNode> rmtNodes = g.forRemotes().nodes();

            if (rmtNodes.size() < 1) {
                System.err.println("I need a partner to play a ping pong!");

                return;
            }

            // Pick first remote node as a partner.
            GridProjection nodeB = g.forNode(rmtNodes.iterator().next());

            // Note that both nodeA and nodeB will always point to
            // same nodes regardless of whether they were implicitly
            // serialized and deserialized on another node as part of
            // anonymous closure's state during its remote execution.

            // Set up remote player.
            nodeB.message().remoteListen(null, new GridMessagingListenActor<String>() {
                @Override public void receive(UUID nodeId, String rcvMsg) throws GridException {
                    System.out.println(rcvMsg);

                    if ("PING".equals(rcvMsg))
                        respond("PONG");
                    else if ("STOP".equals(rcvMsg))
                        stop();
                }
            }).get();

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
            nodeB.message().send(null, "PING");

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
