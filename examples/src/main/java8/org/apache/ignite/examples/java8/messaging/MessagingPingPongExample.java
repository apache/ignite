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

package org.apache.ignite.examples.java8.messaging;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;

/**
 * Demonstrates simple message exchange between local and remote nodes.
 * <p>
 * To run this example you must have at least one remote node started.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public class MessagingPingPongExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // Game is played over the default ignite.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(ignite.cluster(), 2))
                return;

            System.out.println();
            System.out.println(">>> Messaging ping-pong example started.");

            // Pick random remote node as a partner.
            ClusterGroup nodeB = ignite.cluster().forRemotes().forRandom();

            // Note that both nodeA and nodeB will always point to
            // same nodes regardless of whether they were implicitly
            // serialized and deserialized on another node as part of
            // anonymous closure's state during its remote execution.

            // Set up remote player.
            ignite.message(nodeB).remoteListen(null, (nodeId, rcvMsg) -> {
                System.out.println("Received message [msg=" + rcvMsg + ", sender=" + nodeId + ']');

                if ("PING".equals(rcvMsg)) {
                    ignite.message(ignite.cluster().forNodeId(nodeId)).send(null, "PONG");

                    return true; // Continue listening.
                }

                return false; // Unsubscribe.
            });

            int MAX_PLAYS = 10;

            final CountDownLatch cnt = new CountDownLatch(MAX_PLAYS);

            // Set up local player.
            ignite.message().localListen(null, (nodeId, rcvMsg) -> {
                System.out.println("Received message [msg=" + rcvMsg + ", sender=" + nodeId + ']');

                if (cnt.getCount() == 1) {
                    ignite.message(ignite.cluster().forNodeId(nodeId)).send(null, "STOP");

                    cnt.countDown();

                    return false; // Stop listening.
                }
                else if ("PONG".equals(rcvMsg))
                    ignite.message(ignite.cluster().forNodeId(nodeId)).send(null, "PING");
                else
                    throw new IgniteException("Received unexpected message: " + rcvMsg);

                cnt.countDown();

                return true; // Continue listening.
            });

            // Serve!
            ignite.message(nodeB).send(null, "PING");

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
