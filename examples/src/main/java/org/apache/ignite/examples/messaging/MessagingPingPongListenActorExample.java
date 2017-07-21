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

package org.apache.ignite.examples.messaging;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;
import org.apache.ignite.messaging.MessagingListenActor;

/**
 * Demonstrates messaging with {@link MessagingListenActor} convenience adapter.
 * <p>
 * To run this example you must have at least one remote node started.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public class MessagingPingPongListenActorExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        // Game is played over the default ignite.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(ignite.cluster(), 2))
                return;

            System.out.println();
            System.out.println(">>> Messaging ping-pong listen actor example started.");

            // Pick first remote node as a partner.
            Collection<ClusterNode> rmtNodes = ignite.cluster().forRemotes().nodes();

            ClusterGroup nodeB = ignite.cluster().forNode(rmtNodes.iterator().next());

            // Note that both nodeA and nodeB will always point to
            // same nodes regardless of whether they were implicitly
            // serialized and deserialized on another node as part of
            // anonymous closure's state during its remote execution.

            // Set up remote player.
            ignite.message(nodeB).remoteListen(null, new MessagingListenActor<String>() {
                @Override public void receive(UUID nodeId, String rcvMsg) {
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
            ignite.message().localListen(null, new MessagingListenActor<String>() {
                @Override protected void receive(UUID nodeId, String rcvMsg) throws IgniteException {
                    System.out.println(rcvMsg);

                    if (cnt.getCount() == 1)
                        stop("STOP");
                    else if ("PONG".equals(rcvMsg))
                        respond("PING");

                    cnt.countDown();
                }
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