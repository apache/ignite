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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Example that demonstrates how to exchange messages between nodes. Use such
 * functionality for cases when you need to communicate to other nodes outside
 * of ignite task.
 * <p>
 * To run this example you must have at least one remote node started.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public final class MessagingExample {
    /** Number of messages. */
    private static final int MESSAGES_NUM = 10;

    /** Message topics. */
    private enum EXAMPLE_TOPIC { ORDERED, UNORDERED }

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.checkMinTopologySize(ignite.cluster(), 2))
                return;

            System.out.println();
            System.out.println(">>> Messaging example started.");

            // Group for remote nodes.
            ClusterGroup rmts = ignite.cluster().forRemotes();

            // Listen for messages from remote nodes to make sure that they received all the messages.
            int msgCnt = rmts.nodes().size() * MESSAGES_NUM;

            CountDownLatch orderedLatch = new CountDownLatch(msgCnt);
            CountDownLatch unorderedLatch = new CountDownLatch(msgCnt);

            localListen(ignite.message(ignite.cluster().forLocal()), orderedLatch, unorderedLatch);

            // Register listeners on all cluster nodes.
            startListening(ignite.message(rmts));

            // Send unordered messages to all remote nodes.
            for (int i = 0; i < MESSAGES_NUM; i++)
                ignite.message(rmts).send(EXAMPLE_TOPIC.UNORDERED, Integer.toString(i));

            System.out.println(">>> Finished sending unordered messages.");

            // Send ordered messages to all remote nodes.
            for (int i = 0; i < MESSAGES_NUM; i++)
                ignite.message(rmts).sendOrdered(EXAMPLE_TOPIC.ORDERED, Integer.toString(i), 0);

            System.out.println(">>> Finished sending ordered messages.");
            System.out.println(">>> Check output on all nodes for message printouts.");
            System.out.println(">>> Will wait for messages acknowledgements from all remote nodes.");

            orderedLatch.await();
            unorderedLatch.await();

            System.out.println(">>> Messaging example finished.");
        }
    }

    /**
     * Start listening to messages on remote cluster nodes.
     *
     * @param msg Ignite messaging.
     */
    private static void startListening(IgniteMessaging msg) {
        // Add ordered message listener.
        msg.remoteListen(EXAMPLE_TOPIC.ORDERED, new IgniteBiPredicate<UUID, String>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public boolean apply(UUID nodeId, String msg) {
                System.out.println("Received ordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                try {
                    ignite.message(ignite.cluster().forNodeId(nodeId)).send(EXAMPLE_TOPIC.ORDERED, msg);
                }
                catch (IgniteException e) {
                    e.printStackTrace();
                }

                return true; // Return true to continue listening.
            }
        });

        // Add unordered message listener.
        msg.remoteListen(EXAMPLE_TOPIC.UNORDERED, new IgniteBiPredicate<UUID, String>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public boolean apply(UUID nodeId, String msg) {
                System.out.println("Received unordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

                try {
                    ignite.message(ignite.cluster().forNodeId(nodeId)).send(EXAMPLE_TOPIC.UNORDERED, msg);
                }
                catch (IgniteException e) {
                    e.printStackTrace();
                }

                return true; // Return true to continue listening.
            }
        });
    }

    /**
     * Listen for messages from remote nodes.
     *
     * @param msg Ignite messaging.
     * @param orderedLatch Latch for ordered messages acks.
     * @param unorderedLatch Latch for unordered messages acks.
     */
    private static void localListen(
        IgniteMessaging msg,
        final CountDownLatch orderedLatch,
        final CountDownLatch unorderedLatch
    ) {
        msg.localListen(EXAMPLE_TOPIC.ORDERED, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                orderedLatch.countDown();

                // Return true to continue listening, false to stop.
                return orderedLatch.getCount() > 0;
            }
        });

        msg.localListen(EXAMPLE_TOPIC.UNORDERED, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID nodeId, String msg) {
                unorderedLatch.countDown();

                // Return true to continue listening, false to stop.
                return unorderedLatch.getCount() > 0;
            }
        });
    }
}