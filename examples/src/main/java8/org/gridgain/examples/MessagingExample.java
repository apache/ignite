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

package org.gridgain.examples;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.examples.*;

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
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite g = Ignition.start("examples/config/example-compute.xml")) {
            if (g.nodes().size() < 2) {
                System.out.println();
                System.out.println(">>> Please start at least 2 grid nodes to run example.");
                System.out.println();

                return;
            }

            System.out.println();
            System.out.println(">>> Messaging example started.");

            // Projection for remote nodes.
            ClusterGroup rmtPrj = g.forRemotes();

            // Listen for messages from remote nodes to make sure that they received all the messages.
            int msgCnt = rmtPrj.nodes().size() * MESSAGES_NUM;

            CountDownLatch orderedLatch = new CountDownLatch(msgCnt);
            CountDownLatch unorderedLatch = new CountDownLatch(msgCnt);

            localListen(g.forLocal(), orderedLatch, unorderedLatch);

            // Register listeners on all grid nodes.
            startListening(rmtPrj);

            // Send unordered messages to all remote nodes.
            for (int i = 0; i < MESSAGES_NUM; i++)
                rmtPrj.message().send(TOPIC.UNORDERED, Integer.toString(i));

            System.out.println(">>> Finished sending unordered messages.");

            // Send ordered messages to all remote nodes.
            for (int i = 0; i < MESSAGES_NUM; i++)
                rmtPrj.message().sendOrdered(TOPIC.ORDERED, Integer.toString(i), 0);

            System.out.println(">>> Finished sending ordered messages.");
            System.out.println(">>> Check output on all nodes for message printouts.");
            System.out.println(">>> Will wait for messages acknowledgements from all remote nodes.");

            orderedLatch.await();
            unorderedLatch.await();

            System.out.println(">>> Messaging example finished.");
        }
    }

    /**
     * Start listening to messages on all grid nodes within passed in projection.
     *
     * @param prj Grid projection.
     * @throws IgniteCheckedException If failed.
     */
    private static void startListening(ClusterGroup prj) throws IgniteCheckedException {
        // Add ordered message listener.
        prj.message().remoteListen(TOPIC.ORDERED, (nodeId, msg) -> {
            System.out.println("Received ordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

            try {
                // Projection does not contain local node: GridProjection rmtPrj = g.forRemotes();
                // So, need to get projection for sender node through entire grid.
                prj.ignite().forNodeId(nodeId).message().send(TOPIC.ORDERED, msg);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }

            return true; // Return true to continue listening.
        }).get();

        // Add unordered message listener.
        prj.message().remoteListen(TOPIC.UNORDERED, (nodeId, msg) -> {
            System.out.println("Received unordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']');

            try {
                // Projection does not contain local node: GridProjection rmtPrj = g.forRemotes();
                // So, need to get projection for sender node through entire grid.
                prj.ignite().forNodeId(nodeId).message().send(TOPIC.UNORDERED, msg);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }

            return true; // Return true to continue listening.
        }).get();
    }

    /**
     * Listen for messages from remote nodes.
     *
     * @param prj Grid projection.
     * @param orderedLatch Latch for ordered messages acks.
     * @param unorderedLatch Latch for unordered messages acks.
     */
    private static void localListen(
        ClusterGroup prj,
        final CountDownLatch orderedLatch,
        final CountDownLatch unorderedLatch
    ) {
        prj.message().localListen(TOPIC.ORDERED, (nodeId, msg) -> {
            orderedLatch.countDown();

            // Return true to continue listening, false to stop.
            return orderedLatch.getCount() > 0;
        });

        prj.message().localListen(TOPIC.UNORDERED, (nodeId, msg) -> {
            unorderedLatch.countDown();

            // Return true to continue listening, false to stop.
            return unorderedLatch.getCount() > 0;
        });
    }
}
