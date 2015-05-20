/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.messaging

import org.apache.ignite.examples.{ExampleNodeStartup, ExamplesUtils}
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.IgniteBiPredicate
import org.apache.ignite.resources.IgniteInstanceResource
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{Ignite, IgniteException, IgniteMessaging}

import java.util.UUID
import java.util.concurrent.CountDownLatch

/**
 * Example that demonstrates how to exchange messages between nodes. Use such
 * functionality for cases when you need to communicate to other nodes outside
 * of ignite task.
 * <p>
 * To run this example you must have at least one remote node started.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat}` examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start node
 * with `examples/config/example-ignite.xml` configuration.
 */
object ScalarMessagingExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Number of messages. */
    private val MESSAGES_NUM = 10

    /** Message topics. */
    private object TOPIC extends Enumeration {
        type TOPIC = Value
        val ORDERED, UNORDERED = Value
    }

    scalar(CONFIG) {
        val cluster = cluster$

        if (ExamplesUtils.checkMinTopologySize(cluster, 2)) {
            println()
            println(">>> Messaging example started.")

            // Group for remote nodes.
            val rmts = cluster.forRemotes

            // Listen for messages from remote nodes to make sure that they received all the messages.
            val msgCnt = rmts.nodes.size * MESSAGES_NUM

            val orderedLatch = new CountDownLatch(msgCnt)
            val unorderedLatch = new CountDownLatch(msgCnt)

            localListen(ignite$.message(cluster.forLocal), orderedLatch, unorderedLatch)

            // Register listeners on all cluster nodes.
            startListening(ignite$.message(rmts))

            // Send unordered messages to all remote nodes.
            for (i <- 0 until MESSAGES_NUM)
                ignite$.message(rmts).send(TOPIC.UNORDERED, Integer.toString(i))

            println(">>> Finished sending unordered messages.")

            // Send ordered messages to all remote nodes.
            for (i <- 0 until MESSAGES_NUM)
                ignite$.message(rmts).sendOrdered(TOPIC.ORDERED, Integer.toString(i), 0)

            println(">>> Finished sending ordered messages.")
            println(">>> Check output on all nodes for message printouts.")
            println(">>> Will wait for messages acknowledgements from all remote nodes.")

            orderedLatch.await()
            unorderedLatch.await()

            println(">>> Messaging example finished.")
        }
    }

    /**
     * Start listening to messages on remote cluster nodes.
     *
     * @param msg Ignite messaging.
     */
    private def startListening(msg: IgniteMessaging) {
        msg.remoteListen(TOPIC.ORDERED, new IgniteBiPredicate[UUID, String] {
            @IgniteInstanceResource private var ignite: Ignite = null

            @impl def apply(nodeId: UUID, msg: String): Boolean = {
                println("Received ordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']')

                try {
                    ignite.message(ignite.cluster.forNodeId(nodeId)).send(TOPIC.ORDERED, msg)
                }
                catch {
                    case e: IgniteException =>
                        e.printStackTrace()
                }

                true
            }
        })
        msg.remoteListen(TOPIC.UNORDERED, new IgniteBiPredicate[UUID, String] {
            @IgniteInstanceResource private var ignite: Ignite = null

            @impl def apply(nodeId: UUID, msg: String): Boolean = {
                println("Received unordered message [msg=" + msg + ", fromNodeId=" + nodeId + ']')

                try {
                    ignite.message(ignite.cluster.forNodeId(nodeId)).send(TOPIC.UNORDERED, msg)
                }
                catch {
                    case e: IgniteException =>
                        e.printStackTrace()
                }

                true
            }
        })
    }

    /**
     * Listen for messages from remote nodes.
     *
     * @param msg Ignite messaging.
     * @param orderedLatch Latch for ordered messages acks.
     * @param unorderedLatch Latch for unordered messages acks.
     */
    private def localListen(msg: IgniteMessaging, orderedLatch: CountDownLatch, unorderedLatch: CountDownLatch) {
        msg.localListen(TOPIC.ORDERED, new IgniteBiPredicate[UUID, String] {
            @impl def apply(nodeId: UUID, msg: String): Boolean = {
                orderedLatch.countDown()

                orderedLatch.getCount > 0
            }
        })
        msg.localListen(TOPIC.UNORDERED, new IgniteBiPredicate[UUID, String] {
            @impl def apply(nodeId: UUID, msg: String): Boolean = {
                unorderedLatch.countDown()

                unorderedLatch.getCount > 0
            }
        })
    }
}
