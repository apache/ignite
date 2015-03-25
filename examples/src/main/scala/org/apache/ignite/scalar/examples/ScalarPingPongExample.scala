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

package org.apache.ignite.scalar.examples

import java.util.UUID
import java.util.concurrent.CountDownLatch

import org.apache.ignite.messaging.MessagingListenActor
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates simple protocol-based exchange in playing a ping-pong between
 * two nodes. It is analogous to `MessagingPingPongExample` on Java side.
 * <p/>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p/>
 * Alternatively you can run `ExampleNodeStartup` in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarPingPongExample extends App {
    scalar("examples/config/example-ignite.xml") {
        pingPong()
        //pingPong2()
    }

    /**
     * Implements Ping Pong example between local and remote node.
     */
    def pingPong() {
        val g = ignite$

        if (g.cluster().nodes().size < 2) {
            println(">>>")
            println(">>> I need a partner to play a ping pong!")
            println(">>>")

            return
        }
        else {
            // Pick first remote node as a partner.
            val nodeB = g.cluster().forNode(g.remoteNodes$().head)

            // Set up remote player: configure remote node 'rmt' to listen
            // for messages from local node 'loc'.
            g.message(nodeB).remoteListen(null, new MessagingListenActor[String]() {
                def receive(nodeId: UUID, msg: String) {
                    println(msg)

                    msg match {
                        case "PING" => respond("PONG")
                        case "STOP" => stop()
                    }
                }
            })

            val latch = new CountDownLatch(10)

            // Set up local player: configure local node 'loc'
            // to listen for messages from remote node 'rmt'.
            ignite$.message().localListen(null, new MessagingListenActor[String]() {
                def receive(nodeId: UUID, msg: String) {
                    println(msg)

                    if (latch.getCount == 1)
                        stop("STOP")
                    else // We know it's 'PONG'.
                        respond("PING")

                    latch.countDown()
                }
            })

            // Serve!
            nodeB.send$("PING", null)

            // Wait til the match is over.
            latch.await()
        }
    }

    /**
     * Implements Ping Pong example between two remote nodes.
     */
    def pingPong2() {
        val g = ignite$

        if (g.cluster().forRemotes().nodes().size() < 2) {
            println(">>>")
            println(">>> I need at least two remote nodes!")
            println(">>>")
        }
        else {
            // Pick two remote nodes.
            val n1 = g.cluster().forRemotes().head
            val n2 = g.cluster().forRemotes().tail.head

            val n1p = g.cluster().forNode(n1)
            val n2p = g.cluster().forNode(n2)

            // Configure remote node 'n1' to receive messages from 'n2'.
            g.message(n1p).remoteListen(null, new MessagingListenActor[String] {
                def receive(nid: UUID, msg: String) {
                    println(msg)

                    msg match {
                        case "PING" => respond("PONG")
                        case "STOP" => stop()
                    }
                }
            })

            // Configure remote node 'n2' to receive messages from 'n1'.
            g.message(n2p).remoteListen(null, new MessagingListenActor[String] {
                // Get local count down latch.
                private lazy val latch: CountDownLatch = g.cluster().nodeLocalMap().get("latch")

                def receive(nid: UUID, msg: String) {
                    println(msg)

                    latch.getCount match {
                        case 1 => stop("STOP")
                        case _ => respond("PING")
                    }

                    latch.countDown()
                }
            })

            // 1. Sets latch into node local storage so that local actor could use it.
            // 2. Sends first 'PING' to 'n1'.
            // 3. Waits until all messages are exchanged between two remote nodes.
            n2p.run$(() => {
                val latch = new CountDownLatch(10)

                g.cluster().nodeLocalMap[String, CountDownLatch].put("latch", latch)

                n1p.send$("PING", null)

                latch.await()
            }, null)
        }
    }
}
