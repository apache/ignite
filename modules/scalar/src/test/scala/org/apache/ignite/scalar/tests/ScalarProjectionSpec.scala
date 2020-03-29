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

package org.apache.ignite.scalar.tests

import org.apache.ignite.Ignition
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.messaging.MessagingListenActor
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import java.util.UUID

import scala.collection.JavaConversions._

/**
 * Scalar cache test.
 */
@RunWith(classOf[JUnitRunner])
class ScalarProjectionSpec extends FunSpec with ShouldMatchers with BeforeAndAfterAll {
    /**
     *
     */
    override def beforeAll() {
        Ignition.start(gridConfig("node-1", false))
        Ignition.start(gridConfig("node-2", true))
    }

    /**
     *
     */
    override def afterAll() {
        Ignition.stop("node-1", true)
        Ignition.stop("node-2", true)
    }

    /**
     *
     * @param name Ignite instance name.
     * @param shown Shown flag.
     */
    private def gridConfig(name: String, shown: Boolean): IgniteConfiguration = {
        val attrs: java.util.Map[String, Boolean] = Map[String, Boolean]("shown" -> shown)

        val cfg = new IgniteConfiguration

        cfg.setIgniteInstanceName(name)
        cfg.setUserAttributes(attrs)

        cfg
    }

    describe("ScalarProjectionPimp class") {
        it("should return all nodes") {
            scalar(gridConfig("node-scalar", true)) {
                assertResult(3)(ignite$("node-scalar").get.cluster().nodes().size)
            }
        }

        it("should return shown nodes") {
            scalar(gridConfig("node-scalar", true)) {
                assert(ignite$("node-scalar").get.nodes$(
                    (node: ClusterNode) => node.attribute[Boolean]("shown")).size == 2)
            }
        }

        it("should return all remote nodes") {
            scalar(gridConfig("node-scalar", true)) {
                assertResult(2)(ignite$("node-scalar").get.remoteNodes$().size)
            }
        }

        it("should return shown remote nodes") {
            scalar(gridConfig("node-scalar", true)) {
                assert(ignite$("node-scalar").get.remoteNodes$((node: ClusterNode) =>
                    node.attribute[Boolean]("shown")).size == 1)
            }
        }

        it("should correctly send messages") {
            scalar(gridConfig("node-scalar", true)) {
                ignite$("node-1").get.message().remoteListen(null, new MessagingListenActor[Any]() {
                    def receive(nodeId: UUID, msg: Any) {
                        println("node-1 received " + msg)
                    }
                })

                ignite$("node-2").get.message().remoteListen(null, new MessagingListenActor[Any]() {
                    def receive(nodeId: UUID, msg: Any) {
                        println("node-2 received " + msg)
                    }
                })

                ignite$("node-scalar").get !<("Message", null)
                ignite$("node-scalar").get !<(Seq("Message1", "Message2"), null)
            }
        }

        it("should correctly make calls") {
            scalar(gridConfig("node-scalar", true)) {
                println("CALL RESULT: " + ignite$("node-scalar").get #<(() => "Message", null))

                println("ASYNC CALL RESULT: " + ignite$("node-scalar").get.callAsync$[String](() => "Message", null).get)

                val call1: () => String = () => "Message1"
                val call2: () => String = () => "Message2"

                println("MULTIPLE CALL RESULT: " + ignite$("node-scalar").get #<(Seq(call1, call2), null))

                println("MULTIPLE ASYNC CALL RESULT: " +
                    (ignite$("node-scalar").get #?(Seq(call1, call2), null)).get)
            }
        }

        it("should correctly make runs") {
            scalar(gridConfig("node-scalar", true)) {
                ignite$("node-scalar").get *<(() => println("RUN RESULT: Message"), null)

                (ignite$("node-scalar").get *?(() => println("ASYNC RUN RESULT: Message"), null)).get

                val run1: () => Unit = () => println("RUN 1 RESULT: Message1")
                val run2: () => Unit = () => println("RUN 2 RESULT: Message2")

                ignite$("node-scalar").get *<(Seq(run1, run2), null)

                val runAsync1: () => Unit = () => println("ASYNC RUN 1 RESULT: Message1")
                val runAsync2: () => Unit = () => println("ASYNC RUN 2 RESULT: Message2")

                (ignite$("node-scalar").get *?(Seq(runAsync1, runAsync2), null)).get
            }
        }

        it("should correctly reduce") {
            scalar(gridConfig("node-scalar", true)) {
                val call1: () => Int = () => 15
                val call2: () => Int = () => 82

                assert(ignite$("node-scalar").get @<(Seq(call1, call2), (n: Seq[Int]) => n.sum, null) == 97)
                assert(ignite$("node-scalar").get.reduceAsync$(Seq(call1, call2), (
                    n: Seq[Int]) => n.sum, null).get == 97)
            }
        }
    }
}
