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

package org.apache.ignite.scalar.examples.datastructures

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.lang.IgniteRunnable
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{Ignite, IgniteException, IgniteQueue}

import java.util.UUID

import scala.collection.JavaConversions._

/**
 * Ignite cache distributed queue example. This example demonstrates `FIFO` unbounded
 * cache queue.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
class ScalarIgniteQueueExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"
    
    /** Number of retries */
    private val RETRIES = 20

    /** Queue instance. */
    private var queue: IgniteQueue[String] = null

    scalar(CONFIG) {
        println()
        println(">>> Ignite queue example started.")

        val queueName = UUID.randomUUID.toString

        queue = initializeQueue(ignite$, queueName)

        readFromQueue(ignite$)

        writeToQueue(ignite$)

        clearAndRemoveQueue()

        println("Cache queue example finished.")
    }

    /**
     * Initialize queue.
     *
     * @param ignite Ignite.
     * @param queueName Name of queue.
     * @return Queue.
     * @throws IgniteException If execution failed.
     */
    @throws(classOf[IgniteException])
    private def initializeQueue(ignite: Ignite, queueName: String): IgniteQueue[String] = {
        val queue = queue$[String](queueName)

        for (i <- 0 until (ignite.cluster.nodes.size * RETRIES * 2))
            queue.put(Integer.toString(i))

        println("Queue size after initializing: " + queue.size)

        queue
    }

    /**
     * Read items from head and tail of queue.
     *
     * @param ignite Ignite.
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def readFromQueue(ignite: Ignite) {
        val queueName = queue.name

        ignite.compute.broadcast(new QueueClosure(queueName, false))

        println("Queue size after reading [expected=0, actual=" + queue.size + ']')
    }

    /**
     * Write items into queue.
     *
     * @param ignite Ignite.
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def writeToQueue(ignite: Ignite) {
        val queueName = queue.name

        ignite.compute.broadcast(new QueueClosure(queueName, true))

        println("Queue size after writing [expected=" + ignite.cluster.nodes.size * RETRIES + ", actual=" + queue.size + ']')

        println("Iterate over queue.")

        for (item <- queue)
            println("Queue item: " + item)
    }

    /**
     * Clear and remove queue.
     *
     * @throws IgniteException If execution failed.
     */
    @throws(classOf[IgniteException])
    private def clearAndRemoveQueue() {
        println("Queue size before clearing: " + queue.size)

        queue.clear()

        println("Queue size after clearing: " + queue.size)

        queue.close()

        try {
            queue.poll
        }
        catch {
            case expected: IllegalStateException =>
                println("Expected exception - " + expected.getMessage)
        }
    }

    /**
     * Closure to populate or poll the queue.
     *
     * @param queueName Queue name.
     * @param put Flag indicating whether to put or poll.
     */
    private class QueueClosure(queueName: String, put: Boolean) extends IgniteRunnable {
        @impl def run() {
            val queue = queue$[String](queueName)

            if (put) {
                val locId = cluster$.localNode.id
                for (i <- 0 until RETRIES) {
                    val item = locId + "_" + Integer.toString(i)

                    queue.put(item)

                    println("Queue item has been added: " + item)
                }
            }
            else {
                for (i <- 0 until RETRIES)
                    println("Queue item has been read from queue head: " + queue.take)

                for (i <- 0 until RETRIES)
                    println("Queue item has been read from queue head: " + queue.poll)
            }
        }
    }
}

object ScalarIgniteQueueExampleStartup extends ScalarIgniteQueueExample
