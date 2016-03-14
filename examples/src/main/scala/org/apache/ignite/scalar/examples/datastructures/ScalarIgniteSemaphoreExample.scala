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

package org.apache.ignite.scalar.examples.datastructures

import java.util.UUID

import org.apache.ignite.Ignition
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.lang.IgniteRunnable
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates a simple usage of distributed atomic long.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
class ScalarIgniteSemaphoreExample extends App {
    /** Number of items for each producer/consumer to produce/consume. */
    private val OPS_COUNT = 100

    /** Number of producers. */
    private val NUM_PRODUCERS = 10

    /** Number of consumers. */
    private val NUM_CONSUMERS = 10

    /** Synchronization semaphore name. */
    private val SEM_NAME = ScalarIgniteSemaphoreExampleStartup.getClass.getSimpleName

    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println
        println(">>> Cache atomic semaphore example started.")

        // Initialize semaphore.
        val syncSemaphore = semaphore$(SEM_NAME, 0, false, true)

        // Make name of semaphore.
        val semaphoreName = UUID.randomUUID.toString

        // Initialize semaphore.
        val semaphore = semaphore$(semaphoreName, 0, false, true)

        // Start consumers on all cluster nodes.
        for (i <- 0 until NUM_CONSUMERS)
            ignite$.compute.withAsync.run(new Consumer(semaphoreName))

        // Start producers on all cluster nodes.
        for (i <- 0 until NUM_PRODUCERS)
            ignite$.compute.withAsync.run(new Producer(semaphoreName))

        println("Master node is waiting for all other nodes to finish...")

        // Wait for everyone to finish.
        syncSemaphore.acquire(NUM_CONSUMERS + NUM_PRODUCERS)
    
        println()
        println("Finished semaphore example...")
        println("Check all nodes for output (this node is also part of the cluster).")
    }

    /**
      * Closure which simply waits on the latch on all nodes.
      *
      * @param semaphoreName Semaphore name.
      */
    private abstract class SemaphoreExampleClosure(semaphoreName: String) extends IgniteRunnable {}

    /**
      * Closure which simply signals the semaphore.
      *
      * @param semaphoreName Semaphore name.
      */
    private class Producer(semaphoreName: String) extends SemaphoreExampleClosure(semaphoreName) {
        /** @inheritdoc */
        def run() {
            val semaphore = semaphore$(semaphoreName, 0, true, true)

            for (i <- 0 until OPS_COUNT) {
                println("Producer [nodeId=" + Ignition.ignite.cluster.localNode.id + ", available=" + semaphore.availablePermits + ']')
                semaphore.release()
            }

            println("Producer finished [nodeId=" + Ignition.ignite.cluster.localNode.id + ']')

            val sem = semaphore$(SEM_NAME, 0, true, true)

            sem.release()
        }
    }

    /**
      * Closure which simply waits on semaphore.
      *
      * @param semaphoreName Semaphore name.
      */
    private class Consumer(semaphoreName: String) extends SemaphoreExampleClosure(semaphoreName) {
        /** @inheritdoc */
        def run() {
            val sem = semaphore$(semaphoreName, 0, true, true)

            for (i <- 0 until OPS_COUNT) {
                sem.acquire()

                println("Consumer [nodeId=" + Ignition.ignite.cluster.localNode.id + ", available=" + sem.availablePermits + ']')
            }

            println("Consumer finished [nodeId=" + Ignition.ignite.cluster.localNode.id + ']')

            val sync = semaphore$(SEM_NAME, 3, true, true)

            sync.release()
        }
    }
}

object ScalarIgniteSemaphoreExampleStartup extends ScalarIgniteSemaphoreExample
