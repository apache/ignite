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

package org.apache.ignite.scalar.examples.computegrid

import org.apache.ignite.compute.{ComputeJob, ComputeJobAdapter, ComputeJobResult, ComputeTaskSplitAdapter}
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import org.jetbrains.annotations.Nullable

import java.util.{Collection => JavaCollection, LinkedList => JavaLinkedList, List => JavaList}

import scala.collection.JavaConversions._

/**
 * Demonstrates a simple use of Ignite with [[ComputeTaskSplitAdapter]].
 * <p>
 * Phrase passed as task argument is split into jobs each taking one word. Then jobs are distributed among
 * cluster nodes. Each node computes word length and returns result to master node where total phrase length
 * is calculated on reduce stage.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start node
 * with `examples/config/example-ignite.xml` configuration.
 */
object ScalarComputeTaskSplitExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println()
        println("Compute task split example started.")

        // Execute task on the cluster and wait for its completion.
        val cnt = ignite$.compute.execute(classOf[CharacterCountTask], "Hello Ignite Enabled World!")

        println()
        println(">>> Total number of characters in the phrase is '" + cnt + "'.")
        println(">>> Check all nodes for output (this node is also part of the cluster).")
    }

    /**
     * Task to count non-white-space characters in a phrase.
     */
    private class CharacterCountTask extends ComputeTaskSplitAdapter[String, Integer] {
        /**
         * Splits the received string to words, creates a child job for each word, and sends
         * these jobs to other nodes for processing. Each such job simply prints out the received word.
         *
         * @param clusterSize Number of available cluster nodes. Note that returned number of
         *                    jobs can be less, equal or greater than this cluster size.
         * @param arg Task execution argument. Can be { @code null}.
         * @return The list of child jobs.
         */
        protected def split(clusterSize: Int, arg: String): JavaCollection[_ <: ComputeJob] = {
            val jobs: JavaCollection[ComputeJob] = new JavaLinkedList[ComputeJob]

            for (word <- arg.split(" ")) {
                jobs.add(new ComputeJobAdapter {
                    @Nullable @impl def execute: Object = {
                        println()
                        println(">>> Printing '" + word + "' on this node from ignite job.")

                        word.length.asInstanceOf[Object]
                    }
                })
            }

            jobs
        }

        @impl def reduce(results: JavaList[ComputeJobResult]): Integer = {
            results.foldLeft(0)(_ + _.getData[Integer])
        }
    }
}
