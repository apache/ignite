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

package org.apache.ignite.scalar.examples.computegrid

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Date, List => JavaList, Map => JavaMap, Queue => JavaQueue}

import org.apache.ignite.IgniteException
import org.apache.ignite.cluster.ClusterNode
import org.apache.ignite.compute._
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.lang.{GridFunc => F}
import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.resources.TaskContinuousMapperResource
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates usage of continuous mapper. With continuous mapper
 * it is possible to continue mapping jobs asynchronously even after
 * initial [[ComputeTask#map(List, Object)]] method completes.
 * <p>
 * String "Hello Continuous Mapper" is passed as an argument for execution
 * of [[ContinuousMapperTask]]. As an outcome, participating
 * nodes will print out a single word from the passed in string and return
 * number of characters in that word. However, to demonstrate continuous
 * mapping, next word will be mapped to a node only after the result from
 * previous word has been received.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start node
 * with `examples/config/example-ignite.xml` configuration.
 */
object ScalarComputeContinuousMapperExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    scalar(CONFIG) {
        println()
        println(">>> Compute continuous mapper example started.")

        val phraseLen = ignite$.compute execute(classOf[ContinuousMapperTask], "Hello Continuous Mapper")

        println()
        println(">>> Total number of characters in the phrase is '" + phraseLen + "'.")
    }
}

/**
 * This task demonstrates how continuous mapper is used. The passed in phrase
 * is split into multiple words and next word is sent out for processing only
 * when the result for the previous word was received.
 * <p>
 * Note that annotation [[ComputeTaskNoResultCache]] is optional and tells Ignite
 * not to accumulate results from individual jobs. In this example we increment
 * total character count directly in [[ComputeTaskNoResultCache#result(ComputeJobResult, List)]] method,
 * and therefore don't need to accumulate them be be processed at reduction step.
 */
@ComputeTaskNoResultCache private class ContinuousMapperTask extends ComputeTaskAdapter[String, Integer] {
    /** This field will be injected with task continuous mapper. */
    @TaskContinuousMapperResource private var mapper: ComputeTaskContinuousMapper = null

    /** Word queue. */
    private final val words: JavaQueue[String] = new ConcurrentLinkedQueue[String]

    /** Total character count. */
    private final val totalChrCnt = new AtomicInteger(0)

    @impl def map(subgrid: JavaList[ClusterNode], phrase: String): JavaMap[_ <: ComputeJob, ClusterNode] = {
        if (F.isEmpty(phrase))
            throw new IgniteException("Phrase is empty.")

        Collections.addAll[String](words, phrase.split(" "):_*)

        sendWord()

        null
    }

    override def result(res: ComputeJobResult, rcvd: JavaList[ComputeJobResult]): ComputeJobResultPolicy = {
        if (res.getException != null)
            super.result(res, rcvd)
        else {
            totalChrCnt.addAndGet(res.getData[Integer])

            sendWord()

            ComputeJobResultPolicy.WAIT
        }
    }

    @impl def reduce(results: JavaList[ComputeJobResult]): Integer = {
        totalChrCnt.get
    }

    /**
     * Sends next queued word to the next node implicitly selected by load balancer.
     */
    private def sendWord() {
        val word = words.poll

        if (word != null) {
            mapper.send(new ComputeJobAdapter(word) {
                @impl def execute() = {
                    println()
                    println(">>> Printing '" + word + "' from ignite job at time: " + new Date())

                    val cnt = word.length

                    try {
                        Thread.sleep(1000)
                    }
                    catch {
                        case ignored: InterruptedException => // No-op.
                    }

                    Integer.valueOf(cnt)
                }
            })
        }
    }
}
