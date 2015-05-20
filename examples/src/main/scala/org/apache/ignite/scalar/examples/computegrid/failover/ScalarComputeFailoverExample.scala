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

package org.apache.ignite.scalar.examples.computegrid.failover

import org.apache.ignite.IgniteLogger
import org.apache.ignite.compute.{ComputeJobFailoverException, ComputeTaskSession, ComputeTaskSessionFullSupport}
import org.apache.ignite.examples.ExamplesUtils
import org.apache.ignite.examples.computegrid.failover.ComputeFailoverNodeStartup
import org.apache.ignite.lang.{IgniteBiTuple, IgniteClosure}
import org.apache.ignite.resources.{LoggerResource, TaskSessionResource}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

/**
 * Demonstrates the usage of checkpoints in Ignite.
 * <p>
 * The example tries to compute phrase length. In order to mitigate possible node failures, intermediate
 * result is saved as as checkpoint after each job step.
 * <p>
 * Remote nodes must be started using [[ComputeFailoverNodeStartup]].
 */
object ScalarComputeFailoverExample extends App {
    scalar(ComputeFailoverNodeStartup.configuration()) {
        if (ExamplesUtils.checkMinTopologySize(cluster$, 2)) {
            println()
            println("Compute failover example started.")

            // Number of letters.
            val charCnt: Int = ignite$.compute.apply(new CheckPointJob, "Stage1 Stage2")

            println()
            println(">>> Finished executing fail-over example with checkpoints.")
            println(">>> Total number of characters in the phrase is '" + charCnt + "'.")
            println(">>> You should see exception stack trace from failed job on some node.")
            println(">>> Failed job will be failed over to another node.")
        }
    }
}

@ComputeTaskSessionFullSupport private final class CheckPointJob extends IgniteClosure[String, Integer] {
    /** Injected distributed task session. */
    @TaskSessionResource private var jobSes: ComputeTaskSession = null

    /** Injected ignite logger. */
    @LoggerResource private var log: IgniteLogger = null

    /** */
    private var state: IgniteBiTuple[Integer, Integer] = null

    /** */
    private var phrase: String = null

    /**
     * The job will check the checkpoint with key `'fail'` and if
     * it's `true` it will throw exception to simulate a failure.
     * Otherwise, it will execute enabled method.
     */
    def apply(phrase: String): Integer = {
        println()
        println(">>> Executing fail-over example job.")

        this.phrase = phrase

        val words = Seq(phrase.split(" "):_*)

        val cpKey = checkpointKey

        val state: IgniteBiTuple[Integer, Integer] = jobSes.loadCheckpoint(cpKey)

        var idx = 0

        var sum = 0

        if (state != null) {
            this.state = state

            idx = state.get1

            sum = state.get2
        }

        for (i <- idx until words.size) {
            sum += words(i).length

            this.state = new IgniteBiTuple[Integer, Integer](i + 1, sum)

            jobSes.saveCheckpoint(cpKey, this.state)

            if (i == 0) {
                println()
                println(">>> Job will be failed over to another node.")

                throw new ComputeJobFailoverException("Expected example job exception.")
            }
        }
        sum
    }

    /**
     * Make reasonably unique checkpoint key.
     *
     * @return Checkpoint key.
     */
    private def checkpointKey: String = {
        getClass.getName + '-' + phrase
    }
}
