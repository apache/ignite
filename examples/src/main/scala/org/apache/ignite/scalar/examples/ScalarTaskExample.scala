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

import org.apache.ignite.compute.{ComputeJob, ComputeJobResult, ComputeTaskSplitAdapter}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import java.util

import scala.collection.JavaConversions._

/**
 * Demonstrates use of full grid task API using Scalar. Note that using task-based
 * grid enabling gives you all the advanced features of GridGain such as custom topology
 * and collision resolution, custom failover, mapping, reduction, load balancing, etc.
 * As a trade off in such cases the more code needs to be written vs. simple closure execution.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-compute.xml'`.
 */
object ScalarTaskExample extends App {
    scalar("examples/config/example-compute.xml") {
        grid$.compute().execute(classOf[GridHelloWorld], "Hello Cloud World!")
    }

    /**
     * This task encapsulates the logic of MapReduce.
     */
    class GridHelloWorld extends ComputeTaskSplitAdapter[String, Void] {
        def split(gridSize: Int, arg: String): java.util.Collection[_ <: ComputeJob] = {
            (for (w <- arg.split(" ")) yield toJob(() => println(w))).toSeq
        }

        def reduce(results: util.List[ComputeJobResult]) = null
    }
}
