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

import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import scala.math._

/**
 * This example calculates Pi number in parallel on the grid. Note that these few
 * lines of code work on one node, two nodes or hundreds of nodes without any changes
 * or any explicit deployment.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-compute.xml'`.
 */
object ScalarPiCalculationExample {
    /** Number of iterations per node. */
    private val N = 10000

    def main(args: Array[String]) {
        scalar("examples/config/example-compute.xml") {
            val jobs = for (i <- 0 until ignite$.cluster().nodes().size()) yield () => calcPi(i * N)

            println("Pi estimate: " + ignite$.reduce$[Double, Double](jobs, _.sum, null))
        }
    }

    /**
      * Calculates Pi range starting with given number.
      *
      * @param start Start the of the `{start, start + N}` range.
      * @return Range calculation.
      */
    def calcPi(start: Int): Double =
        // Nilakantha algorithm.
        ((max(start, 1) until (start + N)) map (i => 4.0 * (2 * (i % 2) - 1) / (2 * i) / (2 * i + 1) / (2 * i + 2)))
            .sum + (if (start == 0) 3 else 0)
}
