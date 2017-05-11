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

import org.apache.ignite.compute.ComputeJobContext
import org.apache.ignite.lang.{IgniteClosure, IgniteFuture}
import org.apache.ignite.resources.JobContextResource
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.jetbrains.annotations.Nullable

import java.math._
import java.util

/**
 * This example recursively calculates `Fibonacci` numbers on the ignite cluster. This is
 * a powerful design pattern which allows for creation of fully distributively recursive
 * (a.k.a. nested) tasks or closures with continuations. This example also shows
 * usage of `continuations`, which allows us to wait for results from remote nodes
 * without blocking threads.
 * <p/>
 * Note that because this example utilizes local node storage via `NodeLocal`,
 * it gets faster if you execute it multiple times, as the more you execute it,
 * the more values it will be cached on remote nodes.
 * <p/>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p/>
 * Alternatively you can run `ExampleNodeStartup` in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarContinuationExample {
    def main(args: Array[String]) {
        scalar("examples/config/example-ignite.xml") {
            // Calculate fibonacci for N.
            val N: Long = 100

            val thisNode = ignite$.cluster().localNode

            val start = System.currentTimeMillis

            // Group that excludes this node if others exists.
            val prj = if (ignite$.cluster().nodes().size() > 1) ignite$.cluster().forOthers(thisNode) else ignite$.cluster().forNode(thisNode)

            val fib = ignite$.compute(prj).apply(new FibonacciClosure(thisNode.id()), N)

            val duration = System.currentTimeMillis - start

            println(">>>")
            println(">>> Finished executing Fibonacci for '" + N + "' in " + duration + " ms.")
            println(">>> Fibonacci sequence for input number '" + N + "' is '" + fib + "'.")
            println(">>> You should see prints out every recursive Fibonacci execution on cluster nodes.")
            println(">>> Check remote nodes for output.")
            println(">>>")
        }
    }
}

/**
 * Closure to execute.
 *
 * @param excludeNodeId Node to exclude from execution if there are more then 1 node in cluster.
 */
class FibonacciClosure (
    private[this] val excludeNodeId: util.UUID
) extends IgniteClosure[Long, BigInteger] {
    // These fields must be *transient* so they do not get
    // serialized and sent to remote nodes.
    // However, these fields will be preserved locally while
    // this closure is being "held", i.e. while it is suspended
    // and is waiting to be continued.
    @transient private var fut1, fut2: IgniteFuture[BigInteger] = null

    // Auto-inject job context.
    @JobContextResource
    private val jobCtx: ComputeJobContext = null

    @Nullable override def apply(num: Long): BigInteger = {
        if (fut1 == null || fut2 == null) {
            println(">>> Starting fibonacci execution for number: " + num)

            // Make sure n is not negative.
            val n = math.abs(num)

            val g = ignite$

            if (n <= 2)
                return if (n == 0)
                    BigInteger.ZERO
                else
                    BigInteger.ONE

            // Get properly typed node-local storage.
            val store = g.cluster().nodeLocalMap[Long, IgniteFuture[BigInteger]]()

            // Check if value is cached in node-local store first.
            fut1 = store.get(n - 1)
            fut2 = store.get(n - 2)

            val excludeNode = ignite$.cluster().node(excludeNodeId)

            // Group that excludes node with id passed in constructor if others exists.
            val prj = if (ignite$.cluster().nodes().size() > 1) ignite$.cluster().forOthers(excludeNode) else ignite$.cluster().forNode(excludeNode)

            val comp = ignite$.compute(prj)

            // If future is not cached in node-local store, cache it.
            // Note recursive execution!
            if (fut1 == null) {
                val futVal = comp.applyAsync(new FibonacciClosure(excludeNodeId), n - 1)

                fut1 = store.putIfAbsent(n - 1, futVal)

                if (fut1 == null)
                    fut1 = futVal
            }

            // If future is not cached in node-local store, cache it.
            if (fut2 == null) {
                val futVal = comp.applyAsync(new FibonacciClosure(excludeNodeId), n - 2)

                fut2 = store.putIfAbsent(n - 2, futVal)

                if (fut2 == null)
                    fut2 = futVal
            }

            // If futures are not done, then wait asynchronously for the result
            if (!fut1.isDone || !fut2.isDone) {
                val lsnr = (fut: IgniteFuture[BigInteger]) => {
                    // This method will be called twice, once for each future.
                    // On the second call - we have to have both futures to be done
                    // - therefore we can call the continuation.
                    if (fut1.isDone && fut2.isDone)
                        jobCtx.callcc() // Resume job execution.
                }

                // Hold (suspend) job execution.
                // It will be resumed in listener above via 'callcc()' call
                // once both futures are done.
                jobCtx.holdcc()

                // Attach the same listener to both futures.
                fut1.listen(lsnr)
                fut2.listen(lsnr)

                return null
            }
        }

        assert(fut1.isDone && fut2.isDone)

        // Return cached results.
        fut1.get.add(fut2.get)
    }
}
