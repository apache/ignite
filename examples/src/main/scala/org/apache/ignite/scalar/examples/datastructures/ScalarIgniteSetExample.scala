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

import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{Ignite, IgniteException, IgniteSet}

import scala.collection.JavaConversions._

/**
 * Ignite cache distributed set example.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat}` examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarIgniteSetExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Set instance. */
    private var set: IgniteSet[String] = null

    scalar(CONFIG) {
        println()
        println(">>> Ignite set example started.")

        val setName = UUID.randomUUID.toString

        set = initializeSet(ignite$, setName)

        writeToSet(ignite$)

        clearAndRemoveSet()

        println("Ignite set example finished.")
    }

    /**
     * Initialize set.
     *
     * @param ignite Ignite.
     * @param setName Name of set.
     * @return Set.
     * @throws IgniteException If execution failed.
     */
    @throws(classOf[IgniteException])
    private def initializeSet(ignite: Ignite, setName: String): IgniteSet[String] = {
        val set = set$[String](setName)

        for (i <- 0 until 10)
            set.add(Integer.toString(i))

        println("Set size after initializing: " + set.size)

        set
    }

    /**
     * Write items into set.
     *
     * @param ignite Ignite.
     * @throws IgniteException If failed.
     */
    @throws(classOf[IgniteException])
    private def writeToSet(ignite: Ignite) {
        val setName = set.name

        ignite.compute.broadcast(toRunnable(() => {
            val set = set$[String](setName)

            val locId = cluster$.localNode.id

            for (i <- 0 until 5) {
                val item = locId + "_" + Integer.toString(i)

                set.add(item)

                println("Set item has been added: " + item)
            }
        }))

        println("Set size after writing [expected=" + (10 + ignite.cluster.nodes.size * 5) + ", actual=" + set.size + ']')

        println("Iterate over set.")

        for (item <- set)
            println("Set item: " + item)

        if (!set.contains("0"))
            throw new RuntimeException("Set should contain '0' among its elements.")

        if (set.add("0"))
            throw new RuntimeException("Set should not allow duplicates.")

        if (!set.remove("0"))
            throw new RuntimeException("Set should correctly remove elements.")

        if (set.contains("0"))
            throw new RuntimeException("Set should not contain '0' among its elements.")

        if (!set.add("0"))
            throw new RuntimeException("Set should correctly add new elements.")
    }

    /**
     * Clear and remove set.
     *
     * @throws IgniteException If execution failed.
     */
    @throws(classOf[IgniteException])
    private def clearAndRemoveSet() {
        println("Set size before clearing: " + set.size)

        set.clear()

        println("Set size after clearing: " + set.size)

        set.close()

        println("Set was removed: " + set.removed)

        try {
            set.contains("1")
        }
        catch {
            case expected: IllegalStateException =>
                println("Expected exception - " + expected.getMessage)
        }
    }
}
