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

import java.lang.{Long => JLong}
import java.util._

import org.apache.ignite.cache.CacheMode._
import org.apache.ignite.cache.affinity.AffinityKey
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.examples.model.{Person, Organization}
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{Ignite, IgniteCache}

import scala.collection.JavaConversions._

/**
 * Demonstrates cache ad-hoc queries with Scalar.
 * <p/>
 * Remote nodes should be started using `ExampleNodeStartup` which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheQueryExample {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Cache name. */
    private val NAME = ScalarCacheQueryExample.getClass.getSimpleName

    /**
     * Example entry point. No arguments required.
     *
     * @param args Command line arguments. None required.
     */
    def main(args: Array[String]) {
        scalar(CONFIG) {
            val cache = createCache$(NAME, indexedTypes = Seq(classOf[JLong], classOf[Organization],
                classOf[AffinityKey[_]], classOf[Person]))

            try {
                example(ignite$)
            }
            finally {
                cache.destroy()
            }
        }
    }

    /**
     * Runs the example.
     *
     * @param ignite Ignite instance to use.
     */
    private def example(ignite: Ignite) {
        // Populate cache.
        initialize()

        // Cache instance shortcut.
        val cache = mkCache[AffinityKey[JLong], Person]

        // Using distributed queries for partitioned cache and local queries for replicated cache.
        // Since in replicated caches data is available on all nodes, including local one,
        // it is enough to just query the local node.
        val prj = if (cache.getConfiguration(classOf[CacheConfiguration[AffinityKey[JLong], Person]]).getCacheMode == PARTITIONED)
            ignite.cluster().forRemotes()
        else
            ignite.cluster().forLocal()

        // Example for SQL-based querying employees based on salary ranges.
        // Gets all persons with 'salary > 1000'.
        print("People with salary more than 1000: ", cache.sql("salary > 1000").getAll.map(e => e.getValue))

        // Example for TEXT-based querying for a given string in people resumes.
        // Gets all persons with 'Bachelor' degree.
        print("People with Bachelor degree: ", cache.text("Bachelor").getAll.map(e => e.getValue))
    }

    /**
     * Gets instance of typed cache view to use.
     *
     * @return Cache to use.
     */
    private def mkCache[K, V]: IgniteCache[K, V] = cache$[K, V](NAME).get

    /**
     * Populates cache with test data.
     */
    private def initialize() {
        // Clean up caches on all nodes before run.
        cache$(NAME).get.clear()

        // Organization cache projection.
        val orgCache = mkCache[JLong, Organization]

        // Organizations.
        val org1 = new Organization("Ignite")
        val org2 = new Organization("Other")

        orgCache += (org1.id -> org1)
        orgCache += (org2.id -> org2)

        // Person cache projection.
        val prnCache = mkCache[AffinityKey[JLong], Person]

        // People.
        val p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.")
        val p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.")
        val p3 = new Person(org2, "John", "Smith", 1500, "John Smith has Bachelor Degree.")
        val p4 = new Person(org2, "Jane", "Smith", 2500, "Jane Smith has Master Degree.")

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        prnCache += (p1.key -> p1)
        prnCache += (p2.key -> p2)
        prnCache += (p3.key -> p3)
        prnCache += (p4.key -> p4)
    }

    /**
     * Prints object or collection of objects to standard out.
     *
     * @param msg Message to print before object is printed.
     * @param o Object to print, can be `Iterable`.
     */
    private def print(msg: String, o: Any) {
        assert(msg != null)
        assert(o != null)

        println(">>> " + msg)

        o match {
            case it: Iterable[Any] => it.foreach(e => println(">>>     " + e.toString))
            case _ => println(">>>     " + o.toString)
        }
    }
}
