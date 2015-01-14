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

package org.gridgain.scalar.examples

import org.apache.ignite.Ignite
import org.gridgain.scalar.scalar
import scalar._
import org.apache.ignite._
import org.gridgain.grid._
import collection.JavaConversions._
import scala.util.control.Breaks._

/**
 * Note that affinity routing is enabled for all caches.
 *
 * Remote nodes should always be started with configuration file which includes
 * cache: `'ggstart.sh examples/config/example-cache.xml'`. Local node can
 * be started with or without cache.
 */
object ScalarCacheAffinityExample2 {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-cache.xml" // Cache.

    /** Name of cache specified in spring configuration. */
    private val NAME = "partitioned"

    /**
     * Example entry point. No arguments required.
     *
     * Note that in case of `LOCAL` configuration,
     * since there is no distribution, values may come back as `nulls`.
     */
    def main(args: Array[String]) {
        scalar(CONFIG) {
            var keys = Seq.empty[String]

            ('A' to 'Z').foreach(keys :+= _.toString)

            populateCache(grid$, keys)

            // Map all keys to nodes.
            val mappings = grid$.cluster().mapKeysToNodes(NAME, keys)

            mappings.foreach(mapping => {
                val node = mapping._1
                val mappedKeys = mapping._2

                if (node != null) {
                    grid$.cluster().forNode(node) *< (() => {
                        breakable {
                            println(">>> Executing affinity job for keys: " + mappedKeys)

                            // Get cache.
                            val cache = cache$[String, String](NAME)

                            // If cache is not defined at this point then it means that
                            // job was not routed by affinity.
                            if (!cache.isDefined)
                                println(">>> Cache not found [nodeId=" + grid$.cluster().localNode().id() +
                                    ", cacheName=" + NAME + ']').^^

                            // Check cache without loading the value.
                            mappedKeys.foreach(key => println(">>> Peeked at: " + cache.get.peek(key)))
                        }
                    }, null)
                }
            })
        }
    }

    /**
     * Populates cache with given keys. This method accounts for the case when
     * cache is not started on local node. In that case a job which populates
     * the cache will be sent to the node where cache is started.
     *
     * @param g Grid.
     * @param keys Keys to populate.
     */
    private def populateCache(g: Ignite, keys: Seq[String]) {
        var prj = g.cluster().forCache(NAME)

        // Give preference to local node.
        if (prj.nodes().contains(g.cluster().localNode))
            prj = g.cluster().forLocal()

        // Populate cache on some node (possibly this node)
        // which has cache with given name started.
        prj.run$(
            () => {
                println(">>> Storing keys in cache: " + keys)

                val c = cache$[String, String](NAME).get

                keys.foreach(key => c += (key -> key.toLowerCase))
            },
            null
        )
    }
}
