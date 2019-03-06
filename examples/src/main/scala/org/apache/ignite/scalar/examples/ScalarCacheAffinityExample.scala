/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.scalar.examples

import org.apache.ignite.IgniteCache
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._

import scala.collection.JavaConversions._

/**
 * This example demonstrates the simplest code that populates the distributed cache
 * and co-locates simple closure execution with each key. The goal of this particular
 * example is to provide the simplest code example of this logic.
 * <p/>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p/>
 * Alternatively you can run `ExampleNodeStartup` in another JVM which will
 * start node with `examples/config/example-ignite.xml` configuration.
 */
object ScalarCacheAffinityExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Name of cache. */
    private val NAME = ScalarCacheAffinityExample.getClass.getSimpleName

    /** Number of keys. */
    private val KEY_CNT = 20

    /** Type alias. */
    type Cache = IgniteCache[Int, String]

    /*
     * Note that in case of `LOCAL` configuration,
     * since there is no distribution, values may come back as `nulls`.
     */
    scalar(CONFIG) {
        val cache = createCache$[Int, String](NAME)

        try {
            populate (cache)

            visitUsingAffinityRun(cache)

            visitUsingMapKeysToNodes(cache)
        }
        finally {
            cache.destroy()
        }
    }

    /**
     * Visits every in-memory data ignite entry on the remote node it resides by co-locating visiting
     * closure with the cache key.
     *
     * @param c Cache to use.
     */
    private def visitUsingAffinityRun(c: IgniteCache[Int, String]) {
        (0 until KEY_CNT).foreach (i =>
            ignite$.compute ().affinityRun (NAME, i,
                () => println ("Co-located using affinityRun [key= " + i + ", value=" + c.localPeek (i) + ']') )
        )
    }

    /**
     * Collocates jobs with keys they need to work.
     *
     * @param c Cache to use.
     */
    private def visitUsingMapKeysToNodes(c: IgniteCache[Int, String]) {
        val keys = (0 until KEY_CNT).toSeq

        // Map all keys to nodes.
        val mappings = ignite$.affinity(NAME).mapKeysToNodes(keys)

        mappings.foreach(mapping => {
            val node = mapping._1
            val mappedKeys = mapping._2

            if (node != null) {
                ignite$.cluster().forNode(node) *< (() => {
                    // Check cache without loading the value.
                    mappedKeys.foreach(key => println("Co-located using mapKeysToNodes [key= " + key +
                        ", value=" + c.localPeek(key) + ']'))
                }, null)
            }
        })
    }

    /**
     * Populates given cache.
     *
     * @param c Cache to populate.
     */
    private def populate(c: Cache) {
        (0 until KEY_CNT).foreach(i => c += (i -> i.toString))
    }
}
