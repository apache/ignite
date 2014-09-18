/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.examples

import org.gridgain.scalar.scalar
import scalar._
import org.gridgain.grid._
import cache.affinity.GridCacheAffinityKeyMapped
import cache.GridCacheName
import org.jetbrains.annotations.Nullable
import java.util.concurrent.Callable

/**
 * Example of how to collocate computations and data in GridGain using
 * `GridCacheAffinityKeyMapped` annotation as opposed to direct API calls. This
 * example will first populate cache on some node where cache is available, and then
 * will send jobs to the nodes where keys reside and print out values for those
 * keys.
 *
 * Remote nodes should always be started with configuration file which includes
 * cache: `'ggstart.sh examples/config/example-cache.xml'`. Local node can
 * be started with or without cache.
 */
object ScalarCacheAffinityExample1 {
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

            var results = Map.empty[String, String]

            keys.foreach(key => {
                val res = grid$.call$(
                    new Callable[String] {
                        @GridCacheAffinityKeyMapped
                        def affinityKey(): String = key

                        @GridCacheName
                        def cacheName(): String = NAME

                        @Nullable def call: String = {
                            println(">>> Executing affinity job for key: " + key)

                            val cache = cache$[String, String](NAME)

                            if (!cache.isDefined) {
                                println(">>> Cache not found [nodeId=" + grid$.localNode.id +
                                    ", cacheName=" + NAME + ']')

                                "Error"
                            }
                            else
                                cache.get.peek(key)
                        }
                    },
                    null
                )

                results += (key -> res.head)
            })

            results.foreach(e => println(">>> Affinity job result for key '" + e._1 + "': " + e._2))
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
    private def populateCache(g: Grid, keys: Seq[String]) {
        var prj = g.forCache(NAME)

        // Give preference to local node.
        if (prj.nodes().contains(g.localNode))
            prj = g.forLocal()

        // Populate cache on some node (possibly this node) which has cache with given name started.
        prj.run$(() => {
            println(">>> Storing keys in cache: " + keys)

            val c = cache$[String, String](NAME).get

            keys.foreach(key => c += (key -> key.toLowerCase))
        }, null)
    }
}
