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

import org.apache.ignite.dataload.IgniteDataLoadCacheUpdater
import org.gridgain.grid.GridException
import org.gridgain.grid.cache.GridCache

import java.util
import java.util.Timer
import java.util.Map.Entry

import org.gridgain.scalar.scalar
import org.gridgain.scalar.scalar._

import scala.util.Random

/**
 * Real time popular number counter.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `ggstart.sh examples/config/example-cache.xml`
 * <p>
 * Alternatively you can run [[org.gridgain.examples.datagrid.CacheNodeStartup]] in another JVM which will
 * start GridGain node with `examples/config/example-cache.xml` configuration.
 * <p>
 * The counts are kept in cache on all remote nodes. Top `10` counts from each node are then grabbed to produce
 * an overall top `10` list within the grid.
 */
object ScalarCachePopularNumbersExample extends App {
    /** Cache name. */
    private final val CACHE_NAME = "partitioned"

    /** Count of most popular numbers to retrieve from grid. */
    private final val POPULAR_NUMBERS_CNT = 10

    /** Range within which to generate numbers. */
    private final val RANGE = 1000

    /** Count of total numbers to generate. */
    private final val CNT = 100000

    scalar("examples/config/example-cache.xml") {
        println()
        println(">>> Cache popular numbers example started.")

        val prj = grid$.cluster().forCache(CACHE_NAME)

        if (prj.nodes().isEmpty)
            println("Grid does not have cache configured: " + CACHE_NAME);
        else {
            val popularNumbersQryTimer = new Timer("numbers-query-worker")

            try {
                // Schedule queries to run every 3 seconds during populates cache phase.
                popularNumbersQryTimer.schedule(timerTask(query(POPULAR_NUMBERS_CNT)), 3000, 3000)

                streamData()

                // Force one more run to get final counts.
                query(POPULAR_NUMBERS_CNT)

                // Clean up caches on all nodes after run.
                grid$.cluster().forCache(CACHE_NAME).bcastRun(() => grid$.cache(CACHE_NAME).clearAll(), null)
            }
            finally {
                popularNumbersQryTimer.cancel()
            }
        }
    }

    /**
     * Populates cache in real time with numbers and keeps count for every number.
     * @throws GridException If failed.
     */
    @throws[GridException]
    def streamData() {
        // Set larger per-node buffer size since our state is relatively small.
        // Reduce parallel operations since we running the whole grid locally under heavy load.
        val ldr = dataLoader$[Int, Long](CACHE_NAME, 2048)

        val f = (i: Long) => i + 1

        // Set custom updater to increment value for each key.
        ldr.updater(new IgniteDataLoadCacheUpdater[Int, Long] {
            def update(cache: GridCache[Int, Long], entries: util.Collection[Entry[Int, Long]]) = {
                import scala.collection.JavaConversions._

                for (e <- entries)
                    cache.transform(e.getKey, f)
            }
        })

        (0 until CNT) foreach (_ => ldr.addData(Random.nextInt(RANGE), 1L))

        ldr.close(false)
    }

    /**
     * Queries a subset of most popular numbers from in-memory data grid.
     *
     * @param cnt Number of most popular numbers to return.
     */
    def query(cnt: Int) {
        cache$[Int, Long](CACHE_NAME).get.
            sqlFields(clause = "select _key, _val from Long order by _val desc limit " + cnt).
            sortBy(_(1).asInstanceOf[Long]).reverse.take(cnt).foreach(println)

        println("------------------")
    }
}
