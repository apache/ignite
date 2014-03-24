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
import java.util.{Random, Timer}
import org.gridgain.grid.product.{GridOnlyAvailableIn, GridProductEdition}
import org.gridgain.grid.cache.GridCache
import java.util
import org.gridgain.grid.dataload.GridDataLoadCacheUpdater
import java.util.Map.Entry

/**
 * Real time popular number counter. In order to run this example, you must start
 * at least one in-memory data grid nodes using command `ggstart.sh examples/config/example-cache.xml`
 * The counts are kept in cache on all remote nodes. Top `10` counts from each node are then grabbed to produce
 * an overall top `10` list within the grid.
 */
@GridOnlyAvailableIn(Array(GridProductEdition.DATA_GRID))
object ScalarPopularNumbersRealTimeExample extends App {
    private final val NUM_CNT = 10
    private final val RANGE = 1000
    private final val TOTAL_CNT = 100000

    // Will generate random numbers to get most popular ones.
    private final val RAND = new Random

    scalar("examples/config/example-cache.xml") {
        val timer = new Timer("query-worker")

        try {
            // Schedule queries to run every 3 seconds during ingestion phase.
            timer.schedule(timerTask(query(NUM_CNT)), 3000, 3000)

            // Ingest data & force one more run to get the final counts.
            ingest()
            query(NUM_CNT)

            // Clean up after ourselves.
            grid$.forCache("partitioned").bcastRun(() => grid$.cache("partitioned").clearAll(), null)
        }
        finally {
            timer.cancel()
        }
    }

    /**
     * Caches number counters in in-memory data grid.
     */
    def ingest() {
        // Set larger per-node buffer size since our state is relatively small.
        // Reduce parallel operations since we running the whole grid locally under heavy load.
        val ldr = dataLoader$[Int, Long]("partitioned", 2048)

        val f = (i: Long) => if (i == null) 1 else i + 1

        // Set custom updater to increment value for each key.
        ldr.updater(new GridDataLoadCacheUpdater[Int, Long] {
            def update(cache: GridCache[Int, Long],
                entries: util.Collection[Entry[Int, Long]]) = {
                import scala.collection.JavaConversions._

                for (e <- entries)
                    cache.transform(e.getKey, f)
            }
        })

        (0 until TOTAL_CNT) foreach (_ => ldr.addData(RAND.nextInt(RANGE), 1L))

        ldr.close(false)
    }

    /**
     * Queries a subset of most popular numbers from in-memory data grid.
     *
     * @param cnt Number of most popular numbers to return.
     */
    def query(cnt: Int) {
        cache$[Int, Long]("partitioned").get.sqlFields(clause = "select * from Long order by _val desc limit " + cnt).
            sortBy(_(1).asInstanceOf[Long]).reverse.take(cnt).foreach(println)

        println("------------------")
    }
}
