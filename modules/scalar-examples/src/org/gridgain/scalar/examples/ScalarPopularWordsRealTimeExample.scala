// @scala.file.header

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
import java.io.File
import scala.io.{Codec, Source}
import java.util.Timer
import org.gridgain.grid.cache.GridCache
import java.util
import org.gridgain.grid.dataload.GridDataLoadCacheUpdater
import org.gridgain.grid.product.{GridOnlyAvailableIn, GridProductEdition}
import java.util.Map.Entry
import org.gridgain.grid.util.GridUtils

/**
 * Real time popular words counter. In order to run this example, you must start
 * at least one in-memory data grid nodes using command `ggstart.sh examples/config/example-cache-popularcounts.xml`
 * The counts are kept in cache on all remote nodes. Top `10` counts from each node are then grabbed to produce
 * an overall top `10` list within the grid.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(Array(GridProductEdition.DATA_GRID))
object ScalarPopularWordsRealTimeExample extends App {
    private final val WORDS_CNT = 10
    private final val BOOK_PATH = "examples/java/org/gridgain/examples/datagrid/loaddata/realtime/books"

    val ggHome = systemOrEnv("GRIDGAIN_HOME")

    if (ggHome == null)
        throw new RuntimeException("GRIDGAIN_HOME must be set to GridGain installation root.")

    val dir = GridUtils.resolveGridGainPath(BOOK_PATH)

    if (dir == null)
        sys.error("Input directory does not exist: " + BOOK_PATH)
    else
        scalar("examples/config/example-cache-popularcounts.xml") {
            // Data cache instance (default cache).
            val c = cache$[String, Int].get

            val timer = new Timer("query-worker")

            try {
                // Schedule word queries to run every 3 seconds.
                // Note that queries will run during ingestion phase.
                timer.schedule(timerTask(query(WORDS_CNT)), 2000, 2000)

                // Populate cache & force one more run to get the final counts.
                ingest(dir)
                query(WORDS_CNT)

                // Clean up after ourselves.
                c.globalClearAll()
            }
            finally {
                timer.cancel()
            }

            /**
             * Caches word counters in in-memory data grid.
             *
             * @param dir Directory where books are.
             */
            def ingest(dir: File) {
                // Set larger per-node buffer size since our state is relatively small.
                val ldr = dataLoader$[String, Int](null, 2048)

                val f = (i: Int) => if (i == null) 1 else i + 1

                // Set custom updater to increment value for each key.
                ldr.updater(new GridDataLoadCacheUpdater[String, Int] {
                    def update(cache: GridCache[String, Int],
                        entries: util.Collection[Entry[String, Int]]) = {
                        import scala.collection.JavaConversions._

                        for (e <- entries)
                            cache.transform(e.getKey, f)
                    }
                })

                try
                    for {
                        book <- dir.list()
                        line <- Source.fromFile(new File(dir, book))(Codec.UTF8).getLines()
                        word <- line.split("[^a-zA-Z0-9]") if word.length > 3
                    } ldr.addData(word, 1)
                finally
                    ldr.close(false) // Wait for data loader to complete.
            }

            /**
             * Queries a subset of most popular words from in-memory data grid.
             *
             * @param cnt Number of most popular words to return.
             */
            def query(cnt: Int) {
                c.sql("select * from Integer order by _val desc limit " + cnt).
                    toSeq.sortBy[Int](_._2).reverse.take(cnt).foreach(println)

                println("------------------")
            }
        }

    def systemOrEnv(name: String): String = {
        var v = System.getProperty(name)

        if (v == null)
            v = System.getenv(name)

        v
    }
}
