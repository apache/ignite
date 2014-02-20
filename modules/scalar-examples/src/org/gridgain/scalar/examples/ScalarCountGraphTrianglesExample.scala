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
import org.gridgain.grid._
import cache._
import resources._
import java.util.concurrent.Callable
import org.gridgain.grid.lang.GridOutClosure
import org.gridgain.grid.compute.GridComputeJobContext
import org.gridgain.grid.product.{GridOnlyAvailableIn, GridProductEdition}
import scala.collection.JavaConversions._

/**
 * Example shows how can GridGain be used to count triangles in undirectional graph.
 *
 * It first creates adjacency list graph representation and puts it in cache.
 * Adjacency list for each vertex is referenced by vertex number.
 *
 * Then computation and data collocation is used to process each vertex without
 * unnecessary data transfers. For each vertex we know all its neighbors (stored in
 * adjacency list), so we can generate a list of all possible edges that can complete
 * a triangle. If edge exists, triangle exists too (e.g., if vertex '3' is connected to
 * vertices '5' and '8' and edge (5 -> 8) exists, they form a triangle).
 *
 * Reduce step is trivial - we just summarize results for all vertices to get
 * total number of triangles in graph.
 *
 * Remote nodes should always be started with configuration file which includes
 * cache: `'ggstart.sh examples/config/example-cache.xml'`.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(Array(GridProductEdition.DATA_GRID))
object ScalarCountGraphTrianglesExample {
    /** Cache name. */
    private val CACHE_NAME = "partitioned"

    /**
     * @param args Command line arguments (none required).
     */
    def main(args: Array[String]) {
        scalar("examples/config/example-cache.xml") {
            // Create example graph.
            val vertices = createGraph(grid$)

            // Count triangles.
            val trianglesCnt = count(grid$, vertices)

            println(">>>")
            println(">>> Number of triangles: " + trianglesCnt)
            println(">>>")
        }
    }

    /**
     * Creates adjacency list graph representation in cache.
     * Created graph has `3` triangles.
     *
     * @param grid Grid.
     * @return Collection of vertices.
     * @throws GridException In case of error.
     */
    private def createGraph(grid: Grid): Seq[Int] = {
        assert(grid != null)

        val c = cache(grid)

        // Put adjacency list for each vertex into cache.
        c += (1 -> Seq(2, 3))
        c += (2 -> Seq(1, 3, 4))
        c += (3 -> Seq(1, 2))
        c += (4 -> Seq(2, 5, 7))
        c += (5 -> Seq(4, 6, 7, 8))
        c += (6 -> Seq(5))
        c += (7 -> Seq(4, 5, 8))
        c += (8 -> Seq(5, 7))

        // Return collection of vertices.
        Seq.range(1, 9)
    }

    /**
     * Counts triangles in graph.
     *
     * @param grid Grid.
     * @param vertices Collection of vertices.
     * @return Triangles quantity.
     * @throws GridException In case of error.
     */
    private def count(grid: Grid, vertices: Seq[Int]): Int = {
        assert(grid != null)

        // Run algorithm closure co-located with each vertex.
        // For each vertex we take all its neighbors (directly from adjacency list),
        // generate the list of all possible edges between neighbors and check their existence.
        // Existence of the edge means existence of triangle.
        val counts = grid.compute().affinityCall[Int](CACHE_NAME, toJavaCollection(vertices), new GridOutClosure[Callable[Int]] {
            override def apply(): Callable[Int] = {
                new GridOutClosure[Int] {
                    @GridJobContextResource
                    @transient
                    private val ctx: GridComputeJobContext = null

                    @GridInstanceResource
                    @transient
                    private val grid: Grid = null

                    private var futs: Seq[GridFuture[Boolean]] = null

                    override def apply(): Int = {
                        if (futs == null) {
                            // Get currently processed vertex from job context.
                            val keyVertex = ctx.affinityKey[Int]

                            println(">>> Processing vertex #" + keyVertex)

                            // Get neighbors of the vertex.
                            val list = cache(grid).peek(keyVertex)

                            // We used 'peek' method to get neighbors, but it should never
                            // be 'null', because computations are co-located with data.
                            // We never transfer data to computation node.
                            assert(list != null)

                            futs = Seq.empty[GridFuture[Boolean]]

                            // Loop through all neighbors.
                            list.foreach(i => {
                                // We include only neighbors that have larger number than current vertex.
                                // This is done to count edges only once (e.g., we count edge (3 -> 5), but
                                // not (5 -> 3), even if both of them are found in adjacency lists).
                                if (i > keyVertex) {
                                    // Nested loop to create all possible pairs of vertices (i.e. edges).
                                    list.foreach(j => {
                                        // Again, we count each edge only once.
                                        if (j > i) {
                                            // Check if edge (i -> j) exists. To do this, we run a closure on the
                                            // node that stores adjacency list for vertex 'i' and check whether
                                            // vertex 'j' is found among its neighbors.
                                            futs :+= grid.compute().affinityCall(CACHE_NAME, i, new GridOutClosure[Boolean] {
                                                @GridInstanceResource
                                                private val grid: Grid = null

                                                override def apply(): Boolean = {
                                                    val list = cache(grid).peek(i)

                                                    assert(list != null)

                                                    list.contains(j)
                                                }
                                            })
                                        }
                                    })
                                }
                            })

                            if (futs.nonEmpty) {
                                val lsnr = (f: GridFuture[Boolean]) => {
                                    // If all futures are done, resume the continuation.
                                    if (futs.forall(_.isDone))
                                        ctx.callcc()
                                }

                                // Attach listener to all futures.
                                futs.foreach(_.listenAsync(lsnr))

                                // Hold (suspend) job execution.
                                // It will be resumed in listener above via 'callcc()' call
                                // once all futures are done.
                                return ctx.holdcc()
                            }
                        }

                        futs.count(_.get)
                    }
                }
            }
        })

        // Reduce and return total number of triangles in graph.
        counts.get.sum
    }

    /**
     * Gets cache referenced by `CACHE_NAME`.
     *
     * @param grid Grid.
     * @return Cache.
     */
    private def cache(grid: Grid): GridCache[Int, Seq[Int]] = {
        assert (grid != null, "Grid cannot be null.")

        grid.cache[Int, Seq[Int]](CACHE_NAME)
    }
}
