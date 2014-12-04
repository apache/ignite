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

import org.apache.ignite.compute.{GridComputeTaskSplitAdapter, ComputeJob, GridComputeJobResult}
import org.gridgain.scalar.scalar
import scalar._
import collection.JavaConversions._
import org.gridgain.grid.compute._
import java.util

/**
 * Demonstrates use of full grid task API using Scalar. Note that using task-based
 * grid enabling gives you all the advanced features of GridGain such as custom topology
 * and collision resolution, custom failover, mapping, reduction, load balancing, etc.
 * As a trade off in such cases the more code needs to be written vs. simple closure execution.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ggstart.{sh|bat} examples/config/example-compute.xml'`.
 */
object ScalarTaskExample extends App {
    scalar("examples/config/example-compute.xml") {
        grid$.compute().execute(classOf[GridHelloWorld], "Hello Cloud World!")
    }

    /**
     * This task encapsulates the logic of MapReduce.
     */
    class GridHelloWorld extends GridComputeTaskSplitAdapter[String, Void] {
        def split(gridSize: Int, arg: String): java.util.Collection[_ <: ComputeJob] = {
            (for (w <- arg.split(" ")) yield toJob(() => println(w))).toSeq
        }

        def reduce(results: util.List[GridComputeJobResult]) = null
    }
}
