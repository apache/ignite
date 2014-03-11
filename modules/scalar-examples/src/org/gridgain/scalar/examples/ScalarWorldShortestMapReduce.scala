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

/**
 * Shows the world's shortest MapReduce application that calculates non-space
 * length of the input string. This example works equally on one computer or
 * on thousands requiring no special configuration or deployment.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ggstart.{sh|bat} examples/config/example-compute.xml'`.
 */
object ScalarWorldShortestMapReduce extends App {
    scalar("examples/config/example-compute.xml") {
        val input = "World shortest mapreduce application"

        println("Non-space characters count: " +
            grid$.reduce$[Int, Int](for (w <- input.split(" ")) yield () => w.length, _.sum, null)
        )
    }
}
