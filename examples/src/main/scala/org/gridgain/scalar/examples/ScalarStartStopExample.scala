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
import org.gridgain.grid.Ignite

/**
 * Demonstrates various starting and stopping ways of grid using Scalar.
 * <p>
 * Grid nodes in this example start with the default configuration file `'GRIDGAIN_HOME/config/default-config.xml'`.
 * <p>
 * Remote nodes should also be started with the default one: `'ggstart.{sh|bat}'`.
 */
object ScalarStartStopExample {
    /**
     * Example entry point. No arguments required.
     */
    def main(args: Array[String]) {
        way1()
        way2()
        way3()
    }

    /**
     * One way to start GridGain.
     */
    def way1() {
        scalar {
            println("Hurrah - I'm in the grid!")
            println("Local node ID is: " + grid$.cluster().localNode.id)
        }
    }

    /**
     * One way to start GridGain.
     */
    def way2() {
        scalar.start()

        try {
            println("Hurrah - I'm in the grid!")
        }
        finally {
            scalar.stop()
        }
    }

    /**
     * One way to start GridGain.
     */
    def way3() {
        scalar { g: Ignite =>
            println("Hurrah - local node ID is: " + g.cluster().localNode.id)
        }
    }
}
