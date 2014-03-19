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
 * Demonstrates a cron-based `Runnable` execution scheduling.
 * Test runnable object broadcasts a phrase to all grid nodes every minute
 * three times with initial scheduling delay equal to five seconds.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ggstart.{sh|bat} examples/config/example-compute.xml'`.
 */
object ScalarScheduleExample extends App {
    scalar("examples/config/example-compute.xml") {
        println()
        println("Compute schedule example started.")

        val g = grid$

        var invocations = 0

        // Schedule callable that returns incremented value each time.
        val fut = grid$.scheduleLocalCall(
            () => {
                invocations += 1

                g.bcastRun(() => {
                    println()
                    println("Howdy! :)")
                }, null)

                invocations
            },
            "{5, 3} * * * * *" // Cron expression.
        )

        while (!fut.isDone)
            println(">>> Invocation #: " + fut.get)

        // Prints.
        println()
        println(">>> Schedule future is done and has been unscheduled.")
        println(">>> Check all nodes for hello message output.")
    }
}
