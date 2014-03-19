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
 * Test runnable object broadcasts a phrase to all grid nodes every minute,
 * 3 times with initial scheduling delay equal to five seconds.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ggstart.{sh|bat} examples/config/example-compute.xml'`.
 */
object ScalarScheduleRunnableExample extends App {
    scalar("examples/config/example-compute.xml") {
        val g = grid$

        // Schedule output message every minute.
        val fut = g.scheduleLocalRun(
            () => g.bcastRun(() => println("Howdy! :)"), null),
            "{5, 3} * * * * *" // Cron expression.
        )

        while (!fut.isDone)
            fut.get

        println(">>>>> Check all nodes for hello message output.")
    }
}
