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

import org.apache.ignite.cluster.ClusterNode
import org.gridgain.scalar.scalar
import scalar._
import org.apache.ignite._
import org.gridgain.grid._

/**
 * Demonstrates various closure executions on the cloud using Scalar.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ggstart.{sh|bat} examples/config/example-compute.xml'`.
 */
object ScalarClosureExample extends App {
    scalar("examples/config/example-compute.xml") {
        topology()
        helloWorld()
        helloWorld2()
        broadcast()
        greetRemotes()
        greetRemotesAgain()
    }

    /**
     * Prints grid topology.
     */
    def topology() {
        grid$ foreach (n => println("Node: " + nid8$(n)))
    }

    /**
     * Obligatory example (2) - cloud enabled Hello World!
     */
    def helloWorld2() {
        // Notice the example usage of Java-side closure 'F.println(...)' and method 'scala'
        // that explicitly converts Java side object to a proper Scala counterpart.
        // This method is required since implicit conversion won't be applied here.
        grid$.run$(for (w <- "Hello World!".split(" ")) yield () => println(w), null)
    }

    /**
     * Obligatory example - cloud enabled Hello World!
     */
    def helloWorld() {
        grid$.run$("HELLO WORLD!".split(" ") map (w => () => println(w)), null)
    }

    /**
     * One way to execute closures on the grid.
     */
    def broadcast() {
        grid$.bcastRun(() => println("Broadcasting!!!"), null)
    }

    /**
     *  Greats all remote nodes only.
     */
    def greetRemotes() {
        val me = grid$.cluster().localNode.id

        // Note that usage Java-based closure.
        grid$.cluster().forRemotes() match {
            case p if p.isEmpty => println("No remote nodes!")
            case p => p.bcastRun(() => println("Greetings from: " + me), null)
        }
    }

    /**
     * Same as previous greetings for all remote nodes but remote projection is created manually.
     */
    def greetRemotesAgain() {
        val me = grid$.cluster().localNode.id

        // Just show that we can create any projections we like...
        // Note that usage of Java-based closure via 'F' typedef.
        grid$.cluster().forPredicate((n: ClusterNode) => n.id != me) match {
            case p if p.isEmpty => println("No remote nodes!")
            case p => p.bcastRun(() => println("Greetings again from: " + me), null)
        }
    }
}
