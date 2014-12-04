/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.tests

import org.apache.ignite.cluster.ClusterNode
import org.gridgain.scalar._
import scalar._
import org.scalatest.matchers._
import org.scalatest._
import junit.JUnitRunner
import org.gridgain.grid._
import org.gridgain.grid.{GridGain => G}
import collection.JavaConversions._
import java.util.UUID
import org.junit.runner.RunWith
import org.gridgain.grid.messaging.GridMessagingListenActor

/**
 * Scalar cache test.
 */
@RunWith(classOf[JUnitRunner])
class ScalarProjectionSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
    /**
     *
     */
    override def beforeAll() {
        G.start(gridConfig("node-1", false))
        G.start(gridConfig("node-2", true))
    }

    /**
     *
     */
    override def afterAll() {
        G.stop("node-1", true)
        G.stop("node-2", true)
    }

    /**
     *
     * @param name Grid name.
     * @param shown Shown flag.
     */
    private def gridConfig(name: String, shown: Boolean): IgniteConfiguration = {
        val attrs: java.util.Map[String, Boolean] = Map[String, Boolean]("shown" -> shown)

        val cfg = new IgniteConfiguration

        cfg.setGridName(name)
        cfg.setUserAttributes(attrs)

        cfg
    }

    behavior of "ScalarProjectionPimp class"

    it should "return all nodes" in scalar(gridConfig("node-scalar", true)) {
        assertResult(3) {
            grid$("node-scalar").get.cluster().nodes().size
        }
    }

    it should "return shown nodes" in  scalar(gridConfig("node-scalar", true)) {
        assert(grid$("node-scalar").get.nodes$((node: ClusterNode) => node.attribute[Boolean]("shown")).size == 2)
    }

    it should "return all remote nodes" in scalar(gridConfig("node-scalar", true)) {
        assertResult(2) {
            grid$("node-scalar").get.remoteNodes$().size
        }
    }

    it should "return shown remote nodes" in  scalar(gridConfig("node-scalar", true)) {
        assert(grid$("node-scalar").get.remoteNodes$((node: ClusterNode) =>
            node.attribute[Boolean]("shown")).size == 1)
    }

    it should "correctly send messages" in scalar(gridConfig("node-scalar", true)) {

        grid$("node-1").get.message().remoteListen(null, new GridMessagingListenActor[Any]() {
            def receive(nodeId: UUID, msg: Any) {
                println("node-1 received " + msg)
            }
        })

        grid$("node-2").get.message().remoteListen(null, new GridMessagingListenActor[Any]() {
            def receive(nodeId: UUID, msg: Any) {
                println("node-2 received " + msg)
            }
        })

        grid$("node-scalar").get !< ("Message", null)
        grid$("node-scalar").get !< (Seq("Message1", "Message2"), null)
    }

    it should "correctly make calls" in scalar(gridConfig("node-scalar", true)) {
        println("CALL RESULT: " + grid$("node-scalar").get #< (() => "Message", null))

        println("ASYNC CALL RESULT: " + grid$("node-scalar").get.callAsync$[String](() => "Message", null).get)

        val call1: () => String = () => "Message1"
        val call2: () => String = () => "Message2"

        println("MULTIPLE CALL RESULT: " + grid$("node-scalar").get #< (Seq(call1, call2), null))

        println("MULTIPLE ASYNC CALL RESULT: " +
            (grid$("node-scalar").get #? (Seq(call1, call2), null)).get)
    }

    it should "correctly make runs" in scalar(gridConfig("node-scalar", true)) {
        grid$("node-scalar").get *< (() => println("RUN RESULT: Message"), null)

        (grid$("node-scalar").get *? (() => println("ASYNC RUN RESULT: Message"), null)).get

        val run1: () => Unit = () => println("RUN 1 RESULT: Message1")
        val run2: () => Unit = () => println("RUN 2 RESULT: Message2")

        grid$("node-scalar").get *< (Seq(run1, run2), null)

        val runAsync1: () => Unit = () => println("ASYNC RUN 1 RESULT: Message1")
        val runAsync2: () => Unit = () => println("ASYNC RUN 2 RESULT: Message2")

        (grid$("node-scalar").get *? (Seq(runAsync1, runAsync2), null)).get
    }

    it should "correctly reduce" in scalar(gridConfig("node-scalar", true)) {
        val call1: () => Int = () => 15
        val call2: () => Int = () => 82

        assert(grid$("node-scalar").get @< (Seq(call1, call2), (n: Seq[Int]) => n.sum, null) == 97)
        assert(grid$("node-scalar").get.reduceAsync$(Seq(call1, call2), (n: Seq[Int]) => n.sum, null).get == 97)
    }
}
