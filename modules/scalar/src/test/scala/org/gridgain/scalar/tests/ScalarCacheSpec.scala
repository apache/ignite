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

import org.apache.ignite.events.{GridEventType, IgniteEvent}
import org.apache.ignite.lang.IgnitePredicate
import org.gridgain.scalar._
import scalar._
import org.scalatest.matchers._
import org.scalatest._
import junit.JUnitRunner
import org.gridgain.grid.events.GridEventType
import GridEventType._
import collection.JavaConversions._
import org.junit.runner.RunWith

/**
 * Scalar cache test.
 */
@RunWith(classOf[JUnitRunner])
class ScalarCacheSpec extends FlatSpec with ShouldMatchers {
    behavior of "Scalar cache"

    it should "work properly via Java APIs" in {
        scalar("examples/config/example-cache.xml") {
            registerListener()

            val c = cache$("partitioned").get.viewByType(classOf[Int], classOf[Int])

            c.putx(1, 1)
            c.putx(2, 2)

            c.values foreach println

            println("Size is: " + c.size)
        }
    }

    /**
     * This method will register listener for cache events on all nodes,
     * so we can actually see what happens underneath locally and remotely.
     */
    def registerListener() {
        val g = grid$

        g *< (() => {
            val lsnr = new IgnitePredicate[IgniteEvent]() {
                override def apply(e: IgniteEvent): Boolean = {
                    println(e.shortDisplay)

                    true
                }
            }

            if (g.cluster().nodeLocalMap[String, AnyRef].putIfAbsent("lsnr", lsnr) == null) {
                g.events.localListen(lsnr,
                    EVT_CACHE_OBJECT_PUT,
                    EVT_CACHE_OBJECT_READ,
                    EVT_CACHE_OBJECT_REMOVED)

                println("Listener is registered.")
            }
        }, null)
    }
}
