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

import org.scalatest.matchers._
import org.scalatest._
import junit.JUnitRunner
import org.gridgain.scalar.scalar
import scalar._
import collection.JavaConversions._
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith

/**
 * Tests for `affinityRun..` and `affinityCall..` methods.
 */
@RunWith(classOf[JUnitRunner])
class ScalarAffinityRoutingSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
    /** Cache name. */
    private val CACHE_NAME = "partitioned_tx"

    "affinityRun$ method" should "run correctly" in scalar("examples/config/example-cache.xml") {
        val c = cache$[Int, Int](CACHE_NAME).get

        c += (0 -> 0)
        c += (1 -> 1)
        c += (2 -> 2)

        val cnt = c.dataStructures().atomicLong("affinityRun", 0, true)

        grid$.affinityRun$(CACHE_NAME, 0, () => { cnt.incrementAndGet() }, null)
        grid$.affinityRun$(CACHE_NAME, 1, () => { cnt.incrementAndGet() }, null)
        grid$.affinityRun$(CACHE_NAME, 2, () => { cnt.incrementAndGet() }, null)

        assert(cnt.get === 3)
    }

    "affinityRunAsync$ method" should "run correctly" in scalar("examples/config/example-cache.xml") {
        val c = cache$[Int, Int](CACHE_NAME).get

        c += (0 -> 0)
        c += (1 -> 1)
        c += (2 -> 2)

        val cnt = c.dataStructures().atomicLong("affinityRunAsync", 0, true)

        grid$.affinityRunAsync$(CACHE_NAME, 0, () => { cnt.incrementAndGet() }, null).get
        grid$.affinityRunAsync$(CACHE_NAME, 1, () => { cnt.incrementAndGet() }, null).get
        grid$.affinityRunAsync$(CACHE_NAME, 2, () => { cnt.incrementAndGet() }, null).get

        assert(cnt.get === 3)
    }
}
