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

import org.gridgain.scalar._
import scalar._
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

/**
 * Test for using grid.cache(..).projection(...) from scala code.
 */
@RunWith(classOf[JUnitRunner])
class ScalarCacheProjectionSpec extends FlatSpec {
    behavior of "Cache projection"

    it should "work properly via grid.cache(...).viewByType(...)" in scalar("examples/config/example-cache.xml") {
        val cache = grid$.cache("local").viewByType(classOf[String], classOf[Int])

        assert(cache.putx("1", 1))
        assert(cache.get("1") == 1)
    }
}
