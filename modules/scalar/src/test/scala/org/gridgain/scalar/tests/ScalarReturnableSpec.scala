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
import org.gridgain.scalar._
import scalar._
import org.scalatest._
import junit.JUnitRunner
import scala.util.control.Breaks._
import org.junit.runner.RunWith

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class ScalarReturnableSpec extends FlatSpec with ShouldMatchers {
    "Scalar '^^'" should "work" in {
        var i = 0

        breakable {
            while (true) {
                if (i == 0)
                    println("Only once!") ^^

                i += 1
            }
        }

        assert(i == 0)
    }

    "Scalar '^^'" should "also work" in {
        test()
    }

    // Ignore exception below.
    def test() = breakable {
        while (true) {
            println("Only once!") ^^
        }
    }
}
