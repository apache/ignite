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
import org.scalatest._
import junit.JUnitRunner
import org.junit.runner.RunWith

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class ScalarSpec extends FunSpec {
    describe("Scalar") {
        it("should start and stop") {
            scalar start()
            scalar.logo()
            scalar stop()
        }
    }
}
