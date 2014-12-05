/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.testsuites

import org.scalatest._
import org.gridgain.scalar.tests._
import examples.{ScalarExamplesMultiNodeSelfTest, ScalarExamplesSelfTest}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.gridgain.grid.IgniteSystemProperties._
import org.gridgain.testframework.GridTestUtils

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class ScalarExamplesSelfTestSuite extends Suites(
    new ScalarExamplesSelfTest,
    new ScalarExamplesMultiNodeSelfTest
) {
    System.setProperty(GG_OVERRIDE_MCAST_GRP,
        GridTestUtils.getNextMulticastGroup(classOf[ScalarExamplesSelfTest]))
}
