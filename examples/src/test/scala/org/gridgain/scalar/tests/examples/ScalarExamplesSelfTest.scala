/* @scala.file.header */

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.tests.examples

import org.gridgain.scalar.examples._
import org.gridgain.scalar.scalar
import org.scalatest.junit.JUnitSuiteLike
import org.gridgain.testframework.junits.common.GridAbstractExamplesTest

/**
 * Scalar examples self test.
 */
class ScalarExamplesSelfTest extends GridAbstractExamplesTest with JUnitSuiteLike {
    /** */
    private def EMPTY_ARGS = Array.empty[String]

    /** */
    def testScalarCacheAffinityExample1() {
        ScalarCacheAffinityExample1.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheAffinityExample2() {
        ScalarCacheAffinityExample2.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheAffinitySimpleExample() {
        ScalarCacheAffinitySimpleExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheExample() {
        ScalarCacheExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheQueryExample() {
        ScalarCacheQueryExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarClosureExample() {
        ScalarClosureExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarContinuationExample() {
        ScalarContinuationExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCreditRiskExample() {
        ScalarCreditRiskExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarPiCalculationExample() {
        ScalarPiCalculationExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarPingPongExample() {
        scalar("modules/scalar/src/test/resources/spring-ping-pong-partner.xml") {
            ScalarPingPongExample.main(EMPTY_ARGS)
        }
    }

    /** */
    def testScalarPopularNumbersRealTimeExample() {
        ScalarCachePopularNumbersExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarPrimeExample() {
        ScalarPrimeExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarScheduleCallableExample() {
        ScalarScheduleExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarStartStopExample() {
        ScalarStartStopExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarTaskExample() {
        ScalarTaskExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarWorldShortestMapReduceExample() {
        ScalarWorldShortestMapReduce.main(EMPTY_ARGS)
    }

    /** */
    def testScalarSnowflakeSchemaExample() {
        ScalarSnowflakeSchemaExample.main(EMPTY_ARGS)
    }
}
