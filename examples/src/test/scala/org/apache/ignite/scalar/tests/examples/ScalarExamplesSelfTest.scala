/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.scalar.tests.examples

import org.apache.ignite.scalar.examples._
import org.apache.ignite.scalar.examples.spark._
import org.apache.ignite.scalar.scalar
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest
import org.scalatest.junit.JUnitSuiteLike

/**
 * Scalar examples self test.
 */
class ScalarExamplesSelfTest extends GridAbstractExamplesTest with JUnitSuiteLike {
    /** */
    private def EMPTY_ARGS = Array.empty[String]

    /** */
    def testScalarCacheAffinitySimpleExample() {
        ScalarCacheAffinityExample.main(EMPTY_ARGS)
    }

    /** */
    def testScalarCacheEntryProcessorExample() {
        ScalarCacheEntryProcessorExample.main(EMPTY_ARGS)
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

    /** */
    def testScalarSharedRDDExample() {
        ScalarSharedRDDExample.main(EMPTY_ARGS)
    }
}
