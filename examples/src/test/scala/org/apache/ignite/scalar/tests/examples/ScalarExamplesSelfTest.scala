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
import org.junit.Test
import org.scalatest.Suite

/**
 * Scalar examples self test.
 */
class ScalarExamplesSelfTest extends GridAbstractExamplesTest with Suite {
    /** */
    private def EMPTY_ARGS = Array.empty[String]

    /** */
    @Test
    def testScalarCacheAffinitySimpleExample() {
        ScalarCacheAffinityExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarCacheEntryProcessorExample() {
        ScalarCacheEntryProcessorExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarCacheExample() {
        ScalarCacheExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarCacheQueryExample() {
        ScalarCacheQueryExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarClosureExample() {
        ScalarClosureExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarContinuationExample() {
        ScalarContinuationExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarCreditRiskExample() {
        ScalarCreditRiskExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarPingPongExample() {
        scalar("modules/scalar/src/test/resources/spring-ping-pong-partner.xml") {
            ScalarPingPongExample.main(EMPTY_ARGS)
        }
    }

    /** */
    @Test
    def testScalarPopularNumbersRealTimeExample() {
        ScalarCachePopularNumbersExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarPrimeExample() {
        ScalarPrimeExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarTaskExample() {
        ScalarTaskExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarWorldShortestMapReduceExample() {
        ScalarWorldShortestMapReduce.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarSnowflakeSchemaExample() {
        ScalarSnowflakeSchemaExample.main(EMPTY_ARGS)
    }

    /** */
    @Test
    def testScalarSharedRDDExample() {
        ScalarSharedRDDExample.main(EMPTY_ARGS)
    }
}
