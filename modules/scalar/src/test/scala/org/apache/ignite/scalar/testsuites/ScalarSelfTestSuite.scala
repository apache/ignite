/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.scalar.testsuites

import org.apache.ignite.IgniteSystemProperties._
import org.apache.ignite.scalar.tests._
import org.apache.ignite.testframework.GridTestUtils

import org.junit.runner.RunWith

import org.scalatest._
import org.scalatest.junit.JUnitRunner

/**
 *
 */
@RunWith(classOf[JUnitRunner])
class ScalarSelfTestSuite extends Suites(
    new ScalarAffinityRoutingSpec,
    new ScalarCacheQueriesSpec,
    new ScalarCacheSpec,
    new ScalarConversionsSpec,
    new ScalarProjectionSpec,
    new ScalarReturnableSpec,
    new ScalarSpec
) {
    System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
        GridTestUtils.getNextMulticastGroup(classOf[ScalarSelfTestSuite]))
}
