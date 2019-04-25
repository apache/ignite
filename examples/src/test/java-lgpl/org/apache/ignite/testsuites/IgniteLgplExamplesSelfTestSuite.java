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

package org.apache.ignite.testsuites;

import org.apache.ignite.examples.ComputeScheduleExampleMultiNodeSelfTest;
import org.apache.ignite.examples.ComputeScheduleExampleSelfTest;
import org.apache.ignite.examples.HibernateL2CacheExampleMultiNodeSelfTest;
import org.apache.ignite.examples.HibernateL2CacheExampleSelfTest;
import org.apache.ignite.examples.SpatialQueryExampleMultiNodeSelfTest;
import org.apache.ignite.examples.SpatialQueryExampleSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Examples test suite. <p> Contains only Spring ignite examples tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    HibernateL2CacheExampleSelfTest.class,
    SpatialQueryExampleSelfTest.class,
    ComputeScheduleExampleSelfTest.class,

    // Multi-node.
    HibernateL2CacheExampleMultiNodeSelfTest.class,
    SpatialQueryExampleMultiNodeSelfTest.class,
    ComputeScheduleExampleMultiNodeSelfTest.class,
})
public class IgniteLgplExamplesSelfTestSuite {
    /** */
    @BeforeClass
    public static void init() {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteLgplExamplesSelfTestSuite.class));
    }
}
