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

import org.apache.ignite.gridify.BasicAopSelfTest;
import org.apache.ignite.gridify.GridifySetToXXXNonSpringAopSelfTest;
import org.apache.ignite.gridify.GridifySetToXXXSpringAopSelfTest;
import org.apache.ignite.gridify.NonSpringAopSelfTest;
import org.apache.ignite.gridify.SpringAopSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.test.gridify.ExternalNonSpringAopSelfTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * AOP test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Test configuration.
    BasicAopSelfTest.class,

    SpringAopSelfTest.class,
    NonSpringAopSelfTest.class,
    GridifySetToXXXSpringAopSelfTest.class,
    GridifySetToXXXNonSpringAopSelfTest.class,
    ExternalNonSpringAopSelfTest.class,
})
public class IgniteAopSelfTestSuite {
    /** */
    @BeforeClass
    public static void init() {
        // Examples
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP, GridTestUtils.getNextMulticastGroup(IgniteAopSelfTestSuite.class));
    }
}
