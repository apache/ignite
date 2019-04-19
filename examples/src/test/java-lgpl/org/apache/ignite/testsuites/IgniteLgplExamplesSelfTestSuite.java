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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.examples.HibernateL2CacheExampleMultiNodeSelfTest;
import org.apache.ignite.examples.HibernateL2CacheExampleSelfTest;
import org.apache.ignite.examples.SpatialQueryExampleMultiNodeSelfTest;
import org.apache.ignite.examples.SpatialQueryExampleSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Examples test suite. <p> Contains only Spring ignite examples tests.
 */
@RunWith(AllTests.class)
public class IgniteLgplExamplesSelfTestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteLgplExamplesSelfTestSuite.class));

        TestSuite suite = new TestSuite("Ignite Examples Test Suite");

        suite.addTest(new JUnit4TestAdapter(HibernateL2CacheExampleSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SpatialQueryExampleSelfTest.class));

        // Multi-node.
        suite.addTest(new JUnit4TestAdapter(HibernateL2CacheExampleMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SpatialQueryExampleMultiNodeSelfTest.class));

        return suite;
    }
}
