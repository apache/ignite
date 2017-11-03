/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.examples.HibernateL2CacheExampleMultiNodeSelfTest;
import org.apache.ignite.examples.HibernateL2CacheExampleSelfTest;
import org.apache.ignite.examples.SpatialQueryExampleMultiNodeSelfTest;
import org.apache.ignite.examples.SpatialQueryExampleSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Examples test suite. <p> Contains only Spring ignite examples tests.
 */
public class IgniteLgplExamplesSelfTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteLgplExamplesSelfTestSuite.class));

        TestSuite suite = new TestSuite("Ignite Examples Test Suite");

        suite.addTest(new TestSuite(HibernateL2CacheExampleSelfTest.class));
        suite.addTest(new TestSuite(SpatialQueryExampleSelfTest.class));

        // Multi-node.
        suite.addTest(new TestSuite(HibernateL2CacheExampleMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(SpatialQueryExampleMultiNodeSelfTest.class));

        return suite;
    }
}