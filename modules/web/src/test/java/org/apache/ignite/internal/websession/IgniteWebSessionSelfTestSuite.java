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

package org.apache.ignite.internal.websession;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.IgniteTestSuite;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Test suite for web sessions caching functionality.
 */
public class IgniteWebSessionSelfTestSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new IgniteTestSuite("Ignite Web Sessions Test Suite");

        suite.addTest(new JUnit4TestAdapter(WebSessionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(WebSessionTransactionalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(WebSessionReplicatedSelfTest.class));

        // Old implementation tests.
        suite.addTest(new JUnit4TestAdapter(WebSessionV1SelfTest.class));
        suite.addTest(new JUnit4TestAdapter(WebSessionTransactionalV1SelfTest.class));
        suite.addTest(new JUnit4TestAdapter(WebSessionReplicatedV1SelfTest.class));

        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteWebSessionSelfTestSuite.class));

        return suite;
    }
}
