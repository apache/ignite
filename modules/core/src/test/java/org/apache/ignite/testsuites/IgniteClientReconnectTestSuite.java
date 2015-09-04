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

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.IgniteClientReconnectApiExceptionTest;
import org.apache.ignite.internal.IgniteClientReconnectAtomicsTest;
import org.apache.ignite.internal.IgniteClientReconnectCacheTest;
import org.apache.ignite.internal.IgniteClientReconnectCollectionsTest;
import org.apache.ignite.internal.IgniteClientReconnectComputeTest;
import org.apache.ignite.internal.IgniteClientReconnectContinuousProcessorTest;
import org.apache.ignite.internal.IgniteClientReconnectDiscoveryStateTest;
import org.apache.ignite.internal.IgniteClientReconnectFailoverTest;
import org.apache.ignite.internal.IgniteClientReconnectServicesTest;
import org.apache.ignite.internal.IgniteClientReconnectStopTest;
import org.apache.ignite.internal.IgniteClientReconnectStreamerTest;

/**
 *
 */
public class IgniteClientReconnectTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception In case of error.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Client Reconnect Test Suite");

        suite.addTestSuite(IgniteClientReconnectStopTest.class);
        suite.addTestSuite(IgniteClientReconnectApiExceptionTest.class);
        suite.addTestSuite(IgniteClientReconnectDiscoveryStateTest.class);
        suite.addTestSuite(IgniteClientReconnectCacheTest.class);
        suite.addTestSuite(IgniteClientReconnectContinuousProcessorTest.class);
        suite.addTestSuite(IgniteClientReconnectComputeTest.class);
        suite.addTestSuite(IgniteClientReconnectAtomicsTest.class);
        suite.addTestSuite(IgniteClientReconnectCollectionsTest.class);
        suite.addTestSuite(IgniteClientReconnectServicesTest.class);
        suite.addTestSuite(IgniteClientReconnectStreamerTest.class);
        suite.addTestSuite(IgniteClientReconnectFailoverTest.class);

        return suite;
    }
}