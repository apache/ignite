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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.IgniteClientConnectAfterCommunicationFailureTest;
import org.apache.ignite.internal.IgniteClientReconnectApiExceptionTest;
import org.apache.ignite.internal.IgniteClientReconnectAtomicsTest;
import org.apache.ignite.internal.IgniteClientReconnectBinaryContexTest;
import org.apache.ignite.internal.IgniteClientReconnectCacheTest;
import org.apache.ignite.internal.IgniteClientReconnectCollectionsTest;
import org.apache.ignite.internal.IgniteClientReconnectComputeTest;
import org.apache.ignite.internal.IgniteClientReconnectContinuousProcessorTest;
import org.apache.ignite.internal.IgniteClientReconnectDelayedSpiTest;
import org.apache.ignite.internal.IgniteClientReconnectDiscoveryStateTest;
import org.apache.ignite.internal.IgniteClientReconnectFailoverTest;
import org.apache.ignite.internal.IgniteClientReconnectServicesTest;
import org.apache.ignite.internal.IgniteClientReconnectStopTest;
import org.apache.ignite.internal.IgniteClientReconnectStreamerTest;
import org.apache.ignite.internal.IgniteClientRejoinTest;

/**
 *
 */
public class IgniteClientReconnectTestSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Client Reconnect Test Suite");

        suite.addTest(new JUnit4TestAdapter(IgniteClientConnectAfterCommunicationFailureTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectStopTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectApiExceptionTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectDiscoveryStateTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectDelayedSpiTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectBinaryContexTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectContinuousProcessorTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectComputeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectAtomicsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectCollectionsTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectServicesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectStreamerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientRejoinTest.class));

        return suite;
    }
}
