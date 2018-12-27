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
import org.apache.ignite.internal.ClusterMetricsSelfTest;
import org.apache.ignite.internal.GridCommunicationSelfTest;
import org.apache.ignite.internal.GridDiscoveryEventSelfTest;
import org.apache.ignite.internal.GridDiscoverySelfTest;
import org.apache.ignite.internal.GridFailedInputParametersSelfTest;
import org.apache.ignite.internal.GridGetOrStartSelfTest;
import org.apache.ignite.internal.GridHomePathSelfTest;
import org.apache.ignite.internal.GridKernalConcurrentAccessStopSelfTest;
import org.apache.ignite.internal.GridListenActorSelfTest;
import org.apache.ignite.internal.GridLocalEventListenerSelfTest;
import org.apache.ignite.internal.GridNodeFilterSelfTest;
import org.apache.ignite.internal.GridNodeLocalSelfTest;
import org.apache.ignite.internal.GridNodeVisorAttributesSelfTest;
import org.apache.ignite.internal.GridRuntimeExceptionSelfTest;
import org.apache.ignite.internal.GridSameVmStartupSelfTest;
import org.apache.ignite.internal.GridSpiExceptionSelfTest;
import org.apache.ignite.internal.GridVersionSelfTest;
import org.apache.ignite.internal.IgniteConcurrentEntryProcessorAccessStopTest;
import org.apache.ignite.internal.IgniteConnectionConcurrentReserveAndRemoveTest;
import org.apache.ignite.internal.IgniteUpdateNotifierPerClusterSettingSelfTest;
import org.apache.ignite.internal.LongJVMPauseDetectorTest;
import org.apache.ignite.internal.managers.GridManagerStopSelfTest;
import org.apache.ignite.internal.managers.communication.GridCommunicationSendMessageSelfTest;
import org.apache.ignite.internal.managers.deployment.DeploymentRequestOfUnknownClassProcessingTest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManagerStopSelfTest;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManagerAliveCacheSelfTest;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManagerAttributesSelfTest;
import org.apache.ignite.internal.managers.discovery.IgniteTopologyPrintFormatSelfTest;
import org.apache.ignite.internal.managers.events.GridEventStorageManagerSelfTest;
import org.apache.ignite.internal.processors.cluster.GridAddressResolverSelfTest;
import org.apache.ignite.internal.processors.cluster.GridUpdateNotifierSelfTest;
import org.apache.ignite.internal.processors.port.GridPortProcessorSelfTest;
import org.apache.ignite.internal.util.GridStartupWithUndefinedIgniteHomeSelfTest;
import org.apache.ignite.spi.communication.GridCacheMessageSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

import java.util.Set;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Kernal self test suite.
 */
@RunWith(AllTests.class)
public class IgniteKernalSelfTestSuite {
    /**
     * @return Kernal test suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution.
     * @return Test suite.
     */
    public static TestSuite suite(Set<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Ignite Kernal Test Suite");

        suite.addTest(new JUnit4TestAdapter(GridGetOrStartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSameVmStartupSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSpiExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridRuntimeExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridFailedInputParametersSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridNodeFilterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridNodeVisorAttributesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridDiscoverySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCommunicationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridEventStorageManagerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCommunicationSendMessageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheMessageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridDeploymentManagerStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridManagerStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridDiscoveryManagerAttributesSelfTest.RegularDiscovery.class));
        suite.addTest(new JUnit4TestAdapter(GridDiscoveryManagerAttributesSelfTest.ClientDiscovery.class));
        suite.addTest(new JUnit4TestAdapter(GridDiscoveryManagerAliveCacheSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridDiscoveryEventSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridPortProcessorSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridHomePathSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridStartupWithUndefinedIgniteHomeSelfTest.class));
        GridTestUtils.addTestIfNeeded(suite, GridVersionSelfTest.class, ignoredTests);
        suite.addTest(new JUnit4TestAdapter(GridListenActorSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridNodeLocalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridKernalConcurrentAccessStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteConcurrentEntryProcessorAccessStopTest.class));
        suite.addTest(new JUnit4TestAdapter(GridUpdateNotifierSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridAddressResolverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteUpdateNotifierPerClusterSettingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridLocalEventListenerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTopologyPrintFormatSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteConnectionConcurrentReserveAndRemoveTest.class));
        suite.addTest(new JUnit4TestAdapter(LongJVMPauseDetectorTest.class));
        suite.addTest(new JUnit4TestAdapter(ClusterMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DeploymentRequestOfUnknownClassProcessingTest.class));

        return suite;
    }
}
