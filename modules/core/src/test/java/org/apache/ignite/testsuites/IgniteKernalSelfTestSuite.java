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

import java.util.Set;
import junit.framework.TestSuite;
import org.apache.ignite.internal.ComputeJobCancelWithServiceSelfTest;
import org.apache.ignite.internal.GridCommunicationSelfTest;
import org.apache.ignite.internal.GridDiscoveryEventSelfTest;
import org.apache.ignite.internal.GridDiscoverySelfTest;
import org.apache.ignite.internal.GridFailedInputParametersSelfTest;
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
import org.apache.ignite.internal.IgniteUpdateNotifierPerClusterSettingSelfTest;
import org.apache.ignite.internal.managers.GridManagerStopSelfTest;
import org.apache.ignite.internal.managers.communication.GridCommunicationSendMessageSelfTest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManagerStopSelfTest;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManagerAliveCacheSelfTest;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManagerAttributesSelfTest;
import org.apache.ignite.internal.managers.discovery.IgniteTopologyPrintFormatSelfTest;
import org.apache.ignite.internal.managers.events.GridEventStorageManagerSelfTest;
import org.apache.ignite.internal.managers.swapspace.GridSwapSpaceManagerSelfTest;
import org.apache.ignite.internal.processors.cluster.GridAddressResolverSelfTest;
import org.apache.ignite.internal.processors.cluster.GridUpdateNotifierSelfTest;
import org.apache.ignite.internal.processors.port.GridPortProcessorSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceClientNodeTest;
import org.apache.ignite.internal.processors.service.GridServicePackagePrivateSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorMultiNodeConfigSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorMultiNodeSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorProxySelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorSingleNodeSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorStopSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProxyClientReconnectSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProxyNodeStopSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceReassignmentSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceSerializationSelfTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeployment2ClassLoadersDefaultMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeployment2ClassLoadersJdkMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeployment2ClassLoadersOptimizedMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeploymentClassLoadingDefaultMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeploymentClassLoadingJdkMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDeploymentClassLoadingOptimizedMarshallerTest;
import org.apache.ignite.internal.processors.service.IgniteServiceDynamicCachesSelfTest;
import org.apache.ignite.internal.processors.service.IgniteServiceProxyTimeoutInitializedTest;
import org.apache.ignite.internal.processors.service.IgniteServiceReassignmentTest;
import org.apache.ignite.internal.processors.service.ServicePredicateAccessCacheTest;
import org.apache.ignite.internal.util.GridStartupWithUndefinedIgniteHomeSelfTest;
import org.apache.ignite.spi.communication.GridCacheMessageSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Kernal self test suite.
 */
public class IgniteKernalSelfTestSuite extends TestSuite {
    /**
     * @return Kernal test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("Ignite Kernal Test Suite");

        suite.addTestSuite(GridSameVmStartupSelfTest.class);
        suite.addTestSuite(GridSpiExceptionSelfTest.class);
        suite.addTestSuite(GridRuntimeExceptionSelfTest.class);
        suite.addTestSuite(GridFailedInputParametersSelfTest.class);
        suite.addTestSuite(GridNodeFilterSelfTest.class);
        suite.addTestSuite(GridNodeVisorAttributesSelfTest.class);
        suite.addTestSuite(GridDiscoverySelfTest.class);
        suite.addTestSuite(GridCommunicationSelfTest.class);
        suite.addTestSuite(GridEventStorageManagerSelfTest.class);
        suite.addTestSuite(GridSwapSpaceManagerSelfTest.class);
        suite.addTestSuite(GridCommunicationSendMessageSelfTest.class);
        suite.addTestSuite(GridCacheMessageSelfTest.class);
        suite.addTestSuite(GridDeploymentManagerStopSelfTest.class);
        suite.addTestSuite(GridManagerStopSelfTest.class);
        suite.addTestSuite(GridDiscoveryManagerAttributesSelfTest.RegularDiscovery.class);
        suite.addTestSuite(GridDiscoveryManagerAttributesSelfTest.ClientDiscovery.class);
        suite.addTestSuite(GridDiscoveryManagerAliveCacheSelfTest.class);
        suite.addTestSuite(GridDiscoveryEventSelfTest.class);
        suite.addTestSuite(GridPortProcessorSelfTest.class);
        suite.addTestSuite(GridHomePathSelfTest.class);
        suite.addTestSuite(GridStartupWithUndefinedIgniteHomeSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridVersionSelfTest.class, ignoredTests);
        suite.addTestSuite(GridListenActorSelfTest.class);
        suite.addTestSuite(GridNodeLocalSelfTest.class);
        suite.addTestSuite(GridKernalConcurrentAccessStopSelfTest.class);
        suite.addTestSuite(IgniteConcurrentEntryProcessorAccessStopTest.class);
        suite.addTestSuite(GridUpdateNotifierSelfTest.class);
        suite.addTestSuite(GridAddressResolverSelfTest.class);
        suite.addTestSuite(IgniteUpdateNotifierPerClusterSettingSelfTest.class);
        suite.addTestSuite(GridLocalEventListenerSelfTest.class);
        suite.addTestSuite(IgniteTopologyPrintFormatSelfTest.class);
        suite.addTestSuite(ComputeJobCancelWithServiceSelfTest.class);

        // Managed Services.
        suite.addTestSuite(GridServiceProcessorSingleNodeSelfTest.class);
        suite.addTestSuite(GridServiceProcessorMultiNodeSelfTest.class);
        suite.addTestSuite(GridServiceProcessorMultiNodeConfigSelfTest.class);
        suite.addTestSuite(GridServiceProcessorProxySelfTest.class);
        suite.addTestSuite(GridServiceReassignmentSelfTest.class);
        suite.addTestSuite(GridServiceClientNodeTest.class);
        suite.addTestSuite(GridServiceProcessorStopSelfTest.class);
        suite.addTestSuite(ServicePredicateAccessCacheTest.class);
        suite.addTestSuite(GridServicePackagePrivateSelfTest.class);
        suite.addTestSuite(GridServiceSerializationSelfTest.class);
        suite.addTestSuite(GridServiceProxyNodeStopSelfTest.class);
        suite.addTestSuite(GridServiceProxyClientReconnectSelfTest.class);
        suite.addTestSuite(IgniteServiceReassignmentTest.class);
        suite.addTestSuite(IgniteServiceProxyTimeoutInitializedTest.class);
        suite.addTestSuite(IgniteServiceDynamicCachesSelfTest.class);

        suite.addTestSuite(IgniteServiceDeploymentClassLoadingDefaultMarshallerTest.class);
        suite.addTestSuite(IgniteServiceDeploymentClassLoadingOptimizedMarshallerTest.class);
        suite.addTestSuite(IgniteServiceDeploymentClassLoadingJdkMarshallerTest.class);
        suite.addTestSuite(IgniteServiceDeployment2ClassLoadersDefaultMarshallerTest.class);
        suite.addTestSuite(IgniteServiceDeployment2ClassLoadersOptimizedMarshallerTest.class);
        suite.addTestSuite(IgniteServiceDeployment2ClassLoadersJdkMarshallerTest.class);

        return suite;
    }
}
