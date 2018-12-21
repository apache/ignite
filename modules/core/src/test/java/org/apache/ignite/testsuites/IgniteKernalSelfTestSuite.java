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
import org.apache.ignite.internal.ComputeJobCancelWithServiceSelfTest;
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
import org.apache.ignite.internal.processors.service.GridServiceClientNodeTest;
import org.apache.ignite.internal.processors.service.GridServiceContinuousQueryRedeployTest;
import org.apache.ignite.internal.processors.service.GridServiceDeploymentCompoundFutureSelfTest;
import org.apache.ignite.internal.processors.service.GridServicePackagePrivateSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorBatchDeploySelfTest;
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
import org.apache.ignite.internal.processors.service.SystemCacheNotConfiguredTest;
import org.apache.ignite.internal.util.GridStartupWithUndefinedIgniteHomeSelfTest;
import org.apache.ignite.services.ServiceThreadPoolSelfTest;
import org.apache.ignite.spi.communication.GridCacheMessageSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

import java.util.Set;

/**
 * Kernal self test suite.
 */
public class IgniteKernalSelfTestSuite extends TestSuite {
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
        suite.addTest(new JUnit4TestAdapter(ComputeJobCancelWithServiceSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteConnectionConcurrentReserveAndRemoveTest.class));
        suite.addTest(new JUnit4TestAdapter(LongJVMPauseDetectorTest.class));
        suite.addTest(new JUnit4TestAdapter(ClusterMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DeploymentRequestOfUnknownClassProcessingTest.class));

        // Managed Services.
        suite.addTest(new JUnit4TestAdapter(GridServiceProcessorSingleNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceProcessorMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceProcessorMultiNodeConfigSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceProcessorProxySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceReassignmentSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceClientNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceProcessorStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ServicePredicateAccessCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServicePackagePrivateSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceSerializationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceProxyNodeStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceProxyClientReconnectSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceReassignmentTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceProxyTimeoutInitializedTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDynamicCachesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceContinuousQueryRedeployTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceThreadPoolSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceProcessorBatchDeploySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridServiceDeploymentCompoundFutureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SystemCacheNotConfiguredTest.class));
        // IGNITE-3392
        //suite.addTest(new JUnit4TestAdapter(GridServiceDeploymentExceptionPropagationTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeploymentClassLoadingDefaultMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeploymentClassLoadingJdkMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeploymentClassLoadingOptimizedMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeployment2ClassLoadersDefaultMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeployment2ClassLoadersJdkMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeployment2ClassLoadersOptimizedMarshallerTest.class));

        return suite;
    }
}
