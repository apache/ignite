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

import org.apache.ignite.internal.ComputeJobCancelWithServiceSelfTest;
import org.apache.ignite.internal.processors.service.ClosureServiceClientsNodesTest;
import org.apache.ignite.internal.processors.service.GridServiceClientNodeTest;
import org.apache.ignite.internal.processors.service.GridServiceClusterReadOnlyModeTest;
import org.apache.ignite.internal.processors.service.GridServiceContinuousQueryRedeployTest;
import org.apache.ignite.internal.processors.service.GridServiceDeployClusterReadOnlyModeTest;
import org.apache.ignite.internal.processors.service.GridServiceDeploymentCompoundFutureSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceDeploymentExceptionPropagationTest;
import org.apache.ignite.internal.processors.service.GridServicePackagePrivateSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorBatchDeploySelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorMultiNodeConfigSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorMultiNodeSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorProxySelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorSingleNodeSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorStopSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProxyClientReconnectSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProxyNodeStopSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProxyTopologyInitializationTest;
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
import org.apache.ignite.internal.processors.service.ServiceDeploymentDiscoveryListenerNotificationOrderTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentNonSerializableStaticConfigurationTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOnActivationTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOnClientDisconnectTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOutsideBaselineTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessIdSelfTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessingOnCoordinatorFailTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessingOnCoordinatorLeftTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessingOnNodesFailTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentProcessingOnNodesLeftTest;
import org.apache.ignite.internal.processors.service.ServiceHotRedeploymentViaDeploymentSpiTest;
import org.apache.ignite.internal.processors.service.ServiceInfoSelfTest;
import org.apache.ignite.internal.processors.service.ServicePredicateAccessCacheTest;
import org.apache.ignite.internal.processors.service.ServiceReassignmentFunctionSelfTest;
import org.apache.ignite.internal.processors.service.SystemCacheNotConfiguredTest;
import org.apache.ignite.services.ServiceThreadPoolSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Contains Service Grid related tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ComputeJobCancelWithServiceSelfTest.class,
    GridServiceProcessorSingleNodeSelfTest.class,
    GridServiceProcessorMultiNodeSelfTest.class,
    GridServiceProcessorMultiNodeConfigSelfTest.class,
    GridServiceProcessorProxySelfTest.class,
    GridServiceReassignmentSelfTest.class,
    GridServiceClientNodeTest.class,
    GridServiceProcessorStopSelfTest.class,
    ServicePredicateAccessCacheTest.class,
    GridServicePackagePrivateSelfTest.class,
    GridServiceSerializationSelfTest.class,
    GridServiceProxyNodeStopSelfTest.class,
    GridServiceProxyClientReconnectSelfTest.class,
    IgniteServiceReassignmentTest.class,
    IgniteServiceProxyTimeoutInitializedTest.class,
    IgniteServiceDynamicCachesSelfTest.class,
    GridServiceContinuousQueryRedeployTest.class,
    ServiceThreadPoolSelfTest.class,
    GridServiceProcessorBatchDeploySelfTest.class,
    GridServiceDeploymentCompoundFutureSelfTest.class,
    SystemCacheNotConfiguredTest.class,
    ClosureServiceClientsNodesTest.class,
    ServiceDeploymentOnActivationTest.class,
    ServiceDeploymentOutsideBaselineTest.class,

    IgniteServiceDeploymentClassLoadingDefaultMarshallerTest.class,
    IgniteServiceDeploymentClassLoadingJdkMarshallerTest.class,
    IgniteServiceDeploymentClassLoadingOptimizedMarshallerTest.class,
    IgniteServiceDeployment2ClassLoadersDefaultMarshallerTest.class,
    IgniteServiceDeployment2ClassLoadersJdkMarshallerTest.class,
    IgniteServiceDeployment2ClassLoadersOptimizedMarshallerTest.class,

    GridServiceDeploymentExceptionPropagationTest.class,
    ServiceDeploymentProcessingOnCoordinatorLeftTest.class,
    ServiceDeploymentProcessingOnCoordinatorFailTest.class,
    ServiceDeploymentProcessingOnNodesLeftTest.class,
    ServiceDeploymentProcessingOnNodesFailTest.class,
    ServiceDeploymentOnClientDisconnectTest.class,
    ServiceDeploymentDiscoveryListenerNotificationOrderTest.class,
    ServiceDeploymentNonSerializableStaticConfigurationTest.class,
    ServiceReassignmentFunctionSelfTest.class,
    ServiceInfoSelfTest.class,
    ServiceDeploymentProcessIdSelfTest.class,
    ServiceHotRedeploymentViaDeploymentSpiTest.class,
    GridServiceProxyTopologyInitializationTest.class,
    GridServiceDeployClusterReadOnlyModeTest.class,
    GridServiceClusterReadOnlyModeTest.class,
})
public class IgniteServiceGridTestSuite {
}
