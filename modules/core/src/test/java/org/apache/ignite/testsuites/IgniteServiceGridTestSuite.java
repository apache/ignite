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
import org.apache.ignite.internal.ComputeJobCancelWithServiceSelfTest;
import org.apache.ignite.internal.processors.service.ClosureServiceClientsNodesTest;
import org.apache.ignite.internal.processors.service.GridServiceClientNodeTest;
import org.apache.ignite.internal.processors.service.GridServiceContinuousQueryRedeployTest;
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
import org.apache.ignite.internal.processors.service.ServiceInfoSelfTest;
import org.apache.ignite.internal.processors.service.ServicePredicateAccessCacheTest;
import org.apache.ignite.internal.processors.service.ServiceReassignmentFunctionSelfTest;
import org.apache.ignite.internal.processors.service.SystemCacheNotConfiguredTest;
import org.apache.ignite.services.ServiceThreadPoolSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Contains Service Grid related tests.
 */
@RunWith(AllTests.class)
public class IgniteServiceGridTestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Service Grid Test Suite");

        suite.addTest(new JUnit4TestAdapter(ComputeJobCancelWithServiceSelfTest.class));
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
        suite.addTest(new JUnit4TestAdapter(ClosureServiceClientsNodesTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentOnActivationTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentOutsideBaselineTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeploymentClassLoadingDefaultMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeploymentClassLoadingJdkMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeploymentClassLoadingOptimizedMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeployment2ClassLoadersDefaultMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeployment2ClassLoadersJdkMarshallerTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteServiceDeployment2ClassLoadersOptimizedMarshallerTest.class));

        suite.addTest(new JUnit4TestAdapter(GridServiceDeploymentExceptionPropagationTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentProcessingOnCoordinatorLeftTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentProcessingOnCoordinatorFailTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentProcessingOnNodesLeftTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentProcessingOnNodesFailTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentOnClientDisconnectTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentDiscoveryListenerNotificationOrderTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentNonSerializableStaticConfigurationTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceReassignmentFunctionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceInfoSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentProcessIdSelfTest.class));

        return suite;
    }
}
