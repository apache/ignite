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
import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.GridSuppressedExceptionSelfTest;
import org.apache.ignite.failure.FailureHandlerTriggeredTest;
import org.apache.ignite.failure.OomFailureHandlerTest;
import org.apache.ignite.failure.StopNodeFailureHandlerTest;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandlerTest;
import org.apache.ignite.internal.ClassSetTest;
import org.apache.ignite.internal.ClusterGroupHostsSelfTest;
import org.apache.ignite.internal.ClusterGroupSelfTest;
import org.apache.ignite.internal.GridFailFastNodeFailureDetectionSelfTest;
import org.apache.ignite.internal.GridLifecycleAwareSelfTest;
import org.apache.ignite.internal.GridLifecycleBeanSelfTest;
import org.apache.ignite.internal.GridMBeansTest;
import org.apache.ignite.internal.GridNodeMetricsLogSelfTest;
import org.apache.ignite.internal.GridProjectionForCachesSelfTest;
import org.apache.ignite.internal.GridReduceSelfTest;
import org.apache.ignite.internal.GridReleaseTypeSelfTest;
import org.apache.ignite.internal.GridSelfTest;
import org.apache.ignite.internal.GridStartStopSelfTest;
import org.apache.ignite.internal.GridStopWithCancelSelfTest;
import org.apache.ignite.internal.IgniteLocalNodeMapBeforeStartTest;
import org.apache.ignite.internal.IgniteSlowClientDetectionSelfTest;
import org.apache.ignite.internal.MarshallerContextLockingSelfTest;
import org.apache.ignite.internal.TransactionsMXBeanImplTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesMultipleConnectionsTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesTest;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessorMemoryLeakTest;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessorRendezvousSelfTest;
import org.apache.ignite.internal.processors.cache.CacheRebalanceConfigValidationTest;
import org.apache.ignite.internal.processors.cache.GridLocalIgniteSerializationTest;
import org.apache.ignite.internal.processors.cache.GridProjectionForCachesOnDaemonNodeSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteDaemonNodeMarshallerCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClassNameConflictTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClientRequestsMappingOnMissTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheConcurrentReadWriteTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheFSRestoreTest;
import org.apache.ignite.internal.processors.cache.SetTxTimeoutOnPartitionMapExchangeTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteRejectConnectOnNodeStopTest;
import org.apache.ignite.internal.processors.cache.transactions.AtomicOperationsInTxTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionIntegrityWithSystemWorkerDeathTest;
import org.apache.ignite.internal.processors.closure.GridClosureProcessorRemoteTest;
import org.apache.ignite.internal.processors.closure.GridClosureProcessorSelfTest;
import org.apache.ignite.internal.processors.closure.GridClosureSerializationTest;
import org.apache.ignite.internal.processors.continuous.GridEventConsumeSelfTest;
import org.apache.ignite.internal.processors.continuous.GridMessageListenSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeFakeReuseSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeReuseSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeSelfTest;
import org.apache.ignite.internal.processors.database.CacheFreeListImplSelfTest;
import org.apache.ignite.internal.processors.database.DataRegionMetricsSelfTest;
import org.apache.ignite.internal.processors.database.IndexStorageSelfTest;
import org.apache.ignite.internal.processors.database.SwapPathConstructionSelfTest;
import org.apache.ignite.internal.processors.odbc.OdbcConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.odbc.OdbcEscapeSequenceSelfTest;
import org.apache.ignite.internal.processors.service.ClosureServiceClientsNodesTest;
import org.apache.ignite.internal.product.GridProductVersionSelfTest;
import org.apache.ignite.internal.util.GridCleanerTest;
import org.apache.ignite.internal.util.nio.IgniteExceptionInNioWorkerSelfTest;
import org.apache.ignite.marshaller.DynamicProxySerializationMultiJvmSelfTest;
import org.apache.ignite.marshaller.MarshallerContextSelfTest;
import org.apache.ignite.messaging.GridMessagingNoPeerClassLoadingSelfTest;
import org.apache.ignite.messaging.GridMessagingSelfTest;
import org.apache.ignite.messaging.IgniteMessagingSendAsyncTest;
import org.apache.ignite.messaging.IgniteMessagingWithClientTest;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilderTest;
import org.apache.ignite.spi.GridSpiLocalHostInjectionTest;
import org.apache.ignite.startup.properties.NotStringSystemPropertyTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.test.ConfigVariationsTestSuiteBuilderTest;
import org.apache.ignite.testframework.test.ListeningTestLoggerTest;
import org.apache.ignite.testframework.test.ParametersTest;
import org.apache.ignite.testframework.test.VariationsIteratorTest;
import org.apache.ignite.util.AttributeNodeFilterSelfTest;
import org.jetbrains.annotations.Nullable;

/**
 * Basic test suite.
 */
public class IgniteBasicTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution. Providing null means nothing to exclude.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(@Nullable final Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("Ignite Basic Test Suite");

        suite.addTest(IgniteMarshallerSelfTestSuite.suite(ignoredTests));
        suite.addTest(IgniteLangSelfTestSuite.suite());
        suite.addTest(IgniteUtilSelfTestSuite.suite(ignoredTests));

        suite.addTest(IgniteKernalSelfTestSuite.suite(ignoredTests));
        suite.addTest(IgniteStartUpTestSuite.suite());
        suite.addTest(IgniteExternalizableSelfTestSuite.suite());
        suite.addTest(IgniteP2PSelfTestSuite.suite(ignoredTests));
        suite.addTest(IgniteCacheP2pUnmarshallingErrorTestSuite.suite(ignoredTests));
        suite.addTest(IgniteStreamSelfTestSuite.suite());

        suite.addTest(IgnitePlatformsTestSuite.suite());

        suite.addTest(new JUnit4TestAdapter(GridSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClusterGroupHostsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteMessagingWithClientTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteMessagingSendAsyncTest.class));

        GridTestUtils.addTestIfNeeded(suite, ClusterGroupSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridMessagingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridMessagingNoPeerClassLoadingSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridReleaseTypeSelfTest.class, ignoredTests);
        suite.addTest(new JUnit4TestAdapter(GridProductVersionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridAffinityProcessorRendezvousSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridAffinityProcessorMemoryLeakTest.class));
        suite.addTest(new JUnit4TestAdapter(GridClosureProcessorSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridClosureProcessorRemoteTest.class));
        suite.addTest(new JUnit4TestAdapter(GridClosureSerializationTest.class));
        suite.addTest(new JUnit4TestAdapter(ClosureServiceClientsNodesTest.class));
        suite.addTest(new JUnit4TestAdapter(GridStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridProjectionForCachesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridProjectionForCachesOnDaemonNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSpiLocalHostInjectionTest.class));
        suite.addTest(new JUnit4TestAdapter(GridLifecycleBeanSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridStopWithCancelSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridReduceSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridEventConsumeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSuppressedExceptionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridLifecycleAwareSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMessageListenSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridFailFastNodeFailureDetectionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteSlowClientDetectionSelfTest.class));
        GridTestUtils.addTestIfNeeded(suite, IgniteDaemonNodeMarshallerCacheTest.class, ignoredTests);
        suite.addTest(new JUnit4TestAdapter(IgniteMarshallerCacheConcurrentReadWriteTest.class));
        suite.addTest(new JUnit4TestAdapter(GridNodeMetricsLogSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridLocalIgniteSerializationTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMBeansTest.class));
        suite.addTest(new JUnit4TestAdapter(TransactionsMXBeanImplTest.class));
        suite.addTest(new JUnit4TestAdapter(SetTxTimeoutOnPartitionMapExchangeTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteExceptionInNioWorkerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteLocalNodeMapBeforeStartTest.class));
        suite.addTest(new JUnit4TestAdapter(OdbcConfigurationValidationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(OdbcEscapeSequenceSelfTest.class));

        GridTestUtils.addTestIfNeeded(suite, DynamicProxySerializationMultiJvmSelfTest.class, ignoredTests);

        // Tests against configuration variations framework.
        suite.addTest(new JUnit4TestAdapter(ParametersTest.class));
        suite.addTest(new JUnit4TestAdapter(VariationsIteratorTest.class));
        suite.addTest(new JUnit4TestAdapter(ConfigVariationsTestSuiteBuilderTest.class));
        suite.addTest(new JUnit4TestAdapter(NotStringSystemPropertyTest.class));

        suite.addTest(new JUnit4TestAdapter(MarshallerContextLockingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(MarshallerContextSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(SecurityPermissionSetBuilderTest.class));

        suite.addTest(new JUnit4TestAdapter(AttributeNodeFilterSelfTest.class));

        // Basic DB data structures.
        suite.addTest(new JUnit4TestAdapter(BPlusTreeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BPlusTreeFakeReuseSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(BPlusTreeReuseSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IndexStorageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheFreeListImplSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DataRegionMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(SwapPathConstructionSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteMarshallerCacheFSRestoreTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteMarshallerCacheClassNameConflictTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteMarshallerCacheClientRequestsMappingOnMissTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteDiagnosticMessagesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDiagnosticMessagesMultipleConnectionsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteRejectConnectOnNodeStopTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCleanerTest.class));

        suite.addTest(new JUnit4TestAdapter(ClassSetTest.class));

        // Basic failure handlers.
        suite.addTest(new JUnit4TestAdapter(FailureHandlerTriggeredTest.class));
        suite.addTest(new JUnit4TestAdapter(StopNodeFailureHandlerTest.class));
        suite.addTest(new JUnit4TestAdapter(StopNodeOrHaltFailureHandlerTest.class));
        suite.addTest(new JUnit4TestAdapter(OomFailureHandlerTest.class));
        suite.addTest(new JUnit4TestAdapter(TransactionIntegrityWithSystemWorkerDeathTest.class));

        suite.addTest(new JUnit4TestAdapter(AtomicOperationsInTxTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheRebalanceConfigValidationTest.class));

        suite.addTest(new JUnit4TestAdapter(ListeningTestLoggerTest.class));

        return suite;
    }
}
