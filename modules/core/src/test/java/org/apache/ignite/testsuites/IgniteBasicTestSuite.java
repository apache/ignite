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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

/**
 * Basic test suite.
 */
@RunWith(IgniteBasicTestSuite.DynamicSuite.class)
public class IgniteBasicTestSuite {
    /**
     * @return Test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        suite.addAll(IgniteMarshallerSelfTestSuite.suite(ignoredTests));
        suite.addAll(IgniteLangSelfTestSuite.suite());
        suite.addAll(IgniteUtilSelfTestSuite.suite(ignoredTests));

        suite.addAll(IgniteKernalSelfTestSuite.suite(ignoredTests));
        suite.addAll(IgniteStartUpTestSuite.suite());
        suite.addAll(IgniteExternalizableSelfTestSuite.suite());
        suite.addAll(IgniteP2PSelfTestSuite.suite(ignoredTests));
        suite.addAll(IgniteCacheP2pUnmarshallingErrorTestSuite.suite(ignoredTests));
        suite.addAll(IgniteStreamSelfTestSuite.suite());

        suite.addAll(IgnitePlatformsTestSuite.suite());

        suite.add(GridSelfTest.class);
        suite.add(ClusterGroupHostsSelfTest.class);
        suite.add(IgniteMessagingWithClientTest.class);
        suite.add(IgniteMessagingSendAsyncTest.class);

        GridTestUtils.addTestIfNeeded(suite, ClusterGroupSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridMessagingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridMessagingNoPeerClassLoadingSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridReleaseTypeSelfTest.class, ignoredTests);
        suite.add(GridProductVersionSelfTest.class);
        suite.add(GridAffinityProcessorRendezvousSelfTest.class);
        suite.add(GridAffinityProcessorMemoryLeakTest.class);
        suite.add(GridClosureProcessorSelfTest.class);
        suite.add(GridClosureProcessorRemoteTest.class);
        suite.add(GridClosureSerializationTest.class);
        suite.add(GridStartStopSelfTest.class);
        suite.add(GridProjectionForCachesSelfTest.class);
        suite.add(GridProjectionForCachesOnDaemonNodeSelfTest.class);
        suite.add(GridSpiLocalHostInjectionTest.class);
        suite.add(GridLifecycleBeanSelfTest.class);
        suite.add(GridStopWithCancelSelfTest.class);
        suite.add(GridReduceSelfTest.class);
        suite.add(GridEventConsumeSelfTest.class);
        suite.add(GridSuppressedExceptionSelfTest.class);
        suite.add(GridLifecycleAwareSelfTest.class);
        suite.add(GridMessageListenSelfTest.class);
        suite.add(GridFailFastNodeFailureDetectionSelfTest.class);
        suite.add(IgniteSlowClientDetectionSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, IgniteDaemonNodeMarshallerCacheTest.class, ignoredTests);
        suite.add(IgniteMarshallerCacheConcurrentReadWriteTest.class);
        suite.add(GridNodeMetricsLogSelfTest.class);
        suite.add(GridLocalIgniteSerializationTest.class);
        suite.add(GridMBeansTest.class);
        suite.add(TransactionsMXBeanImplTest.class);
        suite.add(SetTxTimeoutOnPartitionMapExchangeTest.class);

        suite.add(IgniteExceptionInNioWorkerSelfTest.class);
        suite.add(IgniteLocalNodeMapBeforeStartTest.class);
        suite.add(OdbcConfigurationValidationSelfTest.class);
        suite.add(OdbcEscapeSequenceSelfTest.class);

        GridTestUtils.addTestIfNeeded(suite, DynamicProxySerializationMultiJvmSelfTest.class, ignoredTests);

        // Tests against configuration variations framework.
        suite.add(ParametersTest.class);
        suite.add(VariationsIteratorTest.class);
        suite.add(ConfigVariationsTestSuiteBuilderTest.class);
        suite.add(NotStringSystemPropertyTest.class);

        suite.add(MarshallerContextLockingSelfTest.class);
        suite.add(MarshallerContextSelfTest.class);

        suite.add(SecurityPermissionSetBuilderTest.class);

        suite.add(AttributeNodeFilterSelfTest.class);

        // Basic DB data structures.
        suite.add(BPlusTreeSelfTest.class);
        suite.add(BPlusTreeFakeReuseSelfTest.class);
        suite.add(BPlusTreeReuseSelfTest.class);
        suite.add(IndexStorageSelfTest.class);
        suite.add(CacheFreeListImplSelfTest.class);
        suite.add(DataRegionMetricsSelfTest.class);
        suite.add(SwapPathConstructionSelfTest.class);

        suite.add(IgniteMarshallerCacheFSRestoreTest.class);
        suite.add(IgniteMarshallerCacheClassNameConflictTest.class);
        suite.add(IgniteMarshallerCacheClientRequestsMappingOnMissTest.class);

        suite.add(IgniteDiagnosticMessagesTest.class);
        suite.add(IgniteDiagnosticMessagesMultipleConnectionsTest.class);

        suite.add(IgniteRejectConnectOnNodeStopTest.class);

        suite.add(GridCleanerTest.class);

        suite.add(ClassSetTest.class);

        // Basic failure handlers.
        suite.add(FailureHandlerTriggeredTest.class);
        suite.add(StopNodeFailureHandlerTest.class);
        suite.add(StopNodeOrHaltFailureHandlerTest.class);
        suite.add(OomFailureHandlerTest.class);
        suite.add(TransactionIntegrityWithSystemWorkerDeathTest.class);

        suite.add(AtomicOperationsInTxTest.class);

        suite.add(CacheRebalanceConfigValidationTest.class);

        suite.add(ListeningTestLoggerTest.class);

        return suite;
    }

    /** */
    public static class DynamicSuite extends Suite {
        /** */
        public DynamicSuite(Class<?> cls) throws InitializationError {
            super(cls, suite().toArray(new Class<?>[] {null}));
        }
    }
}
