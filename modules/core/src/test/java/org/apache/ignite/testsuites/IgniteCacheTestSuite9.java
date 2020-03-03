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
import org.apache.ignite.internal.metric.CacheMetricsAddRemoveTest;
import org.apache.ignite.internal.metric.IoStatisticsCachePersistenceSelfTest;
import org.apache.ignite.internal.metric.IoStatisticsCacheSelfTest;
import org.apache.ignite.internal.metric.IoStatisticsMetricsLocalMXBeanImplSelfTest;
import org.apache.ignite.internal.metric.IoStatisticsSelfTest;
import org.apache.ignite.internal.metric.JmxExporterSpiTest;
import org.apache.ignite.internal.metric.LogExporterSpiTest;
import org.apache.ignite.internal.metric.MetricsConfigurationTest;
import org.apache.ignite.internal.metric.MetricsSelfTest;
import org.apache.ignite.internal.metric.ReadMetricsOnNodeStartupTest;
import org.apache.ignite.internal.metric.SystemViewComputeJobTest;
import org.apache.ignite.internal.metric.SystemViewSelfTest;
import org.apache.ignite.internal.processors.cache.CachePutIfAbsentTest;
import org.apache.ignite.internal.processors.cache.GridCacheLongRunningTransactionDiagnosticsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGetCustomCollectionsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLoadRebalanceEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicPrimarySyncBackPressureTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheOperationsInterruptTest;
import org.apache.ignite.internal.processors.cache.distributed.FailBackupOnAtomicOperationTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCacheWriteSynchronizationModesMultithreadedTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxConcurrentRemoveObjectsTest;
import org.apache.ignite.internal.processors.cache.transactions.PartitionUpdateCounterTest;
import org.apache.ignite.internal.processors.cache.transactions.TxCrossCachePartitionConsistencyTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDataConsistencyOnCommitFailureTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateConsistencyHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateConsistencyTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateConsistencyVolatileRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryOneBackupHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryOneBackupTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsFailAllHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStatePutTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateTwoPrimaryTwoBackupsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateWithFilterTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite9 {
    /**
     * @return IgniteCache test suite.
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

        GridTestUtils.addTestIfNeeded(suite, IgniteCacheGetCustomCollectionsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCacheLoadRebalanceEvictionSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteCachePrimarySyncTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTxCachePrimarySyncTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteTxCacheWriteSynchronizationModesMultithreadedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CachePutIfAbsentTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheAtomicPrimarySyncBackPressureTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, IgniteTxConcurrentRemoveObjectsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TxDataConsistencyOnCommitFailureTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheOperationsInterruptTest.class, ignoredTests);

        // Update counters and historical rebalance.
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryOneBackupHistoryRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryOneBackupTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryTwoBackupsFailAllHistoryRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateOnePrimaryTwoBackupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStatePutTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateTwoPrimaryTwoBackupsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateWithFilterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PartitionUpdateCounterTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateConsistencyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateConsistencyHistoryRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxPartitionCounterStateConsistencyVolatileRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxCrossCachePartitionConsistencyTest.class, ignoredTests);

        // IO statistics.
        GridTestUtils.addTestIfNeeded(suite, IoStatisticsCachePersistenceSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IoStatisticsCacheSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IoStatisticsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IoStatisticsMetricsLocalMXBeanImplSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MetricsSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MetricsConfigurationTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SystemViewSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, SystemViewComputeJobTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheMetricsAddRemoveTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, JmxExporterSpiTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, LogExporterSpiTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ReadMetricsOnNodeStartupTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheLongRunningTransactionDiagnosticsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, FailBackupOnAtomicOperationTest.class, ignoredTests);

        return suite;
    }
}
