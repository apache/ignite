/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.metric.IoStatisticsCachePersistenceSelfTest;
import org.apache.ignite.internal.metric.IoStatisticsCacheSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGetCustomCollectionsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLoadRebalanceEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicPrimarySyncBackPressureTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheRemoveWithTombstonesLoadTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheRemoveWithTombstonesTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxCachePrimarySyncTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxConcurrentRemoveObjectsTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.CacheRemoveWithTombstonesFailoverTest;
import org.apache.ignite.internal.processors.cache.transactions.TxCrossCachePartitionConsistencyTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateConsistencyHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateConsistencyVolatileRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryOneBackupHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryOneBackupTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsFailAllHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateOnePrimaryTwoBackupsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStatePutTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateTwoPrimaryTwoBackupsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateConsistencyTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPartitionCounterStateWithFilterTest;
import org.apache.ignite.internal.stat.IoStatisticsManagerSelfTest;
import org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMXBeanCachePersistenceSelfTest;
import org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMXBeanCacheSelfTest;
import org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMXBeanImplSelfTest;
import org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMxBeanCacheGroupsTest;
import org.apache.ignite.internal.stat.IoStatisticsMetricsLocalMxBeanImplIllegalArgumentsTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheMvccTestSuite9 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Collection<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(IgniteTxConcurrentRemoveObjectsTest.class);

        // Non supported modes.
        ignoredTests.add(IgniteCachePrimarySyncTest.class);
        ignoredTests.add(IgniteTxCachePrimarySyncTest.class);

        // Atomic caches.
        ignoredTests.add(CacheAtomicPrimarySyncBackPressureTest.class);

        // Other non-tx tests.
        ignoredTests.add(IgniteCacheGetCustomCollectionsSelfTest.class);
        ignoredTests.add(IgniteCacheLoadRebalanceEvictionSelfTest.class);

        // Non-mvcc counters and history rebalance.
        ignoredTests.add(TxPartitionCounterStateOnePrimaryOneBackupHistoryRebalanceTest.class);
        ignoredTests.add(TxPartitionCounterStateOnePrimaryOneBackupTest.class);
        ignoredTests.add(TxPartitionCounterStateOnePrimaryTwoBackupsFailAllHistoryRebalanceTest.class);
        ignoredTests.add(TxPartitionCounterStateOnePrimaryTwoBackupsFailAllTest.class);
        ignoredTests.add(TxPartitionCounterStateOnePrimaryTwoBackupsHistoryRebalanceTest.class);
        ignoredTests.add(TxPartitionCounterStateOnePrimaryTwoBackupsTest.class);
        ignoredTests.add(TxPartitionCounterStatePutTest.class);
        ignoredTests.add(TxPartitionCounterStateTwoPrimaryTwoBackupsTest.class);
        ignoredTests.add(TxPartitionCounterStateWithFilterTest.class);
        ignoredTests.add(TxPartitionCounterStateConsistencyTest.class);
        ignoredTests.add(TxPartitionCounterStateConsistencyHistoryRebalanceTest.class);
        ignoredTests.add(TxPartitionCounterStateConsistencyVolatileRebalanceTest.class);
        ignoredTests.add(TxCrossCachePartitionConsistencyTest.class);

        // IO statistics.
        ignoredTests.add(IoStatisticsCacheSelfTest.class);
        ignoredTests.add(IoStatisticsCachePersistenceSelfTest.class);
        ignoredTests.add(IoStatisticsManagerSelfTest.class);
        ignoredTests.add(IoStatisticsMetricsLocalMXBeanImplSelfTest.class);
        ignoredTests.add(IoStatisticsMetricsLocalMxBeanImplIllegalArgumentsTest.class);
        ignoredTests.add(IoStatisticsMetricsLocalMxBeanCacheGroupsTest.class);
        ignoredTests.add(IoStatisticsMetricsLocalMXBeanCacheSelfTest.class);
        ignoredTests.add(IoStatisticsMetricsLocalMXBeanCachePersistenceSelfTest.class);

        // Compatibility metrics
        ignoredTests.add(org.apache.ignite.internal.metric.IoStatisticsMetricsLocalMXBeanImplSelfTest.class);
        ignoredTests.add(org.apache.ignite.internal.metric.IoStatisticsMetricsLocalMxBeanCacheGroupsTest.class);

        // Tombstones are not created with mvcc.
        ignoredTests.add(CacheRemoveWithTombstonesTest.class);
        ignoredTests.add(CacheRemoveWithTombstonesLoadTest.class);
        ignoredTests.add(CacheRemoveWithTombstonesFailoverTest.class);

        return new ArrayList<>(IgniteCacheTestSuite9.suite(ignoredTests));
    }
}
