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
import org.apache.ignite.internal.processors.cache.CacheConfigurationSerializationOnDiscoveryTest;
import org.apache.ignite.internal.processors.cache.CacheConfigurationSerializationOnExchangeTest;
import org.apache.ignite.internal.processors.cache.GridTransactionsSystemUserTimeMetricsTest;
import org.apache.ignite.internal.processors.cache.SafeLogTxFinishErrorTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheDataLossOnPartitionMoveTest;
import org.apache.ignite.internal.processors.cache.distributed.CachePartitionLossWithPersistenceTest;
import org.apache.ignite.internal.processors.cache.distributed.CachePartitionLostAfterSupplierHasLeftTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GracefulShutdownTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadDelayedWithPersistenceSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadWaitForBackupsWithPersistenceTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.WaitForBackupsOnShutdownSystemPropertyTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.BlockedEvictionsTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.DelayedOwningDuringExchangeTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.EvictionWhilePartitionGroupIsReservedTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.MovingPartitionIsEvictedDuringClearingTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PreloadingRestartWhileClearingPartitionTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.RentingPartitionIsOwnedDuringEvictionTest;
import org.apache.ignite.internal.processors.cache.persistence.IgniteLostPartitionsOnLeaveBaselineSelfTest;
import org.apache.ignite.internal.processors.cache.transactions.AtomicPartitionCounterStateConsistencyHistoryRebalanceTest;
import org.apache.ignite.internal.processors.cache.transactions.AtomicPartitionCounterStateConsistencyTest;
import org.apache.ignite.internal.processors.cache.transactions.AtomicVolatilePartitionCounterStateConsistencyTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionIntegrityWithPrimaryIndexCorruptionTest;
import org.apache.ignite.internal.processors.cache.transactions.TxCrossCacheMapOnInvalidTopologyTest;
import org.apache.ignite.internal.processors.cache.transactions.TxCrossCacheRemoteMultiplePartitionReservationTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRecoveryConcurrentOnPrimaryFailTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRecoveryWithConcurrentRollbackTest;
import org.apache.ignite.internal.processors.cache.transactions.TxWithKeyContentionSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite12 {
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

        GridTestUtils.addTestIfNeeded(suite, TransactionIntegrityWithPrimaryIndexCorruptionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheDataLossOnPartitionMoveTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CacheConfigurationSerializationOnDiscoveryTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheConfigurationSerializationOnExchangeTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CachePartitionLostAfterSupplierHasLeftTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CachePartitionLossWithPersistenceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, IgniteLostPartitionsOnLeaveBaselineSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TxCrossCacheMapOnInvalidTopologyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxCrossCacheRemoteMultiplePartitionReservationTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridTransactionsSystemUserTimeMetricsTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, SafeLogTxFinishErrorTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, TxRecoveryConcurrentOnPrimaryFailTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxRecoveryWithConcurrentRollbackTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, AtomicPartitionCounterStateConsistencyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AtomicPartitionCounterStateConsistencyHistoryRebalanceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, AtomicVolatilePartitionCounterStateConsistencyTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadWaitForBackupsWithPersistenceTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GracefulShutdownTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, WaitForBackupsOnShutdownSystemPropertyTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, TxWithKeyContentionSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheDhtPreloadDelayedWithPersistenceSelfTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, RentingPartitionIsOwnedDuringEvictionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, BlockedEvictionsTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, PreloadingRestartWhileClearingPartitionTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, MovingPartitionIsEvictedDuringClearingTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, EvictionWhilePartitionGroupIsReservedTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, DelayedOwningDuringExchangeTest.class, ignoredTests);

        return suite;
    }
}
