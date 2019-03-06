/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.PartitionedAtomicCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionedMvccTxPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionedTransactionalOptimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionedTransactionalPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionsExchangeCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.ReplicatedAtomicCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.ReplicatedMvccTxPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.ReplicatedTransactionalOptimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.ReplicatedTransactionalPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteExchangeLatchManagerCoordinatorFailTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheExchangeMergeTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheParallelStartTest;
import org.apache.ignite.internal.processors.cache.distributed.ExchangeMergeStaleServerNodesTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionEvictionDuringReadThroughSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCache150ClientsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteOptimisticTxSuspendResumeTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticOnPartitionExchangeTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticPrepareOnUnstableTopologyTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackOnTimeoutOnePhaseCommitTest;
import org.apache.ignite.internal.processors.cache.transactions.TxStateChangeEventTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheMvccTestSuite6 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Set<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(TxStateChangeEventTest.class);

        // Atomic cache tests.
        ignoredTests.add(ReplicatedAtomicCacheGetsDistributionTest.class);
        ignoredTests.add(PartitionedAtomicCacheGetsDistributionTest.class);
        ignoredTests.add(GridCachePartitionEvictionDuringReadThroughSelfTest.class);

        // Irrelevant Tx tests.
        ignoredTests.add(IgniteOptimisticTxSuspendResumeTest.class);
        ignoredTests.add(TxOptimisticPrepareOnUnstableTopologyTest.class);
        ignoredTests.add(ReplicatedTransactionalOptimisticCacheGetsDistributionTest.class);
        ignoredTests.add(PartitionedTransactionalOptimisticCacheGetsDistributionTest.class);
        ignoredTests.add(TxOptimisticOnPartitionExchangeTest.class);

        ignoredTests.add(TxRollbackOnTimeoutOnePhaseCommitTest.class);

        // Other non-tx tests.
        ignoredTests.add(CacheExchangeMergeTest.class);
        ignoredTests.add(ExchangeMergeStaleServerNodesTest.class);
        ignoredTests.add(IgniteExchangeLatchManagerCoordinatorFailTest.class);
        ignoredTests.add(PartitionsExchangeCoordinatorFailoverTest.class);
        ignoredTests.add(CacheParallelStartTest.class);
        ignoredTests.add(IgniteCache150ClientsTest.class);

        // Skip tests that has Mvcc clones.
        ignoredTests.add(PartitionedTransactionalPessimisticCacheGetsDistributionTest.class); // See PartitionedMvccTxPessimisticCacheGetsDistributionTest.
        ignoredTests.add(ReplicatedTransactionalPessimisticCacheGetsDistributionTest.class); //See ReplicatedMvccTxPessimisticCacheGetsDistributionTest

        List<Class<?>> suite = new ArrayList<>((IgniteCacheTestSuite6.suite(ignoredTests)));

        // Add mvcc versions for skipped tests.
        suite.add(PartitionedMvccTxPessimisticCacheGetsDistributionTest.class);
        suite.add(ReplicatedMvccTxPessimisticCacheGetsDistributionTest.class);

        return suite;
    }
}
