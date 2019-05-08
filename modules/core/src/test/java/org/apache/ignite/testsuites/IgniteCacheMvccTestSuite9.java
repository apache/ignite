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

import java.util.Collection;
import java.util.HashSet;
import junit.framework.TestSuite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.IgniteCacheGetCustomCollectionsSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLoadRebalanceEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAtomicPrimarySyncBackPressureTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxConcurrentRemoveObjectsTest;
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({})
//@RunWith(IgniteCacheMvccTestSuite9.DynamicSuite.class)
public class IgniteCacheMvccTestSuite9 {
    /**
     * @return IgniteCache test suite.
     */
    public static TestSuite suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Collection<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(IgniteTxConcurrentRemoveObjectsTest.class);

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

        TestSuite suite = new TestSuite("IgniteCache Mvcc Test Suite part 9");

        suite.addTest(IgniteCacheTestSuite9.suite(ignoredTests));

        return suite;
    }
}
