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

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccClusterRestartTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccConfigurationValidationTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccIteratorWithConcurrentTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccLocalEntriesWithConcurrentTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccOperationChecksTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccProcessorTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccScanQueryWithConcurrentTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSizeWithConcurrentTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTransactionsTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccVacuumTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorMvccSelfTest;

/**
 *
 */
public class IgniteCacheMvccTestSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("IgniteCache MVCC Test Suite");

        // Basic tests.
        suite.addTestSuite(CacheMvccTransactionsTest.class);
        suite.addTestSuite(CacheMvccProcessorTest.class);
        suite.addTestSuite(CacheMvccVacuumTest.class);
        suite.addTestSuite(CacheMvccConfigurationValidationTest.class);

        suite.addTestSuite(DataStreamProcessorMvccSelfTest.class);
        suite.addTestSuite(CacheMvccOperationChecksTest.class);

        // Concurrent ops tests.
        suite.addTestSuite(CacheMvccIteratorWithConcurrentTransactionTest.class);
        suite.addTestSuite(CacheMvccLocalEntriesWithConcurrentTransactionTest.class);
        suite.addTestSuite(CacheMvccScanQueryWithConcurrentTransactionTest.class);
        suite.addTestSuite(CacheMvccSizeWithConcurrentTransactionTest.class);

        // Failover tests.
        suite.addTestSuite(CacheMvccClusterRestartTest.class);
        suite.addTestSuite(CacheMvccPartitionedCoordinatorFailoverTest.class);
        suite.addTestSuite(CacheMvccReplicatedCoordinatorFailoverTest.class);

        return suite;
    }
}
