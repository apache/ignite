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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxPeekModesTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTxReplicatedPeekModesTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccClusterRestartTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccConfigurationValidationTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccIteratorWithConcurrentTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccLocalEntriesWithConcurrentTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccOperationChecksTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccProcessorLazyStartTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccProcessorTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccRemoteTxOnNearNodeStartTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccScanQueryWithConcurrentTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSizeWithConcurrentTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTransactionsTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxFailoverTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccVacuumTest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCachePeekTest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUnsupportedTxModesTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorMvccPersistenceSelfTest;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorMvccSelfTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

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
        suite.addTest(new JUnit4TestAdapter(CacheMvccTransactionsTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccProcessorTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccVacuumTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccConfigurationValidationTest.class));

        suite.addTest(new JUnit4TestAdapter(DataStreamProcessorMvccSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(DataStreamProcessorMvccPersistenceSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccOperationChecksTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheMvccRemoteTxOnNearNodeStartTest.class));

        suite.addTest(new JUnit4TestAdapter(MvccUnsupportedTxModesTest.class));

        suite.addTest(new JUnit4TestAdapter(MvccCachePeekTest.class));

        suite.addTest(new JUnit4TestAdapter(MvccIgniteCacheTxPeekModesTest.class));
        suite.addTest(new JUnit4TestAdapter(MvccIgniteCacheTxReplicatedPeekModesTest.class));

        // Concurrent ops tests.
        suite.addTest(new JUnit4TestAdapter(CacheMvccIteratorWithConcurrentTransactionTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccLocalEntriesWithConcurrentTransactionTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccScanQueryWithConcurrentTransactionTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccSizeWithConcurrentTransactionTest.class));

        // Failover tests.
        suite.addTest(new JUnit4TestAdapter(CacheMvccTxFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccClusterRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccPartitionedCoordinatorFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccReplicatedCoordinatorFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccProcessorLazyStartTest.class));

        return suite;
    }

    /** */
    public static class MvccIgniteCacheTxPeekModesTest extends IgniteCacheTxPeekModesTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }

        /** {@inheritDoc} */
        @Override public void testLocalEntries() throws Exception {
            fail("https://issues.apache.org/jira/browse/IGNITE-10167");

            super.testLocalEntries();
        }
    }

    /** */
    public static class MvccIgniteCacheTxReplicatedPeekModesTest extends IgniteCacheTxReplicatedPeekModesTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }

        /** {@inheritDoc} */
        @Override public void testLocalEntries() throws Exception {
            fail("https://issues.apache.org/jira/browse/IGNITE-10167");

            super.testLocalEntries();
        }
    }
}
