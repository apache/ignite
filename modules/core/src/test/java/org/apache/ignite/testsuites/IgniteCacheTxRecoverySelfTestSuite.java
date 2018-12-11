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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxPessimisticOriginatingNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledTxOriginatingNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedTxOriginatingNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheCommitDelayTxRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedNearDisabledPrimaryNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedPrimaryNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedTwoBackupsPrimaryNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheTxRecoveryRollbackTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.TxRecoveryStoreEnabledTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearTxPessimisticOriginatingNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxOriginatingNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxPessimisticOriginatingNodeFailureSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Tx recovery self test suite.
 */
@RunWith(AllTests.class)
public class IgniteCacheTxRecoverySelfTestSuite {
    /**
     * @return Cache API test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Cache tx recovery test suite");

        suite.addTest(new JUnit4TestAdapter(IgniteCacheCommitDelayTxRecoveryTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedPrimaryNodeFailureRecoveryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedNearDisabledPrimaryNodeFailureRecoveryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCachePartitionedTwoBackupsPrimaryNodeFailureRecoveryTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedTxOriginatingNodeFailureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNearDisabledTxOriginatingNodeFailureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedTxOriginatingNodeFailureSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheColocatedTxPessimisticOriginatingNodeFailureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearTxPessimisticOriginatingNodeFailureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedTxPessimisticOriginatingNodeFailureSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxRecoveryRollbackTest.class));
        suite.addTest(new JUnit4TestAdapter(TxRecoveryStoreEnabledTest.class));

        return suite;
    }
}
