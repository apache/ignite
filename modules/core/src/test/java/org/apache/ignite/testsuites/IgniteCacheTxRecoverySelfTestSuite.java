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
import org.junit.runners.Suite;

/**
 * Tx recovery self test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteCacheCommitDelayTxRecoveryTest.class,

    IgniteCachePartitionedPrimaryNodeFailureRecoveryTest.class,
    IgniteCachePartitionedNearDisabledPrimaryNodeFailureRecoveryTest.class,
    IgniteCachePartitionedTwoBackupsPrimaryNodeFailureRecoveryTest.class,

    GridCachePartitionedTxOriginatingNodeFailureSelfTest.class,
    GridCachePartitionedNearDisabledTxOriginatingNodeFailureSelfTest.class,
    GridCacheReplicatedTxOriginatingNodeFailureSelfTest.class,

    GridCacheColocatedTxPessimisticOriginatingNodeFailureSelfTest.class,
    GridCacheNearTxPessimisticOriginatingNodeFailureSelfTest.class,
    GridCacheReplicatedTxPessimisticOriginatingNodeFailureSelfTest.class,

    IgniteCacheTxRecoveryRollbackTest.class,
    TxRecoveryStoreEnabledTest.class
})
public class IgniteCacheTxRecoverySelfTestSuite {
}
