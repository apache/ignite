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
