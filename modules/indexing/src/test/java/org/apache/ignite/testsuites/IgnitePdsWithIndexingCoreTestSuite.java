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
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsAtomicCacheRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsBinaryMetadataOnClusterRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsMarshallerMappingRestoreOnNodeStartTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsTxCacheRebalancingTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePersistentStoreCacheGroupsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsMultiNodePutGetRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsPageEvictionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsCacheIntegrationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsNoActualWalHistoryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsThreadInterruptionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRecoveryPPCTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRecoveryTxLogicalRecordsTest;

/**
 * Test suite for tests that cover core PDS features and depend on indexing module.
 */
public class IgnitePdsWithIndexingCoreTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Persistent Store With Indexing Test Suite");

        suite.addTestSuite(IgnitePdsCacheIntegrationTest.class);
        suite.addTestSuite(IgnitePdsPageEvictionTest.class);
        suite.addTestSuite(IgnitePdsMultiNodePutGetRestartTest.class);
        suite.addTestSuite(IgnitePersistentStoreCacheGroupsTest.class);
        suite.addTestSuite(WalRecoveryTxLogicalRecordsTest.class);

        suite.addTestSuite(IgniteWalRecoveryTest.class);
        suite.addTestSuite(IgnitePdsNoActualWalHistoryTest.class);
        suite.addTestSuite(IgnitePdsAtomicCacheRebalancingTest.class);
        suite.addTestSuite(IgnitePdsTxCacheRebalancingTest.class);

        suite.addTestSuite(IgniteWalRecoveryPPCTest.class);

        suite.addTestSuite(IgnitePdsBinaryMetadataOnClusterRestartTest.class);
        suite.addTestSuite(IgnitePdsMarshallerMappingRestoreOnNodeStartTest.class);
        suite.addTestSuite(IgnitePdsThreadInterruptionTest.class);

        return suite;
    }
}
