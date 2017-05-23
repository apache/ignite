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
import org.apache.ignite.cache.database.IgnitePersistentStoreCacheGroupsTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreClientNearCachePutGetWithPersistenceSelfTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreDynamicCacheTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreSingleNodePutGetPersistenceSelfTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreSingleNodeWithIndexingAndGroupPutGetPersistenceSelfTest;
import org.apache.ignite.cache.database.IgnitePersistentStoreSingleNodeWithIndexingPutGetPersistenceSelfTest;
import org.apache.ignite.cache.database.db.IgniteDbMultiNodePutGetRestartSelfTest;
import org.apache.ignite.cache.database.db.IgniteDbPageEvictionSelfTest;
import org.apache.ignite.cache.database.db.file.IgniteCachePageStoreIntegrationSelfTest;
import org.apache.ignite.cache.database.db.file.IgniteWalDirectoriesConfigurationTest;
import org.apache.ignite.cache.database.db.file.PageStoreCheckpointSimulationSelfTest;
import org.apache.ignite.cache.database.db.file.PageStoreEvictionSelfTest;
import org.apache.ignite.cache.database.pagemem.BPlusTreeReuseListPageMemoryImplSelfTest;
import org.apache.ignite.cache.database.pagemem.BPlusTreeSelfTestPageMemoryImplSelfTest;
import org.apache.ignite.cache.database.pagemem.MetadataStoragePageMemoryImplSelfTest;
import org.apache.ignite.cache.database.pagemem.PageMemoryImplNoLoadSelfTest;
import org.apache.ignite.cache.database.pagemem.PageMemoryImplTest;
import org.apache.ignite.internal.processors.database.IgniteDbClientNearCachePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbDynamicCacheSelfTest;
import org.apache.ignite.internal.processors.database.IgniteDbMultiNodePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbMultiNodeWithIndexingPutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeTinyPutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeWithIndexingPutGetTest;


/**
 *
 */
public class IgnitePdsTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite");

        // Basic PageMemory tests.
        suite.addTestSuite(PageMemoryImplNoLoadSelfTest.class);
        suite.addTestSuite(MetadataStoragePageMemoryImplSelfTest.class);
        suite.addTestSuite(PageStoreEvictionSelfTest.class);
        suite.addTestSuite(PageMemoryImplTest.class);

        // Checkpointing smoke-test.
        suite.addTestSuite(IgniteCachePageStoreIntegrationSelfTest.class);
        suite.addTestSuite(PageStoreCheckpointSimulationSelfTest.class);

        // BTree tests with store page memory.
        suite.addTestSuite(BPlusTreeSelfTestPageMemoryImplSelfTest.class);
        suite.addTestSuite(BPlusTreeReuseListPageMemoryImplSelfTest.class);

        // Basic API tests.
        suite.addTestSuite(IgniteDbSingleNodePutGetTest.class);
        suite.addTestSuite(IgniteDbSingleNodeWithIndexingPutGetTest.class);
        suite.addTestSuite(IgniteDbMultiNodePutGetTest.class);
        suite.addTestSuite(IgniteDbMultiNodeWithIndexingPutGetTest.class);
        suite.addTestSuite(IgniteDbSingleNodeTinyPutGetTest.class);
        suite.addTestSuite(IgniteDbDynamicCacheSelfTest.class);
        suite.addTestSuite(IgniteDbClientNearCachePutGetTest.class);

        // Persistence-enabled.
        suite.addTestSuite(IgniteDbMultiNodePutGetRestartSelfTest.class);
        suite.addTestSuite(IgnitePersistentStoreSingleNodePutGetPersistenceSelfTest.class);
        suite.addTestSuite(IgnitePersistentStoreSingleNodeWithIndexingPutGetPersistenceSelfTest.class);
        suite.addTestSuite(IgnitePersistentStoreSingleNodeWithIndexingAndGroupPutGetPersistenceSelfTest.class);
        suite.addTestSuite(IgniteDbPageEvictionSelfTest.class);
        suite.addTestSuite(IgnitePersistentStoreDynamicCacheTest.class);
        suite.addTestSuite(IgniteWalDirectoriesConfigurationTest.class);
        suite.addTestSuite(IgnitePersistentStoreClientNearCachePutGetWithPersistenceSelfTest.class);

        suite.addTestSuite(IgnitePersistentStoreCacheGroupsTest.class);

        return suite;
    }
}
