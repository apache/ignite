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
import org.apache.ignite.internal.processors.cache.IgniteClusterActivateDeactivateTestWithPersistence;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsClientNearCachePutGetTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsDynamicCacheTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsSingleNodePutGetPersistenceTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsCacheRestoreTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.DefaultPageSizeBackwardsCompatibilityTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsCheckpointSimulationWithRealCpDisabledTest;
import org.apache.ignite.internal.processors.cache.persistence.db.file.IgnitePdsEvictionTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.BPlusTreePageMemoryImplTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.BPlusTreeReuseListPageMemoryImplTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.MetadataStoragePageMemoryImplTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImplNoLoadTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImplTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteThrottleSmokeTest;
import org.apache.ignite.internal.processors.database.IgniteDbClientNearCachePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbDynamicCacheSelfTest;
import org.apache.ignite.internal.processors.database.IgniteDbMultiNodePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodePutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeTinyPutGetTest;


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
        suite.addTestSuite(PageMemoryImplNoLoadTest.class);
        suite.addTestSuite(MetadataStoragePageMemoryImplTest.class);
        suite.addTestSuite(IgnitePdsEvictionTest.class);
        suite.addTestSuite(PageMemoryImplTest.class);

        // Checkpointing smoke-test.
        suite.addTestSuite(IgnitePdsCheckpointSimulationWithRealCpDisabledTest.class);

        // BTree tests with store page memory.
        suite.addTestSuite(BPlusTreePageMemoryImplTest.class);
        suite.addTestSuite(BPlusTreeReuseListPageMemoryImplTest.class);

        // Basic API tests.
        suite.addTestSuite(IgniteDbSingleNodePutGetTest.class);
        suite.addTestSuite(IgniteDbMultiNodePutGetTest.class);
        suite.addTestSuite(IgniteDbSingleNodeTinyPutGetTest.class);
        suite.addTestSuite(IgniteDbDynamicCacheSelfTest.class);
        suite.addTestSuite(IgniteDbClientNearCachePutGetTest.class);

        // Persistence-enabled.
        suite.addTestSuite(IgnitePdsSingleNodePutGetPersistenceTest.class);
        suite.addTestSuite(IgnitePdsDynamicCacheTest.class);
        suite.addTestSuite(IgnitePdsClientNearCachePutGetTest.class);

        suite.addTestSuite(IgniteClusterActivateDeactivateTestWithPersistence.class);

        suite.addTestSuite(IgnitePdsCacheRestoreTest.class);

        suite.addTestSuite(DefaultPageSizeBackwardsCompatibilityTest.class);

        // Write throttling
        suite.addTestSuite(PagesWriteThrottleSmokeTest.class);

        return suite;
    }
}
