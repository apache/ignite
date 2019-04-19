/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.cache.IgnitePdsSingleNodeWithIndexingAndGroupPutGetPersistenceSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePdsSingleNodeWithIndexingPutGetPersistenceTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteTcBotInitNewPageTest;
import org.apache.ignite.internal.processors.database.IgniteDbMultiNodeWithIndexingPutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeWithIndexingPutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeWithIndexingWalRestoreTest;
import org.apache.ignite.internal.processors.database.IgnitePersistentStoreQueryWithMultipleClassesPerCacheTest;
import org.apache.ignite.internal.processors.database.IgnitePersistentStoreSchemaLoadTest;
import org.apache.ignite.internal.processors.database.IgniteTwoRegionsRebuildIndexTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IndexingMultithreadedLoadContinuousRestartTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 *
 */
@RunWith(AllTests.class)
public class IgnitePdsWithIndexingTestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Db Memory Leaks With Indexing Test Suite");

        suite.addTest(new JUnit4TestAdapter(IgniteDbSingleNodeWithIndexingWalRestoreTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDbSingleNodeWithIndexingPutGetTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteDbMultiNodeWithIndexingPutGetTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsSingleNodeWithIndexingPutGetPersistenceTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePdsSingleNodeWithIndexingAndGroupPutGetPersistenceSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePersistentStoreSchemaLoadTest.class));
        suite.addTest(new JUnit4TestAdapter(IgnitePersistentStoreQueryWithMultipleClassesPerCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTwoRegionsRebuildIndexTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTcBotInitNewPageTest.class));
        suite.addTest(new JUnit4TestAdapter(IndexingMultithreadedLoadContinuousRestartTest.class));

        return suite;
    }
}
