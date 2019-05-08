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
import org.apache.ignite.internal.processors.cache.IgnitePdsSingleNodeWithIndexingAndGroupPutGetPersistenceSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePdsSingleNodeWithIndexingPutGetPersistenceTest;
import org.apache.ignite.internal.processors.database.IgniteDbMultiNodeWithIndexingPutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeWithIndexingPutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeWithIndexingWalRestoreTest;
import org.apache.ignite.internal.processors.database.IgnitePersistentStoreQueryWithMultipleClassesPerCacheTest;
import org.apache.ignite.internal.processors.database.IgnitePersistentStoreSchemaLoadTest;
import org.apache.ignite.internal.processors.database.IgniteTwoRegionsRebuildIndexTest;

/**
 *
 */
public class IgnitePdsWithIndexingTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Db Memory Leaks With Indexing Test Suite");

        suite.addTestSuite(IgniteDbSingleNodeWithIndexingWalRestoreTest.class);
        suite.addTestSuite(IgniteDbSingleNodeWithIndexingPutGetTest.class);
        suite.addTestSuite(IgniteDbMultiNodeWithIndexingPutGetTest.class);
        suite.addTestSuite(IgnitePdsSingleNodeWithIndexingPutGetPersistenceTest.class);
        suite.addTestSuite(IgnitePdsSingleNodeWithIndexingAndGroupPutGetPersistenceSelfTest.class);
        suite.addTestSuite(IgnitePersistentStoreSchemaLoadTest.class);
        suite.addTestSuite(IgnitePersistentStoreQueryWithMultipleClassesPerCacheTest.class);
        suite.addTestSuite(IgniteTwoRegionsRebuildIndexTest.class);

        return suite;
    }
}
