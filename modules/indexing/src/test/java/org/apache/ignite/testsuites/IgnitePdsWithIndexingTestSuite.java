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

        return suite;
    }
}
