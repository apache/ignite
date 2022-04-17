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

import org.apache.ignite.internal.encryption.CacheGroupReencryptionTest;
import org.apache.ignite.internal.processors.cache.IgnitePdsSingleNodeWithIndexingAndGroupPutGetPersistenceSelfTest;
import org.apache.ignite.internal.processors.cache.IgnitePdsSingleNodeWithIndexingPutGetPersistenceTest;
import org.apache.ignite.internal.processors.cache.index.ClientReconnectWithSqlTableConfiguredTest;
import org.apache.ignite.internal.processors.cache.index.DropIndexTest;
import org.apache.ignite.internal.processors.cache.index.ForceRebuildIndexTest;
import org.apache.ignite.internal.processors.cache.index.RenameIndexTreeTest;
import org.apache.ignite.internal.processors.cache.index.ResumeCreateIndexTest;
import org.apache.ignite.internal.processors.cache.index.ResumeRebuildIndexTest;
import org.apache.ignite.internal.processors.cache.index.StopRebuildIndexTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsIndexingDefragmentationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteTcBotInitNewPageTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IndexingMultithreadedLoadContinuousRestartTest;
import org.apache.ignite.internal.processors.cache.persistence.db.LongDestroyDurableBackgroundTaskTest;
import org.apache.ignite.internal.processors.cache.persistence.db.MultipleParallelCacheDeleteDeadlockTest;
import org.apache.ignite.internal.processors.database.IgniteDbMultiNodeWithIndexingPutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeWithIndexingPutGetTest;
import org.apache.ignite.internal.processors.database.IgniteDbSingleNodeWithIndexingWalRestoreTest;
import org.apache.ignite.internal.processors.database.IgnitePersistentStoreQueryWithMultipleClassesPerCacheTest;
import org.apache.ignite.internal.processors.database.IgnitePersistentStoreSchemaLoadTest;
import org.apache.ignite.internal.processors.database.IgniteTwoRegionsRebuildIndexTest;
import org.apache.ignite.internal.processors.database.RebuildIndexTest;
import org.apache.ignite.internal.processors.database.RebuildIndexWithHistoricalRebalanceTest;
import org.apache.ignite.internal.processors.database.RebuildIndexWithMVCCTest;
import org.apache.ignite.internal.processors.query.h2.maintenance.MaintenanceRebuildIndexUtilsSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/** */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteDbSingleNodeWithIndexingWalRestoreTest.class,
    IgniteDbSingleNodeWithIndexingPutGetTest.class,
    IgniteDbMultiNodeWithIndexingPutGetTest.class,
    IgnitePdsSingleNodeWithIndexingPutGetPersistenceTest.class,
    IgnitePdsSingleNodeWithIndexingAndGroupPutGetPersistenceSelfTest.class,
    IgnitePersistentStoreSchemaLoadTest.class,
    IgnitePersistentStoreQueryWithMultipleClassesPerCacheTest.class,
    IgniteTwoRegionsRebuildIndexTest.class,
    IgniteTcBotInitNewPageTest.class,
    RebuildIndexWithHistoricalRebalanceTest.class,
    IndexingMultithreadedLoadContinuousRestartTest.class,
    LongDestroyDurableBackgroundTaskTest.class,
    RebuildIndexTest.class,
    RebuildIndexWithMVCCTest.class,
    ClientReconnectWithSqlTableConfiguredTest.class,
    MultipleParallelCacheDeleteDeadlockTest.class,
    CacheGroupReencryptionTest.class,
    IgnitePdsIndexingDefragmentationTest.class,
    StopRebuildIndexTest.class,
    ForceRebuildIndexTest.class,
    ResumeRebuildIndexTest.class,
    ResumeCreateIndexTest.class,
    RenameIndexTreeTest.class,
    DropIndexTest.class,
    MaintenanceRebuildIndexUtilsSelfTest.class
})
public class IgnitePdsWithIndexingTestSuite {
}
