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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.IgniteDataStorageMetricsSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsExchangeDuringCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePdsPageSizesTest;
import org.apache.ignite.internal.processors.cache.persistence.IgnitePersistentStoreDataStructuresTest;
import org.apache.ignite.internal.processors.cache.persistence.LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteAbsentEvictionNodeOutOfBaselineTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsReserveWalSegmentsTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgnitePdsReserveWalSegmentsWithCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteShutdownOnSupplyMessageFailureTest;
import org.apache.ignite.internal.processors.cache.persistence.db.filename.IgniteUidAsConsistentIdMigrationTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.FsyncWalRolloverDoesNotBlockTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWALTailIsReachedDuringIterationOverArchiveTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalFormatFileFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorExceptionDuringReadTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalIteratorSwitchSegmentTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalSerializerVersionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionNoArchiverTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionSwitchOnTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalCompactionTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.WalRolloverTypesTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteDataIntegrityTests;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteFsyncReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgnitePureJavaCrcCompatibility;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteReplayWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.crc.IgniteStandaloneWalIteratorInvalidCrcTest;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneWalRecordsIteratorTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsMvccTestSuite2 {
    /**
     * @return Suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Collection<Class> ignoredTests = new HashSet<>();

        // TODO IGNITE-7384: include test when implemented.
        ignoredTests.add(IgniteShutdownOnSupplyMessageFailureTest.class);

        // Classes that are contained mvcc test already.
        ignoredTests.add(LocalWalModeNoChangeDuringRebalanceOnNonNodeAssignTest.class);

        // Atomic caches
        ignoredTests.add(IgnitePersistentStoreDataStructuresTest.class);

        // Skip irrelevant test
        ignoredTests.add(IgniteDataIntegrityTests.class);
        ignoredTests.add(IgniteStandaloneWalIteratorInvalidCrcTest.class);
        ignoredTests.add(IgniteReplayWalIteratorInvalidCrcTest.class);
        ignoredTests.add(IgniteFsyncReplayWalIteratorInvalidCrcTest.class);
        ignoredTests.add(IgnitePureJavaCrcCompatibility.class);
        ignoredTests.add(IgniteAbsentEvictionNodeOutOfBaselineTest.class);

        ignoredTests.add(IgnitePdsPageSizesTest.class);
        ignoredTests.add(IgniteDataStorageMetricsSelfTest.class);
        ignoredTests.add(IgniteWalFormatFileFailoverTest.class);
        ignoredTests.add(IgnitePdsExchangeDuringCheckpointTest.class);
        ignoredTests.add(IgnitePdsReserveWalSegmentsTest.class);
        ignoredTests.add(IgnitePdsReserveWalSegmentsWithCompactionTest.class);

        ignoredTests.add(IgniteUidAsConsistentIdMigrationTest.class);
        ignoredTests.add(IgniteWalSerializerVersionTest.class);
        ignoredTests.add(WalCompactionTest.class);
        ignoredTests.add(WalCompactionNoArchiverTest.class);
        ignoredTests.add(WalCompactionSwitchOnTest.class);
        ignoredTests.add(IgniteWalIteratorSwitchSegmentTest.class);
        ignoredTests.add(IgniteWalIteratorExceptionDuringReadTest.class);
        ignoredTests.add(StandaloneWalRecordsIteratorTest.class);
        ignoredTests.add(IgniteWALTailIsReachedDuringIterationOverArchiveTest.class);
        ignoredTests.add(WalRolloverTypesTest.class);
        ignoredTests.add(FsyncWalRolloverDoesNotBlockTest.class);

        return IgnitePdsTestSuite2.suite(ignoredTests);
    }
}
