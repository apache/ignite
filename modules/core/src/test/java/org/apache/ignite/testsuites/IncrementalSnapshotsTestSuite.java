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

import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotRebalanceTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.ConcurrentTxsIncrementalSnapshotTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotJoiningClientTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotNoBackupMessagesBlockingTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotNoBackupWALBlockingTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotNodeFailureTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotRestoreTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotSingleBackupMessagesBlockingTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotSingleBackupWALBlockingTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotTwoBackupMessagesBlockingTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotTwoBackupWALBlockingTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotTxRecoveryTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental.IncrementalSnapshotWarnAtomicCachesTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for incremental snapshots.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IncrementalSnapshotNoBackupMessagesBlockingTest.class,
    IncrementalSnapshotSingleBackupMessagesBlockingTest.class,
    IncrementalSnapshotTwoBackupMessagesBlockingTest.class,
    IncrementalSnapshotNoBackupWALBlockingTest.class,
    IncrementalSnapshotSingleBackupWALBlockingTest.class,
    IncrementalSnapshotTwoBackupWALBlockingTest.class,
    ConcurrentTxsIncrementalSnapshotTest.class,
    IncrementalSnapshotNodeFailureTest.class,
    IncrementalSnapshotTxRecoveryTest.class,
    IncrementalSnapshotTest.class,
    IncrementalSnapshotJoiningClientTest.class,
    IncrementalSnapshotRestoreTest.class,
    IncrementalSnapshotWarnAtomicCachesTest.class,
    IncrementalSnapshotRebalanceTest.class
})
public class IncrementalSnapshotsTestSuite {
}
