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

import org.apache.ignite.internal.processors.cache.persistence.snapshot.EncryptedSnapshotTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotCheckTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotDeltaTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotHandlerTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotRestoreSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotStreamerTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotWalRecordTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotConsistencyTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotMXBeanTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManagerSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotRemoteRequestTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotRestoreFromRemoteTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotWithMetastorageTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.PlainSnapshotTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/** */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteSnapshotManagerSelfTest.class,
    IgniteClusterSnapshotSelfTest.class,
    IgniteSnapshotRemoteRequestTest.class,
    IgniteClusterSnapshotCheckTest.class,
    IgniteSnapshotWithMetastorageTest.class,
    IgniteSnapshotMXBeanTest.class,
    IgniteClusterSnapshotRestoreSelfTest.class,
    IgniteClusterSnapshotHandlerTest.class,
    IgniteSnapshotRestoreFromRemoteTest.class,
    PlainSnapshotTest.class,
    EncryptedSnapshotTest.class,
    IgniteClusterSnapshotWalRecordTest.class,
    IgniteClusterSnapshotStreamerTest.class,
    IgniteSnapshotConsistencyTest.class,
    IgniteClusterSnapshotDeltaTest.class
})
public class IgniteSnapshotTestSuite {
}
