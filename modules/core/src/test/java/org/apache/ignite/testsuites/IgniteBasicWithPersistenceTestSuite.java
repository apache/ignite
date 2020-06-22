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

import org.apache.ignite.failure.FailureHandlingConfigurationTest;
import org.apache.ignite.failure.IoomFailureHandlerTest;
import org.apache.ignite.failure.SystemWorkersBlockingTest;
import org.apache.ignite.failure.SystemWorkersTerminationTest;
import org.apache.ignite.internal.ClusterBaselineNodesMetricsSelfTest;
import org.apache.ignite.internal.GridNodeMetricsLogPdsSelfTest;
import org.apache.ignite.internal.cluster.IgniteClusterIdTagTest;
import org.apache.ignite.internal.encryption.EncryptedCacheBigEntryTest;
import org.apache.ignite.internal.encryption.EncryptedCacheCreateTest;
import org.apache.ignite.internal.encryption.EncryptedCacheDestroyTest;
import org.apache.ignite.internal.encryption.EncryptedCacheGroupCreateTest;
import org.apache.ignite.internal.encryption.EncryptedCacheNodeJoinTest;
import org.apache.ignite.internal.encryption.EncryptedCachePreconfiguredRestartTest;
import org.apache.ignite.internal.encryption.EncryptedCacheRestartTest;
import org.apache.ignite.internal.encryption.EncryptionMXBeanTest;
import org.apache.ignite.internal.encryption.MasterKeyChangeConsistencyCheckTest;
import org.apache.ignite.internal.encryption.MasterKeyChangeTest;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointReadLockFailureTest;
import org.apache.ignite.internal.processors.cache.persistence.SingleNodePersistenceSslTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotMXBeanTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManagerSelfTest;
import org.apache.ignite.marshaller.GridMarshallerMappingConsistencyTest;
import org.apache.ignite.util.GridCommandHandlerClusterByClassTest;
import org.apache.ignite.util.GridCommandHandlerClusterByClassWithSSLTest;
import org.apache.ignite.util.GridCommandHandlerSslTest;
import org.apache.ignite.util.GridCommandHandlerTest;
import org.apache.ignite.util.GridCommandHandlerWithSSLTest;
import org.apache.ignite.util.GridInternalTaskUnusedWalSegmentsTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Basic test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IoomFailureHandlerTest.class,
    ClusterBaselineNodesMetricsSelfTest.class,
    GridMarshallerMappingConsistencyTest.class,
    SystemWorkersTerminationTest.class,
    FailureHandlingConfigurationTest.class,
    SystemWorkersBlockingTest.class,
    CheckpointReadLockFailureTest.class,

    GridCommandHandlerTest.class,
    GridCommandHandlerWithSSLTest.class,
    GridCommandHandlerClusterByClassTest.class,
    GridCommandHandlerClusterByClassWithSSLTest.class,
    GridCommandHandlerSslTest.class,
    GridInternalTaskUnusedWalSegmentsTest.class,

    GridNodeMetricsLogPdsSelfTest.class,

    EncryptedCacheBigEntryTest.class,
    EncryptedCacheCreateTest.class,
    EncryptedCacheDestroyTest.class,
    EncryptedCacheGroupCreateTest.class,
    EncryptedCacheNodeJoinTest.class,
    EncryptedCacheRestartTest.class,
    EncryptedCachePreconfiguredRestartTest.class,

    SingleNodePersistenceSslTest.class,

    MasterKeyChangeTest.class,
    MasterKeyChangeConsistencyCheckTest.class,

    EncryptionMXBeanTest.class,

    IgniteSnapshotManagerSelfTest.class,
    IgniteClusterSnapshotSelfTest.class,
    IgniteSnapshotMXBeanTest.class,

    IgniteClusterIdTagTest.class
})
public class IgniteBasicWithPersistenceTestSuite {
}
