/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEventBufferTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFactoryFilterRandomOperationTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryLostPartitionTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryOperationFromCallbackTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTwoNodesTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerFailoverTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerLocalSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.ContinuousQueryPeerClassLoadingTest;
import org.apache.ignite.internal.processors.cache.query.continuous.ContinuousQueryRemoteFilterMissingInClassPathSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryConcurrentTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryLocalSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionedP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedTxOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryBackupQueueTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryImmutableEntryTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteCacheContinuousQueryImmutableEntryTest.class,
    CacheContinuousWithTransformerLocalSelfTest.class,
    CacheContinuousQueryEventBufferTest.class,
    GridCacheContinuousQueryReplicatedTxOneNodeTest.class,
    GridCacheContinuousQueryLocalSelfTest.class,
    CacheContinuousWithTransformerFailoverTest.class,
    ContinuousQueryRemoteFilterMissingInClassPathSelfTest.class,
    ContinuousQueryPeerClassLoadingTest.class,
    GridCacheContinuousQueryAtomicP2PDisabledSelfTest.class,
    GridCacheContinuousQueryTxSelfTest.class,
    GridCacheContinuousQueryReplicatedP2PDisabledSelfTest.class,
    GridCacheContinuousQueryPartitionedP2PDisabledSelfTest.class,
    CacheContinuousQueryLostPartitionTest.class,
    GridCacheContinuousQueryConcurrentTest.class,
    CacheContinuousQueryRandomOperationsTwoNodesTest.class,
    IgniteCacheContinuousQueryBackupQueueTest.class,
    CacheContinuousQueryOperationFromCallbackTest.class,
    CacheContinuousQueryFactoryFilterRandomOperationTest.class
})
public class IgniteCacheQuerySelfTestSuite5 {
}
