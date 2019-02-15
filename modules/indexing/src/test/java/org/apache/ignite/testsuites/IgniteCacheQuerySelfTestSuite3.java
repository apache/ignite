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

import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousBatchForceServerModeAckTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryConcurrentPartitionUpdateTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryCounterPartitionedAtomicTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryCounterPartitionedTxTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryCounterReplicatedAtomicTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryCounterReplicatedTxTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryExecuteInPrimaryTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFactoryAsyncFilterRandomOperationTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFailoverAtomicNearEnabledSelfSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerClientSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.ClientReconnectContinuousQueryTest;
import org.apache.ignite.internal.processors.cache.query.continuous.ContinuousQueryReassignmentTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryNodesFilteringTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionTxOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientReconnectTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientTxReconnectTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryNoUnsubscribeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryReconnectTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Continuous queries 1.
    GridCacheContinuousQueryNodesFilteringTest.class,
    GridCacheContinuousQueryPartitionTxOneNodeTest.class,
    CacheContinuousWithTransformerReplicatedSelfTest.class,
    CacheContinuousQueryExecuteInPrimaryTest.class,
    CacheContinuousWithTransformerClientSelfTest.class,
    ClientReconnectContinuousQueryTest.class,
    IgniteCacheContinuousQueryNoUnsubscribeTest.class,
    IgniteCacheContinuousQueryClientTxReconnectTest.class,
    IgniteCacheContinuousQueryClientReconnectTest.class,
    GridCacheContinuousQueryAtomicSelfTest.class,
    GridCacheContinuousQueryAtomicNearEnabledSelfTest.class,
    GridCacheContinuousQueryPartitionTxOneNodeTest.class,
    IgniteCacheContinuousQueryClientReconnectTest.class,
    IgniteCacheContinuousQueryClientTxReconnectTest.class,
    GridCacheContinuousQueryReplicatedSelfTest.class,
    CacheContinuousQueryFactoryAsyncFilterRandomOperationTest.class,
    GridCacheContinuousQueryPartitionedSelfTest.class,
    CacheContinuousBatchForceServerModeAckTest.class,
    CacheContinuousQueryExecuteInPrimaryTest.class,
    GridCacheContinuousQueryNodesFilteringTest.class,
    IgniteCacheContinuousQueryNoUnsubscribeTest.class,
    ClientReconnectContinuousQueryTest.class,
    ContinuousQueryReassignmentTest.class,
    CacheContinuousQueryConcurrentPartitionUpdateTest.class,
    CacheContinuousQueryFactoryAsyncFilterRandomOperationTest.class,

    CacheContinuousQueryCounterPartitionedAtomicTest.class,
    CacheContinuousQueryCounterPartitionedTxTest.class,
    CacheContinuousQueryCounterReplicatedAtomicTest.class,
    CacheContinuousQueryCounterReplicatedTxTest.class,
    CacheContinuousQueryFailoverAtomicNearEnabledSelfSelfTest.class,

    IgniteCacheContinuousQueryReconnectTest.class
})
public class IgniteCacheQuerySelfTestSuite3 {
}
