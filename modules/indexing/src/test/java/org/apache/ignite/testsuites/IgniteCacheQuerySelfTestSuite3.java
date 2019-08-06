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

import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousBatchForceServerModeAckTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryConcurrentPartitionUpdateTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryCounterPartitionedAtomicTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryCounterPartitionedTxTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryCounterReplicatedAtomicTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryCounterReplicatedTxTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryExecuteInPrimaryTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFactoryAsyncFilterRandomOperationTest;
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
    GridCacheContinuousQueryNodesFilteringTest.class, // ok
    GridCacheContinuousQueryPartitionTxOneNodeTest.class, // ok
    CacheContinuousWithTransformerReplicatedSelfTest.class, // ok
    CacheContinuousWithTransformerClientSelfTest.class, // ok
    CacheContinuousQueryExecuteInPrimaryTest.class, // ok
    IgniteCacheContinuousQueryNoUnsubscribeTest.class, // trb

    ClientReconnectContinuousQueryTest.class, // trb
    IgniteCacheContinuousQueryReconnectTest.class, // trb
    IgniteCacheContinuousQueryClientTxReconnectTest.class, // trb
    IgniteCacheContinuousQueryClientReconnectTest.class, // trb

    GridCacheContinuousQueryAtomicSelfTest.class, // trb
    GridCacheContinuousQueryAtomicNearEnabledSelfTest.class, // trb
    GridCacheContinuousQueryPartitionedSelfTest.class, // trb
    GridCacheContinuousQueryReplicatedSelfTest.class, // trb

    CacheContinuousQueryFactoryAsyncFilterRandomOperationTest.class, // trb
    CacheContinuousBatchForceServerModeAckTest.class, // ok
    ContinuousQueryReassignmentTest.class, // trb
    // t0d0 figure out
    CacheContinuousQueryConcurrentPartitionUpdateTest.class, // ?

    CacheContinuousQueryCounterPartitionedAtomicTest.class, // ok
    CacheContinuousQueryCounterPartitionedTxTest.class, // ok
    CacheContinuousQueryCounterReplicatedAtomicTest.class, // ok
    CacheContinuousQueryCounterReplicatedTxTest.class // ok
})
public class IgniteCacheQuerySelfTestSuite3 {
}
