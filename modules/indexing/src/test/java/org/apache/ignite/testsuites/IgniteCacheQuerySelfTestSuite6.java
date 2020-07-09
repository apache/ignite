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

import org.apache.ignite.internal.processors.cache.index.StaticCacheDdlKeepStaticConfigurationTest;
import org.apache.ignite.internal.processors.cache.index.StaticCacheDdlTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousBatchAckTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFilterListenerTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFilterDeploymentFailedTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryOperationP2PTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryOrderingEventTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheKeepBinaryIterationNearEnabledTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheKeepBinaryIterationStoreEnabledTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheKeepBinaryIterationTest;
import org.apache.ignite.internal.processors.cache.query.continuous.ContinuousQueryMarshallerTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryLocalAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryMultiNodesFilteringTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionAtomicOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionedOnlySelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedAtomicOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientTest;
import org.apache.ignite.internal.processors.query.MemLeakOnSqlWithClientReconnectTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCacheContinuousQueryPartitionAtomicOneNodeTest.class,
    CacheContinuousWithTransformerPartitionedSelfTest.class,
    GridCacheContinuousQueryLocalAtomicSelfTest.class,
    GridCacheContinuousQueryReplicatedAtomicOneNodeTest.class,
    ContinuousQueryMarshallerTest.class,
    GridCacheContinuousQueryReplicatedAtomicSelfTest.class,
    CacheKeepBinaryIterationTest.class,
    GridCacheContinuousQueryMultiNodesFilteringTest.class,
    CacheKeepBinaryIterationStoreEnabledTest.class,
    CacheKeepBinaryIterationNearEnabledTest.class,
    GridCacheContinuousQueryPartitionedOnlySelfTest.class,
    CacheContinuousQueryOperationP2PTest.class,
    CacheContinuousBatchAckTest.class,
    CacheContinuousQueryOrderingEventTest.class,
    IgniteCacheContinuousQueryClientTest.class,
    CacheContinuousQueryAsyncFilterListenerTest.class,
    CacheContinuousWithTransformerRandomOperationsTest.class,
    CacheContinuousQueryRandomOperationsTest.class,
    StaticCacheDdlTest.class,
    StaticCacheDdlKeepStaticConfigurationTest.class,
    MemLeakOnSqlWithClientReconnectTest.class,
    CacheContinuousQueryFilterDeploymentFailedTest.class
})
public class IgniteCacheQuerySelfTestSuite6 {
}
