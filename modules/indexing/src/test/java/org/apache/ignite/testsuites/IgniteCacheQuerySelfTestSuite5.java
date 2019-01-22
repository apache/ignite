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
