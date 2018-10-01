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

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousBatchAckTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousBatchForceServerModeAckTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFilterListenerTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryConcurrentPartitionUpdateTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryEventBufferTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryExecuteInPrimaryTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFactoryAsyncFilterRandomOperationTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryFactoryFilterRandomOperationTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryLostPartitionTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryOperationFromCallbackTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryOperationP2PTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryOrderingEventTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryRandomOperationsTwoNodesTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerClientSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerFailoverTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerLocalSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousWithTransformerReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheKeepBinaryIterationNearEnabledTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheKeepBinaryIterationStoreEnabledTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheKeepBinaryIterationTest;
import org.apache.ignite.internal.processors.cache.query.continuous.ClientReconnectContinuousQueryTest;
import org.apache.ignite.internal.processors.cache.query.continuous.ContinuousQueryMarshallerTest;
import org.apache.ignite.internal.processors.cache.query.continuous.ContinuousQueryPeerClassLoadingTest;
import org.apache.ignite.internal.processors.cache.query.continuous.ContinuousQueryRemoteFilterMissingInClassPathSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryConcurrentTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryLocalAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryLocalSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryMultiNodesFilteringTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryNodesFilteringTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionAtomicOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionTxOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionedOnlySelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionedP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedAtomicOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedP2PDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryReplicatedTxOneNodeTest;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryBackupQueueTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientReconnectTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryClientTxReconnectTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryImmutableEntryTest;
import org.apache.ignite.internal.processors.cache.query.continuous.IgniteCacheContinuousQueryNoUnsubscribeTest;

/**
 * Test suite for cache queries.
 */
public class IgniteCacheQuerySelfTestSuite3 extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Cache Queries Test Suite 3");

        // Continuous queries.
        suite.addTestSuite(GridCacheContinuousQueryLocalSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryLocalAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedP2PDisabledSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionedOnlySelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionedP2PDisabledSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryTxSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicP2PDisabledSelfTest.class);

        suite.addTestSuite(GridCacheContinuousQueryReplicatedTxOneNodeTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedAtomicOneNodeTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionTxOneNodeTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionAtomicOneNodeTest.class);

        suite.addTestSuite(IgniteCacheContinuousQueryClientTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientReconnectTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientTxReconnectTest.class);

        suite.addTestSuite(CacheContinuousQueryRandomOperationsTest.class); // TODO Too many unsupported operations.
        suite.addTestSuite(CacheContinuousQueryRandomOperationsTwoNodesTest.class); // TODO Too many unsupported operations.
        suite.addTestSuite(GridCacheContinuousQueryConcurrentTest.class); // Done coordinator state lag

        suite.addTestSuite(CacheContinuousQueryAsyncFilterListenerTest.class); // TODO coordinator state lag
        suite.addTestSuite(CacheContinuousQueryFactoryFilterRandomOperationTest.class); // TODO Too many unsupported operations.
        suite.addTestSuite(CacheContinuousQueryFactoryAsyncFilterRandomOperationTest.class);// TODO Too many unsupported operations.
        suite.addTestSuite(CacheContinuousQueryOrderingEventTest.class); // TODO coordinator state lag
        suite.addTestSuite(CacheContinuousQueryOperationFromCallbackTest.class);  // TODO Too many unsupported operations.
        suite.addTestSuite(CacheContinuousQueryOperationP2PTest.class); // fixed Classloader problem
        suite.addTestSuite(CacheContinuousBatchAckTest.class); // Added tests
        suite.addTestSuite(CacheContinuousBatchForceServerModeAckTest.class); // Added tests
        suite.addTestSuite(CacheContinuousQueryExecuteInPrimaryTest.class); // TODO Too many unsupported operations.
        suite.addTestSuite(CacheContinuousQueryLostPartitionTest.class); // Added tests
        suite.addTestSuite(ContinuousQueryRemoteFilterMissingInClassPathSelfTest.class); // solved Classloader problem
        suite.addTestSuite(GridCacheContinuousQueryNodesFilteringTest.class); // solved Classloader problem
        suite.addTestSuite(GridCacheContinuousQueryMultiNodesFilteringTest.class); // Added tests
        suite.addTestSuite(IgniteCacheContinuousQueryImmutableEntryTest.class); // Added tests
        suite.addTestSuite(CacheKeepBinaryIterationTest.class); // Added tests
        suite.addTestSuite(CacheKeepBinaryIterationStoreEnabledTest.class); // Added tests
        suite.addTestSuite(CacheKeepBinaryIterationNearEnabledTest.class); // Added tests
        suite.addTestSuite(IgniteCacheContinuousQueryBackupQueueTest.class); // Added tests
        suite.addTestSuite(IgniteCacheContinuousQueryNoUnsubscribeTest.class); // Added tests
        suite.addTestSuite(ClientReconnectContinuousQueryTest.class); // No need
        suite.addTestSuite(ContinuousQueryPeerClassLoadingTest.class); // No need
        suite.addTestSuite(ContinuousQueryMarshallerTest.class);// No need

        suite.addTestSuite(CacheContinuousQueryConcurrentPartitionUpdateTest.class); // Done Concurrent operations
        suite.addTestSuite(CacheContinuousQueryEventBufferTest.class); // No need

        suite.addTestSuite(CacheContinuousWithTransformerReplicatedSelfTest.class);
        suite.addTestSuite(CacheContinuousWithTransformerLocalSelfTest.class);
        suite.addTestSuite(CacheContinuousWithTransformerPartitionedSelfTest.class);
        suite.addTestSuite(CacheContinuousWithTransformerClientSelfTest.class);
        suite.addTestSuite(CacheContinuousWithTransformerFailoverTest.class); // TODO failover
        suite.addTestSuite(CacheContinuousWithTransformerRandomOperationsTest.class); // TODO Too many unsupported operations.

        //suite.addTestSuite(CacheContinuousQueryCounterPartitionedAtomicTest.class);
        //suite.addTestSuite(CacheContinuousQueryCounterPartitionedTxTest.class);
        //suite.addTestSuite(CacheContinuousQueryCounterReplicatedAtomicTest.class);
        //suite.addTestSuite(CacheContinuousQueryCounterReplicatedTxTest.class);
        //suite.addTestSuite(CacheContinuousQueryFailoverAtomicNearEnabledSelfSelfTest.class);

        //suite.addTestSuite(IgniteCacheContinuousQueryReconnectTest.class);

        return suite;
    }
}
