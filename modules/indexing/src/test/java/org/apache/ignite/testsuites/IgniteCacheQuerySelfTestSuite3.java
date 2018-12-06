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

/**
 * Test suite for cache queries.
 */
public class IgniteCacheQuerySelfTestSuite3 extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Cache Continuous Queries Test Suite");

        // Continuous queries 1.
/*        suite.addTestSuite(GridCacheContinuousQueryNodesFilteringTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionTxOneNodeTest.class);
        suite.addTestSuite(CacheContinuousWithTransformerReplicatedSelfTest.class);
        suite.addTestSuite(CacheContinuousQueryExecuteInPrimaryTest.class);
        suite.addTestSuite(CacheContinuousWithTransformerClientSelfTest.class);
        suite.addTestSuite(ClientReconnectContinuousQueryTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryNoUnsubscribeTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientTxReconnectTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientReconnectTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionTxOneNodeTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientReconnectTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientTxReconnectTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedSelfTest.class);
        suite.addTestSuite(CacheContinuousQueryFactoryAsyncFilterRandomOperationTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionedSelfTest.class);
        suite.addTestSuite(CacheContinuousBatchForceServerModeAckTest.class);
        suite.addTestSuite(CacheContinuousQueryExecuteInPrimaryTest.class);
        suite.addTestSuite(GridCacheContinuousQueryNodesFilteringTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryNoUnsubscribeTest.class);
        suite.addTestSuite(ClientReconnectContinuousQueryTest.class);
        suite.addTestSuite(ContinuousQueryReassignmentTest.class);
        suite.addTestSuite(CacheContinuousQueryConcurrentPartitionUpdateTest.class);
        suite.addTestSuite(CacheContinuousQueryFactoryAsyncFilterRandomOperationTest.class);

        suite.addTestSuite(CacheContinuousQueryCounterPartitionedAtomicTest.class);
        suite.addTestSuite(CacheContinuousQueryCounterPartitionedTxTest.class);
        suite.addTestSuite(CacheContinuousQueryCounterReplicatedAtomicTest.class);
        suite.addTestSuite(CacheContinuousQueryCounterReplicatedTxTest.class);
        suite.addTestSuite(CacheContinuousQueryFailoverAtomicNearEnabledSelfSelfTest.class);*/

        for (int i = 0; i < 150; i++)
        suite.addTestSuite(IgniteCacheContinuousQueryReconnectTest.class);

        return suite;
    }
}
