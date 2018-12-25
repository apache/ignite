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

import junit.framework.JUnit4TestAdapter;
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for cache queries.
 */
@RunWith(AllTests.class)
public class IgniteCacheQuerySelfTestSuite3 {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Cache Continuous Queries Test Suite");

        // Continuous queries 1.
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryNodesFilteringTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryPartitionTxOneNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousWithTransformerReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryExecuteInPrimaryTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousWithTransformerClientSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientReconnectContinuousQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheContinuousQueryNoUnsubscribeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheContinuousQueryClientTxReconnectTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheContinuousQueryClientReconnectTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryAtomicSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryAtomicNearEnabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryPartitionTxOneNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheContinuousQueryClientReconnectTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheContinuousQueryClientTxReconnectTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFactoryAsyncFilterRandomOperationTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousBatchForceServerModeAckTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryExecuteInPrimaryTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryNodesFilteringTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheContinuousQueryNoUnsubscribeTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientReconnectContinuousQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(ContinuousQueryReassignmentTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryConcurrentPartitionUpdateTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFactoryAsyncFilterRandomOperationTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryCounterPartitionedAtomicTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryCounterPartitionedTxTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryCounterReplicatedAtomicTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryCounterReplicatedTxTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFailoverAtomicNearEnabledSelfSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheContinuousQueryReconnectTest.class));

        return suite;
    }
}
