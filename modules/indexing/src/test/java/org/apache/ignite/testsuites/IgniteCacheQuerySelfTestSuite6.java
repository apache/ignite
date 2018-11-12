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
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFilterListenerTest;
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

/**
 * Test suite for cache queries.
 */
public class IgniteCacheQuerySelfTestSuite6 extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Cache Continuous Queries Test Suite 3");

        // Continuous queries 3.
        suite.addTestSuite(GridCacheContinuousQueryPartitionAtomicOneNodeTest.class);
        suite.addTestSuite(CacheContinuousWithTransformerPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryLocalAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedAtomicOneNodeTest.class);
        suite.addTestSuite(ContinuousQueryMarshallerTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedAtomicSelfTest.class);
        suite.addTestSuite(CacheKeepBinaryIterationTest.class);
        suite.addTestSuite(GridCacheContinuousQueryMultiNodesFilteringTest.class);
        suite.addTestSuite(CacheKeepBinaryIterationStoreEnabledTest.class);
        suite.addTestSuite(CacheKeepBinaryIterationNearEnabledTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionedOnlySelfTest.class);
        suite.addTestSuite(CacheContinuousQueryOperationP2PTest.class);
        suite.addTestSuite(CacheContinuousBatchAckTest.class);
        suite.addTestSuite(CacheContinuousQueryOrderingEventTest.class);
        suite.addTestSuite(IgniteCacheContinuousQueryClientTest.class);
        suite.addTestSuite(CacheContinuousQueryAsyncFilterListenerTest.class);
        suite.addTestSuite(CacheContinuousWithTransformerRandomOperationsTest.class);
        suite.addTestSuite(CacheContinuousQueryRandomOperationsTest.class);

        return suite;
    }
}
