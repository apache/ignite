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
import org.junit.runners.AllTests;

/**
 * Test suite for cache queries.
 */
@RunWith(AllTests.class)
public class IgniteCacheQuerySelfTestSuite5 {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Cache Continuous Queries Test Suite 2");

        // Continuous queries 2.
        suite.addTest(new JUnit4TestAdapter(IgniteCacheContinuousQueryImmutableEntryTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousWithTransformerLocalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryEventBufferTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryReplicatedTxOneNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryLocalSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousWithTransformerFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(ContinuousQueryRemoteFilterMissingInClassPathSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ContinuousQueryPeerClassLoadingTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryAtomicP2PDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryTxSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryReplicatedP2PDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryPartitionedP2PDisabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryLostPartitionTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheContinuousQueryConcurrentTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryRandomOperationsTwoNodesTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheContinuousQueryBackupQueueTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryOperationFromCallbackTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheContinuousQueryFactoryFilterRandomOperationTest.class));

        return suite;
    }
}
