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

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.portable.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.query.continuous.*;
import org.apache.ignite.internal.processors.query.h2.sql.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.testframework.config.*;

import junit.framework.*;

/**
 * Cache query suite with portable marshaller.
 */
public class IgnitePortableCacheQueryTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception In case of error.
     */
    public static TestSuite suite() throws Exception {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, PortableMarshaller.class.getName());

        TestSuite suite = new TestSuite("Grid Cache Query Test Suite using PortableMarshaller");

        // Parsing
        suite.addTestSuite(GridQueryParsingTest.class);

        // Queries tests.
        suite.addTestSuite(GridCacheQueryIndexDisabledSelfTest.class);
        suite.addTestSuite(IgniteCachePartitionedQueryMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheLargeResultSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryEvictsMultiThreadedSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryOffheapMultiThreadedSelfTest.class);

        suite.addTestSuite(IgniteCacheOffheapTieredMultithreadedSelfTest.class);
        suite.addTestSuite(GridCacheReduceQueryMultithreadedSelfTest.class);


        // Fields queries.
        suite.addTestSuite(IgniteCacheFieldsQueryNoDataSelfTest.class);

        // Continuous queries.
        suite.addTestSuite(GridCacheContinuousQueryLocalAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryReplicatedAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryPartitionedOnlySelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheContinuousQueryAtomicP2PDisabledSelfTest.class);

        suite.addTestSuite(GridCacheQueryIndexingDisabledSelfTest.class);

        //Should be adjusted. Not ready to be used with PortableMarshaller.
        //suite.addTestSuite(GridCachePortableSwapScanQuerySelfTest.class);

        suite.addTestSuite(GridOrderedMessageCancelSelfTest.class);

        // Ignite cache and H2 comparison.
        suite.addTestSuite(BaseH2CompareQueryTest.class);
        suite.addTestSuite(H2CompareBigQueryTest.class);

        // Metrics tests
        suite.addTestSuite(CacheLocalQueryMetricsSelfTest.class);
        suite.addTestSuite(CachePartitionedQueryMetricsDistributedSelfTest.class);
        suite.addTestSuite(CachePartitionedQueryMetricsLocalSelfTest.class);
        suite.addTestSuite(CacheReplicatedQueryMetricsDistributedSelfTest.class);
        suite.addTestSuite(CacheReplicatedQueryMetricsLocalSelfTest.class);

        //Unmarshallig query test.
        suite.addTestSuite(IgniteCacheP2pUnmarshallingQueryErrorTest.class);

        suite.addTestSuite(GridCachePortableDuplicateIndexObjectPartitionedAtomicSelfTest.class);
        suite.addTestSuite(GridCachePortableDuplicateIndexObjectPartitionedTransactionalSelfTest.class);

        return suite;
    }
}
