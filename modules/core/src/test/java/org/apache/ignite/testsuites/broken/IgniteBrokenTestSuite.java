/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites.broken;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.GridCacheStopSelfTest;
import org.apache.ignite.internal.processors.cache.OffheapCacheMetricsForClusterGroupSelfTest;
import org.apache.ignite.internal.processors.cache.binary.GridCacheBinaryObjectMetadataExchangeMultinodeTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAffinityEarlyTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionNotLoadedEventSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheGetRestartTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.Random2LruNearEnabledPageEvictionMultinodeTest;
import org.apache.ignite.internal.processors.database.BPlusTreeFakeReuseSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeReuseSelfTest;
import org.apache.ignite.internal.processors.database.MemoryMetricsSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsMetaManagerSelfTest;
import org.apache.ignite.internal.processors.service.GridServiceProcessorMultiNodeSelfTest;

/**
 *
 */
public class IgniteBrokenTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Broken Test Suite");

        suite.addTestSuite(MemoryMetricsSelfTest.class);
        suite.addTestSuite(BPlusTreeFakeReuseSelfTest.class);
        suite.addTestSuite(BPlusTreeReuseSelfTest.class);
        suite.addTestSuite(CacheAffinityEarlyTest.class);
        suite.addTestSuite(GridCacheBinaryObjectMetadataExchangeMultinodeTest.class);
        suite.addTestSuite(GridCacheStopSelfTest.class);
        suite.addTestSuite(IgniteCacheGetRestartTest.class);
        suite.addTestSuite(IgfsMetaManagerSelfTest.class);
        suite.addTestSuite(GridServiceProcessorMultiNodeSelfTest.class);
        suite.addTestSuite(GridCachePartitionNotLoadedEventSelfTest.class);
        suite.addTestSuite(Random2LruNearEnabledPageEvictionMultinodeTest.class);
        suite.addTestSuite(OffheapCacheMetricsForClusterGroupSelfTest.class);

        return suite;
    }
}

