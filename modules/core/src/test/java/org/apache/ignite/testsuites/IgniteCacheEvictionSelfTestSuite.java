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
import org.apache.ignite.internal.processors.cache.GridCachePreloadingEvictionsSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearEvictionSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheConcurrentEvictionConsistencySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheConcurrentEvictionsSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheEmptyEntriesLocalSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheEmptyEntriesPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheEvictableEntryEqualsSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheEvictionFilterSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheEvictionLockUnlockSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.GridCacheEvictionTouchSelfTest;
import org.apache.ignite.internal.processors.cache.eviction.fifo.FifoEvictionPolicyFactorySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.fifo.FifoEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.lru.LruEvictionPolicyFactorySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.lru.LruEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.lru.LruNearEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.lru.LruNearOnlyNearEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionDataStreamerTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionMetricTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionMultinodeMixedRegionsTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionReadThroughTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.PageEvictionTouchOrderTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.Random2LruNearEnabledPageEvictionMultinodeTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.Random2LruPageEvictionMultinodeTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.Random2LruPageEvictionWithRebalanceTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.RandomLruNearEnabledPageEvictionMultinodeTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.RandomLruPageEvictionMultinodeTest;
import org.apache.ignite.internal.processors.cache.eviction.paged.RandomLruPageEvictionWithRebalanceTest;
import org.apache.ignite.internal.processors.cache.eviction.sorted.SortedEvictionPolicyFactorySelfTest;
import org.apache.ignite.internal.processors.cache.eviction.sorted.SortedEvictionPolicySelfTest;

/**
 * Test suite for cache eviction.
 */
public class IgniteCacheEvictionSelfTestSuite extends TestSuite {
    /**
     * @return Cache eviction test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Cache Eviction Test Suite");

        suite.addTest(new TestSuite(FifoEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(SortedEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(LruEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(FifoEvictionPolicyFactorySelfTest.class));
        suite.addTest(new TestSuite(SortedEvictionPolicyFactorySelfTest.class));
        suite.addTest(new TestSuite(LruEvictionPolicyFactorySelfTest.class));
        suite.addTest(new TestSuite(LruNearEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(LruNearOnlyNearEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearEvictionSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicNearEvictionSelfTest.class));
        suite.addTest(new TestSuite(GridCacheEvictionFilterSelfTest.class));
        suite.addTest(new TestSuite(GridCacheConcurrentEvictionsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheConcurrentEvictionConsistencySelfTest.class));
        suite.addTest(new TestSuite(GridCacheEvictionTouchSelfTest.class));
        suite.addTest(new TestSuite(GridCacheEvictionLockUnlockSelfTest.class));
        suite.addTest(new TestSuite(GridCachePreloadingEvictionsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheEmptyEntriesPartitionedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheEmptyEntriesLocalSelfTest.class));
        suite.addTest(new TestSuite(GridCacheEvictableEntryEqualsSelfTest.class));

        suite.addTest(new TestSuite(RandomLruPageEvictionMultinodeTest.class));
        suite.addTest(new TestSuite(RandomLruNearEnabledPageEvictionMultinodeTest.class));
        suite.addTest(new TestSuite(Random2LruPageEvictionMultinodeTest.class));
        suite.addTest(new TestSuite(Random2LruNearEnabledPageEvictionMultinodeTest.class));
        suite.addTest(new TestSuite(RandomLruPageEvictionWithRebalanceTest.class));
        suite.addTest(new TestSuite(Random2LruPageEvictionWithRebalanceTest.class));
        suite.addTest(new TestSuite(PageEvictionTouchOrderTest.class));
        suite.addTest(new TestSuite(PageEvictionReadThroughTest.class));
        suite.addTest(new TestSuite(PageEvictionDataStreamerTest.class));

        suite.addTest(new TestSuite(PageEvictionMultinodeMixedRegionsTest.class));

        suite.addTest(new TestSuite(PageEvictionMetricTest.class));

        return suite;
    }
}
