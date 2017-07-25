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

package org.apache.ignite.internal.processors.cache.query.continuous;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.CacheInterceptorPartitionCounterLocalSanityTest;
import org.apache.ignite.internal.processors.cache.CacheInterceptorPartitionCounterRandomOperationsTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicRebalanceTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicWithStoreReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorAtomicWithStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorLocalAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorLocalAtomicWithStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorLocalSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorLocalWithStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorNearEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorReplicatedWithStoreSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorTransactionalRebalanceTest;
import org.apache.ignite.internal.processors.cache.GridCacheInterceptorWithStoreSelfTest;

/**
 * Cache interceptor suite.
 */
public class IgniteContinuousWithTransformerTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("ContinuousQueryWithTransformer Test Suite");

        suite.addTestSuite(SimpleCacheContinuousWithTransformerTest.class);
        suite.addTestSuite(CacheContinuousWithTransformerBatchAckTest.class);
        suite.addTestSuite(CacheContinuousQueryWithTransformerConcurrentPartitionUpdateTest.class);
        suite.addTestSuite(CacheContinuousQueryWithTransformerExecuteInPrimaryTest.class);
        suite.addTestSuite(CacheContinuousQueryWithTransformerAsyncFilterListenerTest.class);

        return suite;
    }
}
