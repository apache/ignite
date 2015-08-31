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

package org.apache.ignite.internal.processors.cache;

import junit.framework.TestSuite;

/**
 * Cache interceptor suite.
 */
public class IgniteCacheInterceptorSelfTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("CacheInterceptor Test Suite");

        suite.addTestSuite(GridCacheInterceptorLocalSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorLocalWithStoreSelfTest.class);

        suite.addTestSuite(GridCacheInterceptorLocalAtomicSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorLocalAtomicWithStoreSelfTest.class);

        suite.addTestSuite(GridCacheInterceptorAtomicSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorAtomicNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorAtomicWithStoreSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorAtomicPrimaryWriteOrderSelfTest.class);

        suite.addTestSuite(GridCacheInterceptorAtomicReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorAtomicWithStoreReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorAtomicReplicatedPrimaryWriteOrderSelfTest.class);

        suite.addTestSuite(GridCacheInterceptorSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorNearEnabledSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorWithStoreSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorReplicatedWithStoreSelfTest.class);

        suite.addTestSuite(GridCacheOnCopyFlagTxPartitionedSelfTest.class);
        suite.addTestSuite(GridCacheOnCopyFlagReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheOnCopyFlagLocalSelfTest.class);
        suite.addTestSuite(GridCacheOnCopyFlagAtomicSelfTest.class);

        return suite;
    }
}