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

package org.apache.ignite.internal.processors.cache.expiry;

import junit.framework.TestSuite;
import org.apache.ignite.cache.store.IgniteCacheExpiryStoreLoadSelfTest;

/**
 *
 */
public class IgniteCacheExpiryPolicyTestSuite extends TestSuite {
    /**
     * @return Cache Expiry Policy test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Expiry Policy Test Suite");

        suite.addTestSuite(IgniteCacheAtomicLocalExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicWithStoreExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderWithStoreExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicReplicatedExpiryPolicyTest.class);

        suite.addTestSuite(IgniteCacheTxLocalExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheTxExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheTxWithStoreExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheTxReplicatedExpiryPolicyTest.class);

        suite.addTestSuite(IgniteCacheAtomicExpiryPolicyWithStoreTest.class);
        suite.addTestSuite(IgniteCacheTxExpiryPolicyWithStoreTest.class);

        suite.addTestSuite(IgniteCacheExpiryStoreLoadSelfTest.class);

        suite.addTestSuite(IgniteCacheTtlCleanupSelfTest.class);

        suite.addTestSuite(IgniteCacheClientNearCacheExpiryTest.class);

        return suite;
    }
}