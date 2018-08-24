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
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerNotificationTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerExpiredEventsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpireAndUpdateConsistencyTest;

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

        suite.addTestSuite(IgniteCacheLargeValueExpireTest.class);

        suite.addTestSuite(IgniteCacheAtomicLocalExpiryPolicyTest.class);
        //suite.addTestSuite(IgniteCacheAtomicLocalOnheapExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicExpiryPolicyTest.class);
        //suite.addTestSuite(IgniteCacheAtomicOnheapExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicWithStoreExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicReplicatedExpiryPolicyTest.class);

        suite.addTestSuite(IgniteCacheTxLocalExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheTxExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheTxWithStoreExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheTxReplicatedExpiryPolicyTest.class);

        suite.addTestSuite(IgniteCacheAtomicExpiryPolicyWithStoreTest.class);
        suite.addTestSuite(IgniteCacheTxExpiryPolicyWithStoreTest.class);

        suite.addTestSuite(IgniteCacheExpiryStoreLoadSelfTest.class);

        suite.addTestSuite(IgniteCacheClientNearCacheExpiryTest.class);

        suite.addTestSuite(IgniteCacheEntryListenerExpiredEventsTest.class);

        suite.addTestSuite(IgniteCacheExpireAndUpdateConsistencyTest.class);

        // Eager ttl expiration tests.
        suite.addTestSuite(GridCacheTtlManagerNotificationTest.class);
        suite.addTestSuite(IgniteCacheOnlyOneTtlCleanupThreadExistsTest.class);

        return suite;
    }
}
