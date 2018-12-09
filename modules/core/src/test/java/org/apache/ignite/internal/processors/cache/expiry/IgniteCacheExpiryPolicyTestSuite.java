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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.cache.store.IgniteCacheExpiryStoreLoadSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerNotificationTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerExpiredEventsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpireAndUpdateConsistencyTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 *
 */
@RunWith(AllTests.class)
public class IgniteCacheExpiryPolicyTestSuite {
    /**
     * @return Cache Expiry Policy test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Cache Expiry Policy Test Suite");

        suite.addTest(new JUnit4TestAdapter(IgniteCacheLargeValueExpireTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalExpiryPolicyTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicLocalOnheapExpiryPolicyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicExpiryPolicyTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicOnheapExpiryPolicyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicWithStoreExpiryPolicyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicReplicatedExpiryPolicyTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxLocalExpiryPolicyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxExpiryPolicyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxWithStoreExpiryPolicyTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxReplicatedExpiryPolicyTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicExpiryPolicyWithStoreTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxExpiryPolicyWithStoreTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheExpiryStoreLoadSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheClientNearCacheExpiryTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheEntryListenerExpiredEventsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheExpireAndUpdateConsistencyTest.class));

        // Eager ttl expiration tests.
        suite.addTest(new JUnit4TestAdapter(GridCacheTtlManagerNotificationTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheOnlyOneTtlCleanupThreadExistsTest.class));

        return suite;
    }
}
