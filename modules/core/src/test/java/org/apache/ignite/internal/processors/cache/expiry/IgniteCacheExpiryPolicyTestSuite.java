/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.expiry;

import org.apache.ignite.cache.store.IgniteCacheExpiryStoreLoadSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManagerNotificationTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerExpiredEventsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpireAndUpdateConsistencyTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteCacheLargeValueExpireTest.class,

    IgniteCacheAtomicLocalExpiryPolicyTest.class,
    //IgniteCacheAtomicLocalOnheapExpiryPolicyTest.class,
    IgniteCacheAtomicExpiryPolicyTest.class,
    //IgniteCacheAtomicOnheapExpiryPolicyTest.class,
    IgniteCacheAtomicWithStoreExpiryPolicyTest.class,
    IgniteCacheAtomicReplicatedExpiryPolicyTest.class,

    IgniteCacheTxLocalExpiryPolicyTest.class,
    IgniteCacheTxExpiryPolicyTest.class,
    IgniteCacheTxWithStoreExpiryPolicyTest.class,
    IgniteCacheTxReplicatedExpiryPolicyTest.class,

    IgniteCacheAtomicExpiryPolicyWithStoreTest.class,
    IgniteCacheTxExpiryPolicyWithStoreTest.class,

    IgniteCacheExpiryStoreLoadSelfTest.class,

    IgniteCacheClientNearCacheExpiryTest.class,

    IgniteCacheEntryListenerExpiredEventsTest.class,

    IgniteCacheExpireAndUpdateConsistencyTest.class,

    // Eager ttl expiration tests.
    GridCacheTtlManagerNotificationTest.class,
    IgniteCacheOnlyOneTtlCleanupThreadExistsTest.class,

    IgniteCacheExpireWhileRebalanceTest.class
})
public class IgniteCacheExpiryPolicyTestSuite {
}
