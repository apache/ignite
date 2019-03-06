/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.cache.BinaryTypeMismatchLoggingTest;
import org.apache.ignite.internal.processors.cache.CacheBinaryKeyConcurrentQueryTest;
import org.apache.ignite.internal.processors.cache.CacheConfigurationP2PTest;
import org.apache.ignite.internal.processors.cache.CacheIndexStreamerTest;
import org.apache.ignite.internal.processors.cache.CacheOperationsWithExpirationTest;
import org.apache.ignite.internal.processors.cache.CacheQueryAfterDynamicCacheStartFailureTest;
import org.apache.ignite.internal.processors.cache.CacheQueryFilterExpiredTest;
import org.apache.ignite.internal.processors.cache.CacheRandomOperationsMultithreadedTest;
import org.apache.ignite.internal.processors.cache.ClientReconnectAfterClusterRestartTest;
import org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeSqlTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffHeapSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapIndexEntryEvictTest;
import org.apache.ignite.internal.processors.cache.GridCacheOffheapIndexGetSelfTest;
import org.apache.ignite.internal.processors.cache.GridIndexingWithNoopSwapSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigurationPrimitiveTypesSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheGroupsSqlTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheStarvationOnRebalanceTest;
import org.apache.ignite.internal.processors.cache.IgniteClientReconnectQueriesTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlAtomicLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlAtomicPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlTransactionalLocalSelfTest;
import org.apache.ignite.internal.processors.cache.ttl.CacheTtlTransactionalPartitionedSelfTest;
import org.apache.ignite.internal.processors.client.IgniteDataStreamerTest;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexHelperTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Cache tests using indexing.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    InlineIndexHelperTest.class,

    GridIndexingWithNoopSwapSelfTest.class,
    GridCacheOffHeapSelfTest.class,

    CacheTtlTransactionalLocalSelfTest.class,
    CacheTtlTransactionalPartitionedSelfTest.class,
    CacheTtlAtomicLocalSelfTest.class,
    CacheTtlAtomicPartitionedSelfTest.class,

    GridCacheOffheapIndexGetSelfTest.class,
    GridCacheOffheapIndexEntryEvictTest.class,
    CacheIndexStreamerTest.class,

    CacheConfigurationP2PTest.class,

    IgniteCacheConfigurationPrimitiveTypesSelfTest.class,
    IgniteClientReconnectQueriesTest.class,
    CacheRandomOperationsMultithreadedTest.class,
    IgniteCacheStarvationOnRebalanceTest.class,
    CacheOperationsWithExpirationTest.class,
    CacheBinaryKeyConcurrentQueryTest.class,
    CacheQueryFilterExpiredTest.class,

    ClientReconnectAfterClusterRestartTest.class,

    CacheQueryAfterDynamicCacheStartFailureTest.class,

    IgniteCacheGroupsSqlTest.class,

    IgniteDataStreamerTest.class,

    BinaryTypeMismatchLoggingTest.class,

    ClusterReadOnlyModeSqlTest.class
})
public class IgniteCacheWithIndexingTestSuite {
}
