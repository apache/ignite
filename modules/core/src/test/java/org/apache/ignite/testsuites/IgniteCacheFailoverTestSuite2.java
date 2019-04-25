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

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.cache.CacheGetFromJobTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAsyncOperationsFailoverAtomicTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheAsyncOperationsFailoverTxTest;
import org.apache.ignite.internal.processors.cache.distributed.CachePutAllFailoverAtomicTest;
import org.apache.ignite.internal.processors.cache.distributed.CachePutAllFailoverTxTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheCrossCacheTxFailoverTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicReplicatedFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedTxSalvageSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFailoverSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteChangingBaselineDownCachePutAllFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteChangingBaselineUpCachePutAllFailoverTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/** */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCachePartitionedTxSalvageSelfTest.class,
    CacheGetFromJobTest.class,

    GridCacheAtomicFailoverSelfTest.class,
    GridCacheAtomicReplicatedFailoverSelfTest.class,

    GridCachePartitionedFailoverSelfTest.class,
    GridCacheColocatedFailoverSelfTest.class,
    GridCacheReplicatedFailoverSelfTest.class,

    IgniteCacheCrossCacheTxFailoverTest.class,

    CacheAsyncOperationsFailoverAtomicTest.class,
    CacheAsyncOperationsFailoverTxTest.class,

    CachePutAllFailoverAtomicTest.class,
    CachePutAllFailoverTxTest.class,
    //suite.addTest(new JUnit4TestAdapter(IgniteStableBaselineCachePutAllFailoverTest.class,
    //suite.addTest(new JUnit4TestAdapter(IgniteStableBaselineCacheRemoveFailoverTest.class,
    IgniteChangingBaselineDownCachePutAllFailoverTest.class,
    IgniteChangingBaselineUpCachePutAllFailoverTest.class
})
public class IgniteCacheFailoverTestSuite2 {
}
