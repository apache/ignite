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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
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

/**
 *
 */
public class IgniteCacheFailoverTestSuite2 {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Cache Failover Test Suite2");

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedTxSalvageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheGetFromJobTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicReplicatedFailoverSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheColocatedFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedFailoverSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheCrossCacheTxFailoverTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheAsyncOperationsFailoverAtomicTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheAsyncOperationsFailoverTxTest.class));

        suite.addTest(new JUnit4TestAdapter(CachePutAllFailoverAtomicTest.class));
        suite.addTest(new JUnit4TestAdapter(CachePutAllFailoverTxTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteStableBaselineCachePutAllFailoverTest.class));
        //suite.addTest(new JUnit4TestAdapter(IgniteStableBaselineCacheRemoveFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteChangingBaselineDownCachePutAllFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteChangingBaselineUpCachePutAllFailoverTest.class));

        return suite;
    }
}
