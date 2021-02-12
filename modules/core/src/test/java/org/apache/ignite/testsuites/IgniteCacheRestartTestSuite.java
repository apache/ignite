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

import org.apache.ignite.internal.processors.cache.IgniteCacheCreateRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheNearRestartRollbackSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCachePartitionedNearDisabledOptimisticTxNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedOptimisticTxNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.near.NearCacheMultithreadedUpdateTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNodeRestartSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Cache stability test suite on changing topology.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCachePartitionedNodeRestartTest.class,
    GridCachePartitionedOptimisticTxNodeRestartTest.class,
    GridCacheReplicatedNodeRestartSelfTest.class,
    GridCachePartitionedNearDisabledOptimisticTxNodeRestartTest.class,
    IgniteCacheNearRestartRollbackSelfTest.class,
    NearCacheMultithreadedUpdateTest.class,

    IgniteCacheCreateRestartSelfTest.class
})
public class IgniteCacheRestartTestSuite {
}
