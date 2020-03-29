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

package org.apache.ignite.spi.discovery.zk;

import org.apache.ignite.internal.ClusterNodeMetricsUpdateTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePutRetryAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePutRetryTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStoragePersistentTest;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorageTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Regular Ignite tests executed with {@link ZookeeperDiscoverySpi}.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ZookeeperDiscoverySuitePreprocessorTest.class,
    IgniteCachePutRetryAtomicSelfTest.class,
    IgniteCachePutRetryTransactionalSelfTest.class,
    ClusterNodeMetricsUpdateTest.class,
    GridCacheAtomicMultiNodeFullApiSelfTest.class,
    GridCacheReplicatedAtomicMultiNodeFullApiSelfTest.class,
    IgniteCacheReplicatedQuerySelfTest.class,
    DistributedMetaStorageTest.class,
    DistributedMetaStoragePersistentTest.class
})
public class ZookeeperDiscoverySpiTestSuite4 {
    /** */
    @BeforeClass
    public static void init() throws Exception {
        ZookeeperDiscoverySpiTestConfigurator.initTestSuite();
    }
}
