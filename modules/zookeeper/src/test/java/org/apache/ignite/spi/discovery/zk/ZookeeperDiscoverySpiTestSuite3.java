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

import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedNodeRestartTxSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryLongP2PTest;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryOperationP2PTest;
import org.apache.ignite.internal.processors.continuous.GridEventConsumeSelfTest;
import org.apache.ignite.p2p.GridP2PContinuousDeploymentSelfTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Regular Ignite tests executed with {@link ZookeeperDiscoverySpi}.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCacheReplicatedNodeRestartSelfTest.class,
    GridEventConsumeSelfTest.class,
    GridCachePartitionedNodeRestartTxSelfTest.class,
    IgniteClientDataStructuresTest.class,
    GridCacheReplicatedSequenceApiSelfTest.class,
    GridCachePartitionedSequenceApiSelfTest.class,
    GridCacheAtomicMultiJvmFullApiSelfTest.class,
    GridCachePartitionedMultiJvmFullApiSelfTest.class,
    GridP2PContinuousDeploymentSelfTest.class,
    CacheContinuousQueryOperationP2PTest.class,
    CacheContinuousQueryLongP2PTest.class
})
public class ZookeeperDiscoverySpiTestSuite3 {
    /** */
    @BeforeClass
    public static void init() throws Exception {
        ZookeeperDiscoverySpiTestConfigurator.initTestSuite();
    }
}
