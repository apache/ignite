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

import junit.framework.TestSuite;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.curator.TestingCluster;
import org.apache.ignite.internal.ClusterNodeMetricsUpdateTest;
import org.apache.ignite.internal.IgniteClientReconnectCacheTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedNodeRestartTxSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePutRetryAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePutRetryTransactionalSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedNodeRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.IgniteCacheReplicatedQuerySelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.continuous.GridEventConsumeSelfTest;
import org.apache.ignite.p2p.GridP2PContinuousDeploymentSelfTest;
import org.apache.ignite.util.GridCommandHandlerTest;

/**
 * Regular Ignite tests executed with {@link org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi}.
 */
public class ZookeeperDiscoverySpiTestSuite2 extends ZookeeperDiscoverySpiAbstractTestSuite {
    /** */
    private static TestingCluster testingCluster;

    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty("H2_JDBC_CONNECTIONS", "500"); // For multi-jvm tests.

        initSuite();

        TestSuite suite = new TestSuite("ZookeeperDiscoverySpi Test Suite");

        suite.addTestSuite(ZookeeperDiscoverySuitePreprocessorTest.class);

        suite.addTestSuite(GridCacheReplicatedNodeRestartSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNodeRestartTest.class);

        suite.addTestSuite(IgniteCacheEntryListenerWithZkDiscoAtomicTest.class);

        suite.addTestSuite(GridEventConsumeSelfTest.class);

        suite.addTestSuite(IgniteClientReconnectCacheTest.class);

        suite.addTestSuite(IgniteCachePutRetryAtomicSelfTest.class);
        suite.addTestSuite(IgniteCachePutRetryTransactionalSelfTest.class);

        suite.addTestSuite(ClusterNodeMetricsUpdateTest.class);

        suite.addTestSuite(GridCachePartitionedMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(GridCachePartitionedNodeRestartTxSelfTest.class);
        suite.addTestSuite(IgniteClientDataStructuresTest.class);
        suite.addTestSuite(GridCacheReplicatedSequenceApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedSequenceApiSelfTest.class);

        suite.addTestSuite(IgniteCacheReplicatedQuerySelfTest.class);

        suite.addTestSuite(GridCacheAtomicMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMultiJvmFullApiSelfTest.class);

        suite.addTestSuite(GridCommandHandlerTest.class);

        suite.addTestSuite(GridP2PContinuousDeploymentSelfTest.class);

        return suite;
    }
}
