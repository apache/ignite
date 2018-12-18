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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteClientDataStructuresTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedNodeRestartTxSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.partitioned.GridCachePartitionedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.datastructures.replicated.GridCacheReplicatedSequenceApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNodeRestartSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCacheAtomicMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.multijvm.GridCachePartitionedMultiJvmFullApiSelfTest;
import org.apache.ignite.internal.processors.continuous.GridEventConsumeSelfTest;
import org.apache.ignite.p2p.GridP2PContinuousDeploymentSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.curator.TestingCluster;

/**
 * Regular Ignite tests executed with {@link ZookeeperDiscoverySpi}.
 */
public class ZookeeperDiscoverySpiTestSuite3 extends ZookeeperDiscoverySpiAbstractTestSuite {
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

        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedNodeRestartSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridEventConsumeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedNodeRestartTxSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientDataStructuresTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheReplicatedSequenceApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedSequenceApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCachePartitionedMultiJvmFullApiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridP2PContinuousDeploymentSelfTest.class));

        return suite;
    }
}
