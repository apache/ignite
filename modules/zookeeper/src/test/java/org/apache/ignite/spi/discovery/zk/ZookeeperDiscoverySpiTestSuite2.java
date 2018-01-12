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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestSuite;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.ClusterNodeMetricsUpdateTest;
import org.apache.ignite.internal.IgniteClientReconnectCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunTxCacheOpTest;
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
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryAsyncFailoverAtomicSelfTest;
import org.apache.ignite.internal.processors.continuous.GridEventConsumeSelfTest;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.config.GridTestProperties;

/**
 * Regular Ignite tests executed with ZookeeperDiscoverySpi.
 */
public class ZookeeperDiscoverySpiTestSuite2 extends TestSuite {
    /** */
    private static TestingCluster testingCluster;

    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty("zookeeper.forceSync", "false");

        System.setProperty("H2_JDBC_CONNECTIONS", "500"); // For multi-jvm tests.

        testingCluster = createTestingCluster(3);

        testingCluster.start();

        System.setProperty(GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS, ZookeeperDiscoverySpiTestSuite2.class.getName());

        TestSuite suite = new TestSuite("ZookeeperDiscoverySpi Test Suite");

        suite.addTestSuite(ZookeeperDiscoverySuitePreprocessorTest.class);

        suite.addTestSuite(IgniteCacheEntryListenerAtomicTest.class);

        suite.addTestSuite(GridEventConsumeSelfTest.class);

        suite.addTestSuite(IgniteClientReconnectCacheTest.class);

        suite.addTestSuite(IgniteCachePutRetryAtomicSelfTest.class);
        suite.addTestSuite(IgniteCachePutRetryTransactionalSelfTest.class);

        suite.addTestSuite(GridCachePartitionedNodeRestartTest.class);
        suite.addTestSuite(GridCacheReplicatedNodeRestartSelfTest.class);

        suite.addTestSuite(ClusterNodeMetricsUpdateTest.class);

        suite.addTestSuite(GridCachePartitionedMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(GridCacheAtomicMultiNodeFullApiSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedAtomicMultiNodeFullApiSelfTest.class);

        suite.addTestSuite(CacheContinuousQueryAsyncFailoverAtomicSelfTest.class);

        suite.addTestSuite(IgniteCacheLockPartitionOnAffinityRunTxCacheOpTest.class);

        suite.addTestSuite(GridCachePartitionedNodeRestartTxSelfTest.class);
        suite.addTestSuite(IgniteClientDataStructuresTest.class);
        suite.addTestSuite(GridCacheReplicatedSequenceApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedSequenceApiSelfTest.class);

        suite.addTestSuite(IgniteCacheReplicatedQuerySelfTest.class);

        suite.addTestSuite(GridCacheAtomicMultiJvmFullApiSelfTest.class);
        suite.addTestSuite(GridCachePartitionedMultiJvmFullApiSelfTest.class);

        return suite;
    }

    /**
     * Called via reflection by {@link org.apache.ignite.testframework.junits.GridAbstractTest}.
     *
     * @param cfg Configuration to change.
     */
    public synchronized static void preprocessConfiguration(IgniteConfiguration cfg) {
        if (testingCluster == null)
            throw new IllegalStateException("Test Zookeeper cluster is not started.");

        ZookeeperDiscoverySpi zkSpi = new ZookeeperDiscoverySpi();

        DiscoverySpi spi = cfg.getDiscoverySpi();

        if (spi instanceof TcpDiscoverySpi)
            zkSpi.setClientReconnectDisabled(((TcpDiscoverySpi)spi).isClientReconnectDisabled());

        zkSpi.setSessionTimeout(30_000);
        zkSpi.setZkConnectionString(testingCluster.getConnectString());

        cfg.setDiscoverySpi(zkSpi);
    }

    /**
     * @param instances Number of instances in
     * @return Test cluster.
     */
    public static TestingCluster createTestingCluster(int instances) {
        String tmpDir = System.getProperty("java.io.tmpdir");

        List<InstanceSpec> specs = new ArrayList<>();

        for (int i = 0; i < instances; i++) {
            File file = new File(tmpDir, "apacheIgniteTestZk-" + i);

            if (file.isDirectory())
                deleteRecursively0(file);
            else {
                if (!file.mkdirs())
                    throw new IgniteException("Failed to create directory for test Zookeeper server: " + file.getAbsolutePath());
            }

            specs.add(new InstanceSpec(file, -1, -1, -1, true, -1, -1, 500));
        }

        return new TestingCluster(specs);
    }

    /**
     * @param file File or directory to delete.
     */
    private static void deleteRecursively0(File file) {
        File[] files = file.listFiles();

        if (files == null)
            return;

        for (File f : files) {
            if (f.isDirectory())
                deleteRecursively0(f);
            else {
                if (!f.delete())
                    throw new IgniteException("Failed to delete file: " + f.getAbsolutePath());
            }
        }
    }
}
