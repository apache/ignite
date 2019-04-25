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

package org.apache.ignite.spi.discovery.zk;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.curator.TestingCluster;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.config.GridTestProperties;

/**
 * Allows to run regular Ignite tests with {@link org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi}.
 */
public class ZookeeperDiscoverySpiTestConfigurator {
    /** */
    private static final Lock lock = new ReentrantLock();

    /** */
    private static TestingCluster testingCluster;

    /**
     * @throws Exception If failed.
     */
    public static void initTestSuite() throws Exception {
        System.setProperty("H2_JDBC_CONNECTIONS", "500"); // For multi-jvm tests.

        System.setProperty("zookeeper.forceSync", "false");

        lock.lock();
        try {
            testingCluster = ZookeeperDiscoverySpiTestUtil.createTestingCluster(3);

            testingCluster.start();
        }
        finally {
            lock.unlock();
        }

        System.setProperty(GridTestProperties.IGNITE_CFG_PREPROCESSOR_CLS,
            ZookeeperDiscoverySpiTestConfigurator.class.getName());
    }

    /**
     * Called via reflection by {@link org.apache.ignite.testframework.junits.GridAbstractTest}.
     *
     * @param cfg Configuration to change.
     */
    @SuppressWarnings("unused")
    public static void preprocessConfiguration(IgniteConfiguration cfg) {
        lock.lock();
        try {
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
        finally {
            lock.unlock();
        }
    }
}
