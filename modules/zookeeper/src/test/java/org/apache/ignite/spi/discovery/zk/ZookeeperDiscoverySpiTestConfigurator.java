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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.IgniteDiscoverySpiInternalListenerSupport;
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

            ZookeeperDiscoverySpi zkSpi = new TestZookeeperDiscoverySpi();

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

    /** */
    private static class TestZookeeperDiscoverySpi extends ZookeeperDiscoverySpi implements IgniteDiscoverySpiInternalListenerSupport {
        /** */
        private volatile IgniteDiscoverySpiInternalListener internalLsnr;

        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) {
            IgniteDiscoverySpiInternalListener internalLsnr = this.internalLsnr;

            if (internalLsnr != null && !internalLsnr.beforeSendCustomEvent(this, log, msg))
                return;

            super.sendCustomEvent(msg);
        }

        /** {@inheritDoc} */
        @Override public void beforeJoinTopology(ClusterNode locNode) {
            IgniteDiscoverySpiInternalListener internalLsnr = this.internalLsnr;

            if (internalLsnr != null)
                internalLsnr.beforeJoin(locNode, log);
        }

        /** */
        @Override public void setInternalListener(IgniteDiscoverySpiInternalListener lsnr) {
            internalLsnr = lsnr;
        }
    }
}
