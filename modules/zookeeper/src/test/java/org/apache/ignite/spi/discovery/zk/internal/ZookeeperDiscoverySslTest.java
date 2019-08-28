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

package org.apache.ignite.spi.discovery.zk.internal;

import java.nio.file.Paths;
import java.util.function.Function;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.zk.curator.TestingCluster;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiTestUtil;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.spi.discovery.tcp.ipfinder.zk.curator.TestingZooKeeperServer.SECURITY_CLIENT_PORT;
import static org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTestBase.waitForZkClusterReady;

/**
 *
 */
public class ZookeeperDiscoverySslTest extends GridCommonAbstractTest {
    /** Ignite home. */
    private static final String IGNITE_HOME = U.getIgniteHome();

    /** Resource path. */
    private static final Function<String, String> rsrcPath = rsrc -> Paths.get(
            IGNITE_HOME == null ? "." : IGNITE_HOME,
            "modules",
            "core",
            "src",
            "test",
            "resources",
            rsrc
    ).toString();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ZookeeperDiscoverySpi spi = new ZookeeperDiscoverySpi();

        spi.setZkConnectionString("localhost:2281");

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testSsl() throws Exception {
        setupSystemProperties();

        try (TestingCluster zkCluster = ZookeeperDiscoverySpiTestUtil.createTestingCluster(1)) {
            zkCluster.start();

            waitForZkClusterReady(zkCluster);

            System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");

            IgniteEx ignite = startGrids(2);

            assertEquals(2, ignite.cluster().topologyVersion());
        }
        finally {
            clearSystemProperties();
        }
    }

    /**
     *
     */
    private void setupSystemProperties() {
        System.setProperty("zookeeper.serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
        System.setProperty("zookeeper.client.secure", "true");
        System.setProperty("zookeeper.ssl.keyStore.location", rsrcPath.apply("/server.jks"));
        System.setProperty("zookeeper.ssl.keyStore.password", "123456");
        System.setProperty("zookeeper.ssl.trustStore.location", rsrcPath.apply("/trust.jks"));
        System.setProperty("zookeeper.ssl.trustStore.password", "123456");
        System.setProperty("zookeeper.ssl.hostnameVerification", "false");
        System.setProperty(SECURITY_CLIENT_PORT, "2281");
    }

    /**
     *
     */
    private void clearSystemProperties() {
        System.setProperty("zookeeper.serverCnxnFactory", "");
        System.setProperty("zookeeper.client.secure", "");
        System.setProperty("zookeeper.ssl.keyStore.location", "");
        System.setProperty("zookeeper.ssl.keyStore.password", "");
        System.setProperty("zookeeper.ssl.trustStore.location", "");
        System.setProperty("zookeeper.ssl.trustStore.password", "");
        System.setProperty("zookeeper.ssl.hostnameVerification", "");
        System.setProperty(SECURITY_CLIENT_PORT, "");
    }
}
