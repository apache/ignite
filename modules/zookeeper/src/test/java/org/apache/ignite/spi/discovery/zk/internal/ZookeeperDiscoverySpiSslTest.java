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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.junit.Test;

/**
 * Base class for Zookeeper SPI discovery tests in this package. It is intended to provide common overrides for
 * superclass methods to be shared by all subclasses.
 */
public class ZookeeperDiscoverySpiSslTest extends ZookeeperDiscoverySpiSslTestBase {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sslEnabled = true;

        setupSystemProperties();

        super.beforeTest();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgniteSsl() throws Exception {
        System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");

        IgniteEx ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgniteNoSsl() throws Exception {
        sslEnabled = false;

        System.setProperty("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");

        GridTestUtils.assertThrowsAnyCause(log,
            () -> startGrids(2),
            SessionExpiredException.class,
            "KeeperErrorCode = Session expired for /apacheIgnite");
    }

    /**
     * Setup system properties.
     */
    private void setupSystemProperties() {
        System.setProperty("zookeeper.serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
        System.setProperty("zookeeper.client.secure", "true");
        System.setProperty("zookeeper.ssl.keyStore.location", resourcePath("/server.jks"));
        System.setProperty("zookeeper.ssl.keyStore.password", "123456");
        System.setProperty("zookeeper.ssl.trustStore.location", resourcePath("/trust.jks"));
        System.setProperty("zookeeper.ssl.trustStore.password", "123456");
        System.setProperty("zookeeper.ssl.hostnameVerification", "false");
    }

    /**
     * @param rsrc Resource.
     * @return Path to the resource.
     */
    private String resourcePath(String rsrc) {
        String igniteHome = U.getIgniteHome();

        return Paths.get(
            igniteHome == null ? "." : igniteHome,
            "modules",
            "core",
            "src",
            "test",
            "resources",
            rsrc
        ).toString();
    }

    /**
     * Cleanup system properties.
     */
    private void clearSystemProperties() {
        System.clearProperty("zookeeper.clientCnxnSocket");
        System.clearProperty("zookeeper.serverCnxnFactory");
        System.clearProperty("zookeeper.client.secure");
        System.clearProperty("zookeeper.ssl.keyStore.location");
        System.clearProperty("zookeeper.ssl.keyStore.password");
        System.clearProperty("zookeeper.ssl.trustStore.location");
        System.clearProperty("zookeeper.ssl.trustStore.password");
        System.clearProperty("zookeeper.ssl.hostnameVerification");
    }
}
