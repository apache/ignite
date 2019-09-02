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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Base class for Zookeeper SPI discovery tests in this package. It is intended to provide common overrides for
 * superclass methods to be shared by all subclasses.
 */
public class ZookeeperDiscoverySpiSslTest extends ZookeeperDiscoverySpiTestBase {
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
            IgniteCheckedException.class,
            "Failed to start SPI: ZookeeperDiscoverySpi");
    }

    /**
     * Setup system properties.
     */
    private void setupSystemProperties() {
        System.setProperty("zookeeper.serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
        System.setProperty("zookeeper.client.secure", "true");
        System.setProperty("zookeeper.ssl.keyStore.location", rsrcPath.apply("/server.jks"));
        System.setProperty("zookeeper.ssl.keyStore.password", "123456");
        System.setProperty("zookeeper.ssl.trustStore.location", rsrcPath.apply("/trust.jks"));
        System.setProperty("zookeeper.ssl.trustStore.password", "123456");
        System.setProperty("zookeeper.ssl.hostnameVerification", "false");
    }

    /**
     * Cleanup system properties.
     */
    private void clearSystemProperties() {
        System.setProperty("zookeeper.clientCnxnSocket", "");
        System.setProperty("zookeeper.serverCnxnFactory", "");
        System.setProperty("zookeeper.client.secure", "");
        System.setProperty("zookeeper.ssl.keyStore.location", "");
        System.setProperty("zookeeper.ssl.keyStore.password", "");
        System.setProperty("zookeeper.ssl.trustStore.location", "");
        System.setProperty("zookeeper.ssl.trustStore.password", "");
        System.setProperty("zookeeper.ssl.hostnameVerification", "");
    }
}
