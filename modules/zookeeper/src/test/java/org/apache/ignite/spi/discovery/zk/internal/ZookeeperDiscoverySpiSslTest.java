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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.zookeeper.client.ZKClientConfig.SECURE_CLIENT;
import static org.apache.zookeeper.server.ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY;

/**
 * Ignite nodes should be started in different JVM, because ZK server can read system properties on Ignite connection.
 * So, SSL parameters will be the same.
 */
public class ZookeeperDiscoverySpiSslTest extends ZookeeperDiscoverySpiSslTestBase {
    /** */
    public static final String ZOOKEEPER_CLIENT_CNXN_SOCKET = "zookeeper.clientCnxnSocket";

    /** */
    public static final String ZOOKEEPER_SSL_KEYSTORE_LOCATION = "zookeeper.ssl.keyStore.location";

    /** */
    public static final String ZOOKEEPER_SSL_TRUSTSTORE_LOCATION = "zookeeper.ssl.trustStore.location";

    /** */
    public static final String ZOOKEEPER_SSL_KEYSTORE_PASSWORD = "zookeeper.ssl.keyStore.password";

    /** */
    public static final String ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD = "zookeeper.ssl.trustStore.password";

    /** */
    public static final String ZOOKEEPER_SSL_HOSTNAME_VERIFICATION = "zookeeper.ssl.hostnameVerification";

    /** */
    boolean invalidKeystore;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sslEnabled = true;
        invalidKeystore = false;

        setupSystemProperties();

        super.beforeTest();

        System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, "org.apache.zookeeper.ClientCnxnSocketNetty");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        clearSystemProperties();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgniteSsl() throws Exception {
        IgniteEx ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgniteSslWrongPort() throws Exception {
        startGrid(0);

        sslEnabled = false;

        GridTestUtils.assertThrowsAnyCause(log,
            () -> startGrid(1),
            AssertionError.class,
            "Remote node has not joined");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testIgniteSslWrongKeystore() throws Exception {
        startGrid(0);

        invalidKeystore = true;

        GridTestUtils.assertThrowsAnyCause(log,
            () -> startGrid(1),
            AssertionError.class,
            "Remote node has not joined");
    }

    /**
     * Cleanup system properties.
     */
    private void clearSystemProperties() {
        System.clearProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET);
        System.clearProperty(ZOOKEEPER_SERVER_CNXN_FACTORY);
        System.clearProperty(SECURE_CLIENT);
        System.clearProperty(ZOOKEEPER_SSL_KEYSTORE_LOCATION);
        System.clearProperty(ZOOKEEPER_SSL_TRUSTSTORE_LOCATION);
        System.clearProperty(ZOOKEEPER_SSL_KEYSTORE_PASSWORD);
        System.clearProperty(ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD);
        System.clearProperty(ZOOKEEPER_SSL_HOSTNAME_VERIFICATION);
    }

    /**
     * Setup system properties.
     */
    private void setupSystemProperties() {
        System.setProperty(ZOOKEEPER_SSL_KEYSTORE_LOCATION, resourcePath("/server.jks"));
        System.setProperty(ZOOKEEPER_SSL_TRUSTSTORE_LOCATION, resourcePath("/trust.jks"));
        System.setProperty(ZOOKEEPER_SSL_KEYSTORE_PASSWORD, "123456");
        System.setProperty(ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD, "123456");
        System.setProperty(ZOOKEEPER_SERVER_CNXN_FACTORY, "org.apache.zookeeper.server.NettyServerCnxnFactory");
        System.setProperty(SECURE_CLIENT, "true");
        System.setProperty(ZOOKEEPER_SSL_HOSTNAME_VERIFICATION, "false");
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        ArrayList<String> args = new ArrayList<>(7);

        if (invalidKeystore) {
            args.add("-D" + ZOOKEEPER_SSL_KEYSTORE_LOCATION + '=' + resourcePathForKeystore("/node01.jks"));
            args.add("-D" + ZOOKEEPER_SSL_TRUSTSTORE_LOCATION + '=' + resourcePathForKeystore("/trust-one.jks"));
        }
        else {
            args.add("-D" + ZOOKEEPER_SSL_KEYSTORE_LOCATION + '=' + resourcePath("/server.jks"));
            args.add("-D" + ZOOKEEPER_SSL_TRUSTSTORE_LOCATION + '=' + resourcePath("/trust.jks"));
        }

        args.add("-D" + ZOOKEEPER_SSL_KEYSTORE_PASSWORD + "=123456");
        args.add("-D" + ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD + "=123456");
        args.add("-D" + ZOOKEEPER_CLIENT_CNXN_SOCKET + "=org.apache.zookeeper.ClientCnxnSocketNetty");
        args.add("-D" + SECURE_CLIENT + "=true");
        args.add("-D" + ZOOKEEPER_SSL_HOSTNAME_VERIFICATION + "=false");

        return args;
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
     * @param rsrc Resource.
     * @return Path to the resource.
     */
    private String resourcePathForKeystore(String rsrc) {
        String igniteHome = U.getIgniteHome();

        return Paths.get(
            igniteHome == null ? "." : igniteHome,
            "modules",
            "clients",
            "src",
            "test",
            "keystore",
            "ca",
            rsrc
        ).toString();
    }
}
