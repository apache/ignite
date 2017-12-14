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

package org.apache.ignite.jdbc.thin;

import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocketFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.client.ssl.GridSslBasicContextFactory;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinConnectionSSLTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";
    /** Ssl context factory. */
    private static Factory<SSLContext> sslContextFactory;

    /**
     * @return Test SSL context factory.
     */
    private static Factory<SSLContext> getTestSslContextFactory() {
        final GridSslBasicContextFactory factory = (GridSslBasicContextFactory)GridTestUtils.sslContextFactory();

        factory.setKeyStoreFilePath("src/test/keystore/client.jks");
        factory.setKeyStorePassword("123456".toCharArray());
        factory.setTrustStoreFilePath("src/test/keystore/server.jks");
        factory.setTrustStorePassword("123456".toCharArray());

        return new Factory<SSLContext>() {
            @Override public SSLContext create() {
                try {
                    return factory.createSslContext();
                }
                catch (SSLException e) {
                    throw new IgniteException(e);
                }
            }
        };
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClientConnectorConfiguration(
            new ClientConnectorConfiguration()
                .setSslEnabled(true)
                .setSslContextFactory(sslContextFactory));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnection() throws Exception {
        sslContextFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?useSSL=true&" +
                "clientCertificateKeyStoreUrl=src/test/keystore/client.jks&" +
                "clientCertificateKeyStorePassword=123456&" +
                "trustCertificateKeyStoreUrl=src/test/keystore/server.jks&" +
                "trustCertificateKeyStorePassword=123456"
            )) {
                checkConnection(conn);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultContext() throws Exception {
        // Factory return default SSL context
        sslContextFactory = new Factory<SSLContext>() {
            @Override public SSLContext create() {
                try {
                    return SSLContext.getDefault();
                }
                catch (NoSuchAlgorithmException e) {
                    throw new IgniteException(e);
                }
            }
        };

        System.setProperty("javax.net.ssl.keyStore", "src/test/keystore/client.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "123456");
        System.setProperty("javax.net.ssl.trustStore", "src/test/keystore/server.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "123456");

        startGrids(1);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?useSSL=true" +
            "&sslUseDefault=true")) {
            checkConnection(conn);
        }
        finally {
            System.getProperties().remove("javax.net.ssl.keyStore");
            System.getProperties().remove("javax.net.ssl.keyStorePassword");
            System.getProperties().remove("javax.net.ssl.trustStore");
            System.getProperties().remove("javax.net.ssl.trustStorePassword");

            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testContextFactory() throws Exception {
        sslContextFactory = getTestSslContextFactory();

        startGrids(1);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?useSSL=true" +
            "&sslFactory=" + TestSSLFactory.class.getName())) {
            checkConnection(conn);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSslServerAndPlainClient() throws Exception {
        sslContextFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");

                    return null;
                }
            }, SQLException.class, "Failed to connect to Ignite cluster");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidKeystoreConfig() throws Exception {
        startGrids(1);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?useSSL=true&" +
                        "clientCertificateKeyStoreUrl=invalid_client_keystore_path&" +
                        "clientCertificateKeyStorePassword=123456&" +
                        "trustCertificateKeyStoreUrl=src/test/keystore/server.jks&" +
                        "trustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not open client key store");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?useSSL=true&" +
                        "clientCertificateKeyStoreUrl=src/test/keystore/client.jks&" +
                        "clientCertificateKeyStorePassword=invalid_cli_passwd&" +
                        "trustCertificateKeyStoreUrl=src/test/keystore/server.jks&" +
                        "trustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not open client key store");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?useSSL=true&" +
                        "clientCertificateKeyStoreUrl=src/test/keystore/client.jks&" +
                        "clientCertificateKeyStorePassword=123456&" +
                        "trustCertificateKeyStoreUrl=invalid_trust_keystore_path&" +
                        "trustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not open trusted key store");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?useSSL=true&" +
                        "clientCertificateKeyStoreUrl=src/test/keystore/client.jks&" +
                        "clientCertificateKeyStorePassword=123456&" +
                        "trustCertificateKeyStoreUrl=src/test/keystore/server.jks&" +
                        "trustCertificateKeyStorePassword=invalid_trust_passwd");

                    return null;
                }
            }, SQLException.class, "Could not open trusted key store");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?useSSL=true&" +
                        "clientCertificateKeyStoreUrl=src/test/keystore/client.jks&" +
                        "clientCertificateKeyStorePassword=123456&" +
                        "clientCertificateKeyStoreType=INVALID&" +
                        "trustCertificateKeyStoreUrl=src/test/keystore/server.jks&" +
                        "trustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not create client KeyStore instance");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?useSSL=true&" +
                        "clientCertificateKeyStoreUrl=src/test/keystore/client.jks&" +
                        "clientCertificateKeyStorePassword=123456&" +
                        "trustCertificateKeyStoreUrl=src/test/keystore/server.jks&" +
                        "trustCertificateKeyStoreType=INVALID&" +
                        "trustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not create trust KeyStore instance");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param conn Connection to check.
     * @throws SQLException On failed.
     */
    public void checkConnection(Connection conn) throws SQLException {
        assertEquals("PUBLIC", conn.getSchema());

        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT 1");

            assertTrue(rs.next());

            assertEquals(1, rs.getInt(1));
        }
    }

    /**
     *
     */
    public static class TestSSLFactory implements Factory<SSLSocketFactory> {
        /** {@inheritDoc} */
        @Override public SSLSocketFactory create() {
            return getTestSslContextFactory().create().getSocketFactory();
        }
    }
}