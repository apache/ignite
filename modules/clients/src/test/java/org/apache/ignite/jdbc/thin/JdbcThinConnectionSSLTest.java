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
import javax.net.ssl.SSLSocketFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * SSL connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinConnectionSSLTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Client key store path. */
    private static final String CLI_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/client.jks";

    /** Server key store path. */
    private static final String SRV_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/server.jks";

    /** Trust key store path. */
    private static final String TRUST_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/trust.jks";

    /** SSL context factory. */
    private static Factory<SSLContext> sslCtxFactory;

    /** Set SSL context factory to client listener. */
    private static boolean setSslCtxFactoryToCli;

    /** Set SSL context factory to ignite. */
    private static boolean setSslCtxFactoryToIgnite;

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
                .setUseIgniteSslContextFactory(setSslCtxFactoryToIgnite)
                .setSslClientAuth(true)
                .setSslContextFactory(setSslCtxFactoryToCli ? sslCtxFactory : null));

        cfg.setSslContextFactory(setSslCtxFactoryToIgnite ? sslCtxFactory : null);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnection() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
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
    public void testConnectionTrustAll() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustAll=true")) {
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
    public void testConnectionUseIgniteFactory() throws Exception {
        setSslCtxFactoryToIgnite = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
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
        setSslCtxFactoryToCli = true;
        // Factory return default SSL context
        sslCtxFactory = new Factory<SSLContext>() {
            @Override public SSLContext create() {
                try {
                    return SSLContext.getDefault();
                }
                catch (NoSuchAlgorithmException e) {
                    throw new IgniteException(e);
                }
            }
        };

        System.setProperty("javax.net.ssl.keyStore", CLI_KEY_STORE_PATH);
        System.setProperty("javax.net.ssl.keyStorePassword", "123456");
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEY_STORE_PATH);
        System.setProperty("javax.net.ssl.trustStorePassword", "123456");

        startGrids(1);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require")) {
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
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
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
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

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
        setSslCtxFactoryToCli = true;

        startGrids(1);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=invalid_client_keystore_path" +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not open client key store");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=invalid_cli_passwd" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not open client key store");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=invalid_trust_keystore_path" +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not open trusted key store");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=invalid_trust_passwd");

                    return null;
                }
            }, SQLException.class, "Could not open trusted key store");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslClientCertificateKeyStoreType=INVALID" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not create client KeyStore instance");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStoreType=INVALID" +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not create trust KeyStore instance");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnknownClientCertificate() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Connection c = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Failed to SSL connect to server");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnsupportedSslProtocol() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Connection c = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslProtocol=TLSv1.3" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "TLSv1.3 is not a valid SSL protocol");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidKeyAlgorithm() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Connection c = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslKeyAlgorithm=INVALID" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Default algorithm definitions for TrustManager and/or KeyManager are invalid");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidKeyStoreType() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Connection c = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreType=PKCS12" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Could not open client key store");
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
     * @return Test SSL context factory.
     */
    private static Factory<SSLContext> getTestSslContextFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(SRV_KEY_STORE_PATH);
        factory.setKeyStorePassword("123456".toCharArray());
        factory.setTrustStoreFilePath(TRUST_KEY_STORE_PATH);
        factory.setTrustStorePassword("123456".toCharArray());

        return factory;
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