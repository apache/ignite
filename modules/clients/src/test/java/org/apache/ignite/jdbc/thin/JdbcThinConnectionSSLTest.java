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
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * SSL connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinConnectionSSLTest extends JdbcThinAbstractSelfTest {
    /** Client key store path. */
    private static final String CLI_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/client.jks";

    /** Server key store path. */
    private static final String SRV_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/server.jks";

    /** Trust key store path. */
    private static final String TRUST_KEY_STORE_PATH = U.getIgniteHome() +
        "/modules/clients/src/test/keystore/trust-one.jks";

    /** SSL context factory. */
    private static Factory<SSLContext> sslCtxFactory;

    /** Set SSL context factory to client listener. */
    private static boolean setSslCtxFactoryToCli;

    /** Set SSL context factory to ignite. */
    private static boolean setSslCtxFactoryToIgnite;

    /** Supported ciphers. */
    private static String[] supportedCiphers;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        setSslCtxFactoryToCli = false;
        setSslCtxFactoryToIgnite = false;
        supportedCiphers = null;
        sslCtxFactory = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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
    @Test
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
    @Test
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
    @Test
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
    @Test
    public void testDefaultContext() throws Exception {
        // Store exists default SSL context to restore after test.
        final SSLContext dfltSslCtx = SSLContext.getDefault();

        // Setup default context
        SSLContext.setDefault(getTestSslContextFactory().create());

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

        startGrids(1);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require")) {
            checkConnection(conn);
        }
        finally {
            stopAllGrids();

            // Restore SSL context.
            SSLContext.setDefault(dfltSslCtx);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
    public void testCustomCiphersOnClient() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            // Default ciphers
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
                checkConnection(conn);
            }

            // Explicit cipher (one of defaults).
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslCipherSuites=TLS_RSA_WITH_AES_256_CBC_SHA256" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
                checkConnection(conn);
            }

            // Explicit ciphers.
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslCipherSuites=TLS_RSA_WITH_NULL_SHA256,TLS_RSA_WITH_AES_256_CBC_SHA256" +
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
    @Test
    public void testCustomCiphersOnServer() throws Exception {
        setSslCtxFactoryToCli = true;
        supportedCiphers = new String[] {"TLS_RSA_WITH_AES_256_CBC_SHA256" /* Enabled by default */};
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            // Default ciphers.
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
                checkConnection(conn);
            }

            // Explicit cipher.
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslCipherSuites=TLS_RSA_WITH_AES_256_CBC_SHA256" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
                checkConnection(conn);
            }

            // Disabled by default cipher.
            GridTestUtils.assertThrows(log, () -> {
                return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                    "&sslCipherSuites=TLS_RSA_WITH_NULL_SHA256" +
                    "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                    "&sslClientCertificateKeyStorePassword=123456" +
                    "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                    "&sslTrustCertificateKeyStorePassword=123456");
            }, SQLException.class, "Failed to SSL connect to server");

            // Explicit ciphers.
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslCipherSuites=TLS_RSA_WITH_NULL_SHA256,TLS_RSA_WITH_AES_256_CBC_SHA256" +
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
     *
     * Note: Disabled cipher suite can be enabled via Java Security property "jdk.tls.disabledAlgorithms" or in
     * &lt;JRE_8_HOME&gt;/lib/security/java.security file.
     *
     * Note: java.security file location may be changed for Java 9+ version
     */
    @Test
    public void testDisabledCustomCipher() throws Exception {
        setSslCtxFactoryToCli = true;
        supportedCiphers = new String[] {"TLS_RSA_WITH_NULL_SHA256" /* Disabled by default */};
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);
        try {
            // Explicit supported ciphers.
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslCipherSuites=TLS_RSA_WITH_NULL_SHA256" +
                "&sslTrustAll=true" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
                checkConnection(conn);
            }

            // Default ciphers.
            GridTestUtils.assertThrows(log, () -> {
                return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                    "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                    "&sslClientCertificateKeyStorePassword=123456" +
                    "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                    "&sslTrustCertificateKeyStorePassword=123456");
            }, SQLException.class, "Failed to SSL connect to server");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     *
     * Note: Disabled cipher suite can be enabled via Java Security property "jdk.tls.disabledAlgorithms" or in
     * &lt;JRE_8_HOME&gt;/lib/security/java.security file.
     *
     * Note: java.security file location may be changed for Java 9+ version
     */
    @Test
    public void testUnsupportedCustomCipher() throws Exception {
        setSslCtxFactoryToCli = true;
        supportedCiphers = new String[] {
            "TLS_RSA_WITH_NULL_SHA256" /* Disabled by default */,
            "TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA" /* With disabled protocol*/};
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);
        try {
            // Enabled ciphers with unsupported algorithm can't be negotiated.
            GridTestUtils.assertThrows(log, () -> {
                return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                    "&sslCipherSuites=TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA" +
                    "&sslTrustAll=true" +
                    "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                    "&sslClientCertificateKeyStorePassword=123456" +
                    "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                    "&sslTrustCertificateKeyStorePassword=123456");
            }, SQLException.class, "Failed to SSL connect to server");

            // Supported cipher.
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslCipherSuites=TLS_RSA_WITH_NULL_SHA256" +
                "&sslTrustAll=true" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
                checkConnection(conn);
            }

            // Default ciphers.
            GridTestUtils.assertThrows(log, () -> {
                return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                    "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                    "&sslClientCertificateKeyStorePassword=123456" +
                    "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                    "&sslTrustCertificateKeyStorePassword=123456");
            }, SQLException.class, "Failed to SSL connect to server");

        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidCustomCipher() throws Exception {
        setSslCtxFactoryToCli = true;
        supportedCiphers = new String[] {"TLS_RSA_WITH_INVALID_SHA256"};
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            // Same invalid cipher.
            GridTestUtils.assertThrows(log, () -> {
                return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                    "&sslCipherSuites=TLS_RSA_WITH_INVALID_SHA256" +
                    "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                    "&sslClientCertificateKeyStorePassword=123456" +
                    "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                    "&sslTrustCertificateKeyStorePassword=123456");
            }, SQLException.class, "Failed to connect to server");

            // Default ciphers.
            GridTestUtils.assertThrows(log, () -> {
                return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                    "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                    "&sslClientCertificateKeyStorePassword=123456" +
                    "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                    "&sslTrustCertificateKeyStorePassword=123456");
            }, SQLException.class, "Failed to SSL connect to server");
        }
        finally {
            stopAllGrids();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
    public void testInvalidKeystoreConfig() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

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
            }, SQLException.class, "Failed to initialize key store (key store file was not found):");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=invalid_cli_passwd" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Failed to initialize key store (I/O error occurred):");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=invalid_trust_keystore_path" +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Failed to initialize key store (key store file was not found):");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=invalid_trust_passwd");

                    return null;
                }
            }, SQLException.class, "Failed to initialize key store (I/O error occurred):");

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
            }, SQLException.class, "Failed to initialize key store (security exception occurred) [type=INVALID,");

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
            }, SQLException.class, "Failed to initialize key store (security exception occurred) [type=INVALID,");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
            }, SQLException.class, "connect to");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUnsupportedSslProtocol() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Connection c = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslProtocol=TLSv1.13" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Unsupported SSL protocol: TLSv1.13");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
            }, SQLException.class, "Unsupported keystore algorithm: INVALID");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidKeyStoreType() throws Exception {
        setSslCtxFactoryToCli = true;
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Connection c = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                        "&sslClientCertificateKeyStoreType=INVALID_TYPE" +
                        "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                        "&sslClientCertificateKeyStorePassword=123456" +
                        "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                        "&sslTrustCertificateKeyStorePassword=123456");

                    return null;
                }
            }, SQLException.class, "Failed to initialize key store (security exception occurred) [type=INVALID_TYPE");
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

        factory.setCipherSuites(supportedCiphers);
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
