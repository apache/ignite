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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
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
     * &lt;JAVA_HOME&gt;/conf/security/java.security file.
     */
    @Test
    public void testDisabledCustomCipher() throws Exception {
        Set<String> disabledSuites = disabledByDefaultCipherSuites();
        String disabledSuite = disabledSuites.iterator().next();

        System.out.println("Run test with cipher suite: " + disabledSuite);

        setSslCtxFactoryToCli = true;
        supportedCiphers = new String[] {disabledSuite /* Disabled by default */};
        sslCtxFactory = getTestSslContextFactory();

        startGrids(1);
        try {
            // Explicit supported ciphers.
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                "&sslCipherSuites=" + disabledSuite +
                "&sslTrustAll=true" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
                checkConnection(conn);
            }

            //completely disabled jdk 17+

            //DES (56-bit):
            //TLS_RSA_WITH_DES_CBC_SHA, TLS_DHE_RSA_WITH_DES_CBC_SHA,
            //    TLS_DHE_DSS_WITH_DES_CBC_SHA, TLS_ECDHE_ECDSA_WITH_DES_CBC_SHA,
            //    TLS_ECDHE_RSA_WITH_DES_CBC_SHA, TLS_ECDHE_PSK_WITH_DES_CBC_SHA,
            //    TLS_ECDH_ECDSA_WITH_DES_CBC_SHA, TLS_ECDH_RSA_WITH_DES_CBC_SHA,
            //    TLS_ECDH_anon_WITH_DES_CBC_SHA

            //3DES/DESede:
            //TLS_RSA_WITH_3DES_EDE_CBC_SHA, TLS_DHE_RSA_WITH_3DES_EDE_CBC_SHA,
            //    TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA,
            //    TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA, TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
            //    TLS_ECDHE_PSK_WITH_3DES_EDE_CBC_SHA, TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA,
            //    TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA, TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA

            String completelyDisabledSuite = "TLS_RSA_WITH_DES_CBC_SHA";

            assertFalse(supportedCipherSuites().contains(completelyDisabledSuite));

            // Java 17+, the cipher suite TLS_RSA_WITH_NULL_SHA256 is completely disabled by default.
            GridTestUtils.assertThrows(log, () -> {
                return DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/?sslMode=require" +
                    "&sslCipherSuites=" + completelyDisabledSuite +
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
     * &lt;JAVA_HOME&gt;/conf/security/java.security file.
     */
    @Test
    public void testUnsupportedCustomCipher() throws Exception {
        String disabledSuite = disabledByDefaultCipherSuites().iterator().next();

        setSslCtxFactoryToCli = true;
        supportedCiphers = new String[] {
            disabledSuite /* Supported by JDK */,
            "TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA" /* Anonymous cipher is disabled by default */};
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
                "&sslCipherSuites=" + disabledSuite +
                "&sslTrustAll=true" +
                "&sslClientCertificateKeyStoreUrl=" + CLI_KEY_STORE_PATH +
                "&sslClientCertificateKeyStorePassword=123456" +
                "&sslTrustCertificateKeyStoreUrl=" + TRUST_KEY_STORE_PATH +
                "&sslTrustCertificateKeyStorePassword=123456")) {
                checkConnection(conn);
            }

            // Default ciphers.
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
            }, SQLException.class, "Unsupported key algorithm: INVALID");
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

    /** */
    private Set<String> supportedCipherSuites() throws Exception {
        // Initialize a standard SSL/TLS context to load all protocols
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, null, null);
        SSLSocketFactory factory = ctx.getSocketFactory();

        // Retrieve all available cipher suites
        return Set.of(factory.getSupportedCipherSuites());
    }

    /** */
    private Set<String> disabledByDefaultCipherSuites() throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLSv1.2");
        ctx.init(null, null, null);
        SSLSocketFactory factory = ctx.getSocketFactory();

        Set<String> dfltCiphersSuites = Set.of(factory.getDefaultCipherSuites());
        Set<String> supportedCiphersSuites = new HashSet<>(Arrays.stream(factory.getSupportedCipherSuites()).toList());

        // Fulter supported, but NOT in the default active list.
        supportedCiphersSuites.removeAll(dfltCiphersSuites);

        // Current TC settings.
        supportedCiphersSuites.removeIf(s -> s.contains("_anon_"));

        assertFalse("No one disabled by default suite found", supportedCiphersSuites.isEmpty());

        return supportedCiphersSuites;
    }
}
