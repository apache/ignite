/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
            }, SQLException.class, "Default algorithm definitions for TrustManager and/or KeyManager are invalid");
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
