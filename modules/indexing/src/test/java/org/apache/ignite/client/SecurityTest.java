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

package org.apache.ignite.client;

import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.filename.SharedFileTree;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.ignite.ssl.SslContextFactory.DFLT_KEY_ALGORITHM;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_STORE_TYPE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Thin client security test.
 */
public class SecurityTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int)GridTestUtils.DFLT_TEST_TIMEOUT);

    /** Ignite home. */
    private static final String IGNITE_HOME = U.getIgniteHome();

    /**
     * Setup before each test.
     */
    @Before
    public void beforeEach() throws IgniteCheckedException {
        SharedFileTree sft = new SharedFileTree(U.defaultWorkDirectory());

        U.delete(sft.db());

        assertTrue(sft.db().mkdirs());
    }

    /** Test SSL/TLS encryption. */
    @Test
    public void testEncryption() throws Exception {
        // Do not test old protocols.
        SslProtocol[] protocols = new SslProtocol[] {
            SslProtocol.TLS,
            SslProtocol.TLSv1_2,
            SslProtocol.TLSv1_3
        };

        for (SslProtocol protocol : protocols) {
            try {
                testEncryption(protocol);
            }
            catch (Throwable t) {
                throw new Exception("Failed for protocol: " + protocol, t);
            }
        }
    }

    /** Test SSL/TLS encryption. */
    private void testEncryption(SslProtocol protocol) {
        // Server-side security configuration
        IgniteConfiguration srvCfg = Config.getServerConfiguration();

        SslContextFactory sslCfg = new SslContextFactory();

        Function<String, String> rsrcPath = rsrc -> Paths.get(
            IGNITE_HOME == null ? "." : IGNITE_HOME,
            "modules",
            "core",
            "src",
            "test",
            "resources",
            rsrc
        ).toString();

        sslCfg.setKeyStoreFilePath(rsrcPath.apply("/server.jks"));
        sslCfg.setKeyStorePassword("123456".toCharArray());
        sslCfg.setTrustStoreFilePath(rsrcPath.apply("/trust.jks"));
        sslCfg.setTrustStorePassword("123456".toCharArray());

        srvCfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
        );

        srvCfg.setSslContextFactory(sslCfg);

        // Client-side security configuration
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses(Config.SERVER);

        try (Ignite ignored = Ignition.start(srvCfg)) {
            boolean failed;

            try (IgniteClient client = Ignition.startClient(clientCfg)) {
                client.<Integer, String>cache(Config.DEFAULT_CACHE_NAME).put(1, "1");

                failed = false;
            }
            catch (Exception ex) {
                failed = true;
            }

            assertTrue("Client connection without SSL must fail", failed);

            // Not using user-supplied SSL Context Factory:
            try (IgniteClient client = Ignition.startClient(clientCfg
                .setSslMode(SslMode.REQUIRED)
                .setSslClientCertificateKeyStorePath(rsrcPath.apply("/client.jks"))
                .setSslClientCertificateKeyStoreType(DFLT_STORE_TYPE)
                .setSslClientCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStorePath(rsrcPath.apply("/trust.jks"))
                .setSslTrustCertificateKeyStoreType(DFLT_STORE_TYPE)
                .setSslTrustCertificateKeyStorePassword("123456")
                .setSslKeyAlgorithm(DFLT_KEY_ALGORITHM)
                .setSslTrustAll(false)
                .setSslProtocol(protocol)
            )) {
                client.<Integer, String>cache(Config.DEFAULT_CACHE_NAME).put(1, "1");
            }

            // Using user-supplied SSL Context Factory
            try (IgniteClient client = Ignition.startClient(clientCfg
                .setSslMode(SslMode.REQUIRED)
                .setSslContextFactory(sslCfg)
            )) {
                client.<Integer, String>cache(Config.DEFAULT_CACHE_NAME).put(1, "1");
            }
        }
    }

    /** Test invalid user authentication. */
    @Test
    public void testInvalidUserAuthentication() {
        testInvalidUserAuthentication(client -> client.getOrCreateCache("testAuthentication"));
    }

    /** Test invalid user authentication with async method. */
    @Test
    public void testInvalidUserAuthenticationAsync() {
        testInvalidUserAuthentication(client -> {
            try {
                client.getOrCreateCacheAsync("testAuthentication").get();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
                throw (IgniteClientException)e.getCause();
            }
        });
    }

    /** Test valid user authentication. */
    private void testInvalidUserAuthentication(Consumer<IgniteClient> action) {
        Exception authError = null;

        try (Ignite ignored = igniteWithAuthentication();
             IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                 .setUserName("JOE")
                 .setUserPassword("password")
             )
        ) {
            action.accept(client);
        }
        catch (Exception e) {
            authError = e;
        }

        assertNotNull("Authentication with invalid credentials succeeded", authError);
        assertTrue("Invalid type of authentication error", authError instanceof ClientAuthenticationException);
    }

    /** Test valid user authentication. */
    @Test
    public void testValidUserAuthentication() throws Exception {
        final String USER = "joe";
        final String PWD = "password";

        try (Ignite ignored = igniteWithAuthentication(new SimpleEntry<>(USER, PWD));
             IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                 .setUserName(USER)
                 .setUserPassword(PWD)
             )
        ) {
            client.getOrCreateCache("testAuthentication");
        }
    }

    /** Test user cannot create user. */
    @Test
    public void testUserCannotCreateUser() throws Exception {
        final String USER = "joe";
        final String PWD = "password";

        try (Ignite ignored = igniteWithAuthentication(new SimpleEntry<>(USER, PWD));
             IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                 .setUserName(USER)
                 .setUserPassword(PWD)
             )
        ) {
            Exception authError = null;

            try {
                client.query(
                    new SqlFieldsQuery(String.format("CREATE USER \"%s\" WITH PASSWORD '%s'", "joe2", "password"))
                ).getAll();
            }
            catch (Exception e) {
                authError = e;
            }

            assertNotNull("User created another user", authError);
        }
    }

    /**
     * @return Ignite configuration with authentication enabled
     */
    @SafeVarargs
    private static Ignite igniteWithAuthentication(SimpleEntry<String, String>... users) throws Exception {
        Ignite ignite = Ignition.start(Config.getServerConfiguration()
            .setAuthenticationEnabled(true)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            )
        );

        ignite.cluster().state(ClusterState.ACTIVE);

        for (SimpleEntry<String, String> u : users)
            createUser(u.getKey(), u.getValue());

        return ignite;
    }

    /**
     * Create user.
     */
    private static void createUser(String user, String pwd) throws Exception {
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setUserName("ignite")
            .setUserPassword("ignite")
        )) {
            client.query(
                new SqlFieldsQuery(String.format("CREATE USER \"%s\" WITH PASSWORD '%s'", user, pwd))
            ).getAll();
        }
    }
}
