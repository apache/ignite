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

import java.util.AbstractMap.SimpleEntry;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.ssl.SslContextFactory;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Thin client security test.
 */
public class SecurityTest {
    /** Test SSL/TLS encryption. */
    @Test
    public void testEncryption() throws Exception {
        // Server-side security configuration
        IgniteConfiguration srvCfg = Config.getServerConfiguration();

        SslContextFactory sslCfg = new SslContextFactory();

        Function<String, String> rsrcPath = rsrc -> SecurityTest.class.getResource(rsrc).getPath();

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
                .setSslClientCertificateKeyStoreType("JKS")
                .setSslClientCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStorePath(rsrcPath.apply("/trust.jks"))
                .setSslTrustCertificateKeyStoreType("JKS")
                .setSslTrustCertificateKeyStorePassword("123456")
                .setSslKeyAlgorithm("SunX509")
                .setSslTrustAll(false)
                .setSslProtocol(SslProtocol.TLS)
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

    /** Test authentication. */
    @Test
    public void testAuthentication() throws IgniteCheckedException {
        try (Ignite ignite = Ignition.start(Config.getServerConfiguration()
            .setAuthenticationEnabled(true)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            )
        )) {
            ignite.cluster().active(true);

            Function<SimpleEntry<String, String>, Exception> authenticate = cred -> {
                ClientConfiguration clientCfg = new ClientConfiguration().setAddresses(Config.SERVER)
                    .setUserName(cred.getKey())
                    .setUserPassword(cred.getValue());

                try (IgniteClient client = Ignition.startClient(clientCfg)) {
                    client.getOrCreateCache("testAuthentication");
                }
                catch (Exception e) {
                    return e;
                }

                return null;
            };

            assertTrue(
                "Authentication with invalid credentials succeeded",
                authenticate.apply(new SimpleEntry<>("bad-user", "bad-password"))
                    instanceof ClientAuthenticationException
            );

            SimpleEntry<String, String> validCred = new SimpleEntry<>("user", "password");

            IgniteEx igniteEx = (IgniteEx)ignite;

            AuthorizationContext.context(igniteEx.context().authentication().authenticate("ignite", "ignite"));

            try {
                igniteEx.context().authentication().addUser(validCred.getKey(), validCred.getValue());
            }
            catch (IgniteCheckedException ignore) {
                // Ignore "user already exists" exception
            }

            assertNull(
                "Authentication with valid credentials failed",
                authenticate.apply(validCred)
            );
        }
    }
}
