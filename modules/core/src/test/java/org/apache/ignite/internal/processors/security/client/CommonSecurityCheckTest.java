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

package org.apache.ignite.internal.processors.security.client;

import java.util.Arrays;
import java.util.Map;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.UserAttributesFactory;
import org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.jetbrains.annotations.NotNull;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityProcessor.CLIENT;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Common parent for security tests for thin client.
 */
@RunWith(JUnit4.class)
public abstract class CommonSecurityCheckTest extends AbstractSecurityTest {
    /** */
    protected final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** */
    protected boolean fail;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @return Test data.
     */
    protected TestSecurityData[] clientData() {
        return new TestSecurityData[]{new TestSecurityData(CLIENT,
            SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                .appendSystemPermissions(ADMIN_OPS, CACHE_CREATE)
                .build()
        )};
    }

    /**
     * @param instanceName Instance name.
     */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setActiveOnStart(false);

        boolean isClient = instanceName.endsWith("2");
        String name = isClient ? "client_" + instanceName : "srv_" + instanceName;

        cfg.setPluginProviders(getPluginProvider(name));

        SslContextFactory sslFactory = (SslContextFactory) GridTestUtils.sslFactory();

        cfg.setSslContextFactory(sslFactory);
        cfg.setConnectorConfiguration(new ConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
            .setSslClientAuth(true)
            .setSslFactory(sslFactory));

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
            .setUseIgniteSslContextFactory(false)
            .setSslContextFactory(sslFactory));

        if (instanceName.endsWith("0"))
            cfg.setGridLogger(listeningLog);

        if (isClient)
            cfg.setClientMode(true);

        if (!fail) {
            Map<String, String> attrs = new UserAttributesFactory().create();

            cfg.setUserAttributes(attrs);
        }

        return cfg;
    }

    /**
     * @return Grid client configuration.
     */
    protected GridClientConfiguration getGridClientConfiguration() {
        Map<String, String> userAttrs = new UserAttributesFactory().create();

        if (fail)
            userAttrs.clear();

        return new GridClientConfiguration()
            .setSslContextFactory(getClientSslContextFactory()::create)
            .setRouters(Arrays.asList("127.0.0.1:11211", "127.0.0.1:11212"))
            .setSecurityCredentialsProvider(
                new SecurityCredentialsBasicProvider(new SecurityCredentials(CLIENT, "")))
            .setUserAttributes(userAttrs);
    }

    /**
     * @return Client configuration.
     */
    protected ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
            .setSslContextFactory(getClientSslContextFactory())
            .setAddresses(Config.SERVER)
            .setUserName(CLIENT)
            .setUserPassword("")
            .setUserAttributes(fail ? null : new UserAttributesFactory().create())
            .setSslMode(SslMode.REQUIRED);
    }

    /**
     * @return SSL context factory for clients.
     */
    @NotNull protected SslContextFactory getClientSslContextFactory() {
        SslContextFactory sslFactory = (SslContextFactory) GridTestUtils.sslFactory();

        sslFactory.setKeyStoreFilePath(U.resolveIgnitePath(GridTestProperties.getProperty("ssl.keystore.client.path"))
            .getAbsolutePath());

        return sslFactory;
    }

    /**
     * @param name Ignite instance name
     * @return Plugin provider.
     */
    protected PluginProvider<?> getPluginProvider(String name) {
        return new TestAdditionalSecurityPluginProvider(name, null, ALLOW_ALL,
            globalAuth, true, clientData());
    }
}
