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

import java.util.Collections;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginConfiguration;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/** Security tests for client. */
public class GridClientPermissionCheckTest extends AbstractSecurityTest {
    /** Host. */
    private static final String HOST = "127.0.0.1";
    /** Admin. */
    private static final String ADMIN = "admin";
    /** User. */
    private static final String USER = "user";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestSecurityPluginConfiguration secCfg = secPluginCfg(ADMIN, ADMIN, ALLOW_ALL,
            new TestSecurityData(USER, USER,
                new SecurityPermissionSetBuilder().defaultAllowAll(false).build()
            ));

        DataStorageConfiguration dsc = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dsc)
            .setConnectorConfiguration(new ConnectorConfiguration().setPort(ConnectorConfiguration.DFLT_TCP_PORT))
            .setAuthenticationEnabled(true)
            .setPluginConfigurations(secCfg);

        return cfg;
    }

    /**
     * @param login Login.
     * @param pwd   Password.
     * @return Client.
     * @throws GridClientException In case of error.
     */
    protected GridClient client(String login, String pwd) throws GridClientException {
        GridClientConfiguration cfg = new GridClientConfiguration()
            .setSecurityCredentialsProvider(new SecurityCredentialsBasicProvider(new SecurityCredentials(login, pwd)))
            .setProtocol(GridClientProtocol.TCP)
            .setServers(Collections.singleton(HOST + ":" + ConnectorConfiguration.DFLT_TCP_PORT));
        return GridClientFactory.start(cfg);
    }

    /**
     * Test that getting cluster status is working without ADMIN_OPS permissions,
     * but setting cluster status causes an error.
     *
     * @throws Exception If failed.
     */
    public void testClusterStatus() throws Exception {
        IgniteEx ignite = startGrids(1);

        ignite.cluster().active(true);

        try (GridClient client = client(USER, USER)) {
            assertTrue(client.state().active());

            GridTestUtils.assertThrows(log, () -> client.state().active(false), IgniteException.class, "Authorization failed [perm=ADMIN_OPS");

            assertTrue(client.state().active());
        }
    }
}
