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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.security.Permissions;
import java.util.Map;
import java.util.function.Consumer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestAuthenticationContextSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityProcessor.CLIENT;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Integration test for Grid REST functionality with user attributes; Jetty is under the hood.
 */
public class RestUserAttributesTest extends GridCommonAbstractTest {
    /** Jetty port. */
    private static final int JETTY_PORT = 8080;

    /** */
    private static Map<String, Object> userAttrs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setConnectorConfiguration(new ConnectorConfiguration());

        // Listener for setting 'userAttrs' field from AuthenticationContext during authentication. Required to make
        // sure the user attributes has actually been set to AuthenticationContext during REST connection.
        Consumer<AuthenticationContext> hndr = (ctx) -> userAttrs = ctx.nodeAttributes();

        configuration.setPluginProviders(
            new TestAuthenticationContextSecurityPluginProvider("client", null, ALLOW_ALL,
            true, true, hndr, clientData()));

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /**
     * Runs version command with user attributes.
     */
    @Test
    public void testVersionWithUserAttributes() throws Exception {
        userAttrs = null;

        URLConnection conn = new URL("http://localhost:" + JETTY_PORT +
            "/ignite?cmd=authenticate" +
            "&ignite.login=client" +
            "&userAttributes={\"add.sec.cliVer\":\"client_v1\",\"key\":\"val\"}")
            .openConnection();

        conn.connect();

        String sesToken = assertSuccesfulConnectionAndGetToken(conn);

        assertEquals(userAttrs.get("add.sec.cliVer"), "client_v1");
        assertEquals(userAttrs.get("key"), "val");

        String withToken = String.format("http://localhost:%s/ignite?cmd=version&sessionToken=%s", JETTY_PORT,
            sesToken);

        conn = new URL(withToken).openConnection();

        conn.connect();

        assertSuccesfulConnectionAndGetToken(conn);
    }

    /**
     * @param conn Connection.
     * @return Session token.
     * @throws IOException if failed.
     */
    private String assertSuccesfulConnectionAndGetToken(URLConnection conn) throws IOException {
        try (InputStreamReader streamReader = new InputStreamReader(conn.getInputStream())) {
            ObjectMapper objMapper = new ObjectMapper();
            Map<String, Object> myMap = objMapper.readValue(streamReader,
                new TypeReference<Map<String, Object>>() {
                });

            log.info("Version command response is: " + myMap);

            assertTrue(myMap.containsKey("response"));
            assertEquals(0, myMap.get("successStatus"));

            return myMap.get("sessionToken").toString();
        }
    }

    /**
     * @return Test data.
     */
    private TestSecurityData[] clientData() {
        return new TestSecurityData[]{new TestSecurityData(CLIENT,
            "pwd",
            SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                .appendSystemPermissions(ADMIN_OPS)
                .build(),
            new Permissions()
        )};
    }
}
