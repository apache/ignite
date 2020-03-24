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

package org.apache.ignite.internal.processors.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/** */
public class JettyRestProcessorSecurityCreateDestroyPermissionTest extends AbstractRestProcessorSelfTest {
    /** */
    private static final String CLIENT_WITH_ADMIN_PERMS = "client_with_admin_perms";

    /** */
    private static final String CLIENT_WITH_CACHE_PERMS = "client_with_cache_perms";

    /** */
    private static final String CLIENT_WITH_SYS_PERMS = "client_with_system_perms";

    /** */
    private static final String CLIENT_WITHOUT_PERMS = "client_without_perms";

    /** */
    private static final ObjectMapper JSON_MAPPER = new GridJettyObjectMapper();

    /** */
    private static final String SUCCESS_STATUS = "0";

    /** */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** */
    private static final String UNMANAGED_CACHE = "UNMANAGED_CACHE";

    /** */
    private static final int DFLT_REST_PORT = 8091;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TestSecurityData[] clientData = new TestSecurityData[] {
            new TestSecurityData(CLIENT_WITH_ADMIN_PERMS,
                createBuilder()
                    .appendSystemPermissions(ADMIN_CACHE)
                    .build()
            ),
            new TestSecurityData(CLIENT_WITH_CACHE_PERMS,
                createBuilder()
                    .appendCachePermissions(CACHE_NAME, CACHE_CREATE, CACHE_DESTROY)
                    .build()
            ),
            new TestSecurityData(CLIENT_WITH_SYS_PERMS,
                createBuilder()
                    .appendSystemPermissions(CACHE_CREATE, CACHE_DESTROY)
                    .build()
            ),
            new TestSecurityData(CLIENT_WITHOUT_PERMS,
                createBuilder()
                    .build()
            )
        };

        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(
                new TestSecurityPluginProvider(igniteInstanceName, "", ALLOW_ALL, false, clientData));
    }

    /** */
    private SecurityPermissionSetBuilder createBuilder() {
        return SecurityPermissionSetBuilder.create().defaultAllowAll(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_JETTY_PORT, String.valueOf(DFLT_REST_PORT));

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteEx server = grid(0);

        server.cacheNames().forEach(server::destroyCache);
    }

    /** */
    @Test
    public void testGetOrCreateWithAdminPerms() throws Exception {
        assertNull(grid(0).cache(CACHE_NAME));

        assertEquals(SUCCESS_STATUS,
            status(execute(CLIENT_WITH_ADMIN_PERMS, CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE)));

        assertNotNull(grid(0).cache(CACHE_NAME));
    }

    /** */
    @Test
    public void testGetOrCreateWithCachePermission() throws Exception {
        checkFailWithError(execute(CLIENT_WITHOUT_PERMS, CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE),
            CACHE_CREATE);
        checkFailWithError(execute(CLIENT_WITHOUT_PERMS, UNMANAGED_CACHE, GridRestCommand.GET_OR_CREATE_CACHE),
            CACHE_CREATE);

        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(UNMANAGED_CACHE));

        assertEquals(SUCCESS_STATUS,
            status(execute(CLIENT_WITH_CACHE_PERMS, CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE)));

        checkFailWithError(execute(CLIENT_WITH_CACHE_PERMS, UNMANAGED_CACHE, GridRestCommand.GET_OR_CREATE_CACHE),
            CACHE_CREATE);

        assertNotNull(grid(0).cache(CACHE_NAME));

        assertNull(grid(0).cache(UNMANAGED_CACHE));
    }

    /** */
    @Test
    public void testGetOrCreateWithSystemPermission() throws Exception {
        assertNull(grid(0).cache(CACHE_NAME));

        assertEquals(SUCCESS_STATUS,
            status(execute(CLIENT_WITH_SYS_PERMS, CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE)));

        assertNotNull(grid(0).cache(CACHE_NAME));
    }

    /** */
    @Test
    public void testDestroyCacheWithAdminPerms() throws Exception {
        assertNotNull(grid(0).getOrCreateCache(CACHE_NAME));

        assertEquals(SUCCESS_STATUS,
            status(execute(CLIENT_WITH_ADMIN_PERMS, CACHE_NAME, GridRestCommand.DESTROY_CACHE)));

        assertNull(grid(0).cache(CACHE_NAME));
    }

    /** */
    @Test
    public void testDestroyCacheWithCachePermissions() throws Exception {
        assertNotNull(grid(0).getOrCreateCache(CACHE_NAME));
        assertNotNull(grid(0).getOrCreateCache(UNMANAGED_CACHE));

        checkFailWithError(execute(CLIENT_WITHOUT_PERMS, CACHE_NAME, GridRestCommand.DESTROY_CACHE),
            CACHE_DESTROY);
        checkFailWithError(execute(CLIENT_WITHOUT_PERMS, UNMANAGED_CACHE, GridRestCommand.DESTROY_CACHE),
            CACHE_DESTROY);

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(UNMANAGED_CACHE));

        assertEquals(SUCCESS_STATUS,
            status(execute(CLIENT_WITH_CACHE_PERMS, CACHE_NAME, GridRestCommand.DESTROY_CACHE)));

        checkFailWithError(execute(CLIENT_WITH_CACHE_PERMS, UNMANAGED_CACHE, GridRestCommand.DESTROY_CACHE),
            CACHE_DESTROY);

        assertNull(grid(0).cache(CACHE_NAME));

        assertNotNull(grid(0).cache(UNMANAGED_CACHE));
    }

    /** */
    @Test
    public void testDestroyCacheWithSystemPermissions() throws Exception {
        assertNotNull(grid(0).getOrCreateCache(CACHE_NAME));

        assertEquals(SUCCESS_STATUS,
            status(execute(CLIENT_WITH_SYS_PERMS, CACHE_NAME, GridRestCommand.DESTROY_CACHE)));

        assertNull(grid(0).cache(CACHE_NAME));
    }

    /**
     * Execute REST command and return result.
     *
     * @param login Login.
     * @param cacheName Cache name.
     * @param cmd Command.
     */
    protected String execute(String login, String cacheName, GridRestCommand cmd) throws Exception {
        URL url = new URL(String.format("http://%s:%d/ignite?ignite.login=%s&ignite.password=&cacheName=%s&cmd=%s&",
            LOC_HOST, DFLT_REST_PORT, login, cacheName , cmd.key()));

        try (InputStream in = url.openConnection().getInputStream()) {
            StringBuilder buf = new StringBuilder(256);

            try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                for (String line = rdr.readLine(); line != null; line = rdr.readLine())
                    buf.append(line);
            }

            return buf.toString();
        }
    }

    /**
     * @param json Json.
     */
    private String status(String json) throws IOException {
        return JSON_MAPPER.readTree(json).get("successStatus").asText();
    }

    /**
     * @param json Json.
     */
    private String errorMessage(String json) throws IOException {
        return JSON_MAPPER.readTree(json).get("error").asText();
    }

    /**
     * @param json JSON content.
     * @param perm Missing permission.
     */
    private void checkFailWithError(String json, SecurityPermission perm) throws IOException {
        assertFalse(SUCCESS_STATUS.equals(status(json)));

        assertTrue(JSON_MAPPER.readTree(json).get("error").asText().contains("Authorization failed [perm=" + perm));
    }
}
