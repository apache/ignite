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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.internal.SB;
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
    private static final String CLNT_ADMIN = "clnt_admin";

    /** */
    private static final String ALLOWED_CLNT_CACHE = "allowed_clnt_cache";

    /** */
    private static final String UNALLOWED_CLNT_CACHE = "unallowed_clnt_cache";

    /** */
    private static final String ALLOWED_CLNT_SYSTEM = "allowed_clnt_system";

    /** */
    private static final String UNALLOWED_CLNT_SYSTEM = "unallowed_clnt_system";

    /** */
    protected static final ObjectMapper JSON_MAPPER = new GridJettyObjectMapper();

    /** */
    private static final String STATUS_FLD = "successStatus";

    /** */
    private static final String ERROR_FLD = "error";

    /** */
    private static final String SUCCESS_STATUS = "0";

    /** */
    private static final String ERROR_STATUS = "3";

    /** */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** */
    private static final String UNDECLARED_CACHE = "UNDECLARED_CACHE";

    /** */
    private static final SecurityPermission[] EMPTY_PERM = new SecurityPermission[0];

    /** */
    private static final int DFLT_REST_PORT = 8091;

    /** */
    private static final String REST_URL ="http://" + LOC_HOST +":" + DFLT_REST_PORT + "/ignite?";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        TestSecurityData[] clientData = new TestSecurityData[] {
            new TestSecurityData(CLNT_ADMIN,
                SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                    .appendSystemPermissions(ADMIN_CACHE)
                    .build()
            ),
            new TestSecurityData(ALLOWED_CLNT_CACHE,
                SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                    .appendCachePermissions(CACHE_NAME, CACHE_CREATE, CACHE_DESTROY)
                    .build()
            ),
            new TestSecurityData(UNALLOWED_CLNT_CACHE,
                SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                    .appendCachePermissions(CACHE_NAME, EMPTY_PERM)
                    .build()
            ),
            new TestSecurityData(ALLOWED_CLNT_SYSTEM,
                SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                    .appendSystemPermissions(CACHE_CREATE, CACHE_DESTROY)
                    .build()
            ),
            new TestSecurityData(UNALLOWED_CLNT_SYSTEM,
                SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                    .build()
            )
        };

        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new TestSecurityPluginProvider(igniteInstanceName, "", ALLOW_ALL, false, clientData));
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
        assertNull(grid(0).cache(UNDECLARED_CACHE));

        assertEquals(SUCCESS_STATUS,
            jsonField(execute(CLNT_ADMIN, CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE), STATUS_FLD));
        assertEquals(SUCCESS_STATUS,
            jsonField(execute(CLNT_ADMIN, UNDECLARED_CACHE, GridRestCommand.GET_OR_CREATE_CACHE), STATUS_FLD));

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(UNDECLARED_CACHE));
    }

    /** */
    @Test
    public void testGetOrCreateWithCachePermission() throws Exception {
        checkFailWithError(execute(UNALLOWED_CLNT_CACHE, CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE),
            CACHE_CREATE);
        checkFailWithError(execute(UNALLOWED_CLNT_CACHE, UNDECLARED_CACHE, GridRestCommand.GET_OR_CREATE_CACHE),
            CACHE_CREATE);

        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(UNDECLARED_CACHE));

        assertEquals(SUCCESS_STATUS,
            jsonField(execute(ALLOWED_CLNT_CACHE, CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE), STATUS_FLD));

        checkFailWithError(execute(ALLOWED_CLNT_CACHE, UNDECLARED_CACHE, GridRestCommand.GET_OR_CREATE_CACHE),
            CACHE_CREATE);

        assertNotNull(grid(0).cache(CACHE_NAME));

        assertNull(grid(0).cache(UNDECLARED_CACHE));
    }

    /** */
    @Test
    public void testGetOrCreateWithSystemPermission() throws Exception {
        checkFailWithError(execute(UNALLOWED_CLNT_SYSTEM, CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE),
            CACHE_CREATE);
        checkFailWithError(execute(UNALLOWED_CLNT_SYSTEM, UNDECLARED_CACHE, GridRestCommand.GET_OR_CREATE_CACHE),
            CACHE_CREATE);

        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(UNDECLARED_CACHE));

        assertEquals(SUCCESS_STATUS,
            jsonField(execute(ALLOWED_CLNT_SYSTEM, CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE), STATUS_FLD));
        assertEquals(SUCCESS_STATUS,
            jsonField(execute(ALLOWED_CLNT_SYSTEM, UNDECLARED_CACHE, GridRestCommand.GET_OR_CREATE_CACHE), STATUS_FLD));

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(UNDECLARED_CACHE));
    }

    /** */
    @Test
    public void testDestroyCacheWithAdminPerms() throws Exception {
        assertNotNull(grid(0).getOrCreateCache(CACHE_NAME));
        assertNotNull(grid(0).getOrCreateCache(UNDECLARED_CACHE));

        assertEquals(SUCCESS_STATUS,
            jsonField(execute(CLNT_ADMIN, CACHE_NAME, GridRestCommand.DESTROY_CACHE), STATUS_FLD));
        assertEquals(SUCCESS_STATUS,
            jsonField(execute(CLNT_ADMIN, UNDECLARED_CACHE, GridRestCommand.DESTROY_CACHE), STATUS_FLD));

        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(UNDECLARED_CACHE));
    }

    /** */
    @Test
    public void testDestroyCacheWithCachePermissions() throws Exception {
        assertNotNull(grid(0).getOrCreateCache(CACHE_NAME));
        assertNotNull(grid(0).getOrCreateCache(UNDECLARED_CACHE));

        checkFailWithError(execute(UNALLOWED_CLNT_CACHE, CACHE_NAME, GridRestCommand.DESTROY_CACHE),
            CACHE_DESTROY);
        checkFailWithError(execute(UNALLOWED_CLNT_CACHE, UNDECLARED_CACHE, GridRestCommand.DESTROY_CACHE),
            CACHE_DESTROY);

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(UNDECLARED_CACHE));

        assertEquals(SUCCESS_STATUS,
            jsonField(execute(ALLOWED_CLNT_CACHE, CACHE_NAME, GridRestCommand.DESTROY_CACHE), STATUS_FLD));

        checkFailWithError(execute(ALLOWED_CLNT_CACHE, UNDECLARED_CACHE, GridRestCommand.DESTROY_CACHE),
            CACHE_DESTROY);

        assertNull(grid(0).cache(CACHE_NAME));

        assertNotNull(grid(0).cache(UNDECLARED_CACHE));
    }

    /** */
    @Test
    public void testDestroyCacheWithSystemPermissions() throws Exception {
        assertNotNull(grid(0).getOrCreateCache(CACHE_NAME));
        assertNotNull(grid(0).getOrCreateCache(UNDECLARED_CACHE));

        checkFailWithError(execute(UNALLOWED_CLNT_SYSTEM, CACHE_NAME, GridRestCommand.DESTROY_CACHE),
            CACHE_DESTROY);
        checkFailWithError(execute(UNALLOWED_CLNT_SYSTEM, UNDECLARED_CACHE, GridRestCommand.DESTROY_CACHE),
            CACHE_DESTROY);

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(UNDECLARED_CACHE));

        assertEquals(SUCCESS_STATUS,
            jsonField(execute(ALLOWED_CLNT_SYSTEM, CACHE_NAME, GridRestCommand.DESTROY_CACHE), STATUS_FLD));
        assertEquals(SUCCESS_STATUS,
            jsonField(execute(ALLOWED_CLNT_SYSTEM, UNDECLARED_CACHE, GridRestCommand.DESTROY_CACHE), STATUS_FLD));

        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(UNDECLARED_CACHE));
    }

    /**
     * Execute REST command and return result.
     *
     * @param login Login.
     * @param cacheName Cache name.
     * @param cmd Command.
     */
    protected String execute(String login, String cacheName, GridRestCommand cmd) throws Exception {
        SB sb = new SB(REST_URL);
        sb.a("ignite.login=").a(login).a("&")
            .a("ignite.password=&")
            .a("cacheName=").a(cacheName).a('&')
            .a("cmd=").a(cmd.key()).a('&');

        URL url = new URL(sb.toString());

        try (InputStream in = url.openConnection().getInputStream()) {
            StringBuilder buf = new StringBuilder(256);

            try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in, "UTF-8"))) {
                for (String line = rdr.readLine(); line != null; line = rdr.readLine())
                    buf.append(line);
            }

            return buf.toString();
        }
    }

    /**
     * @param json Json.
     * @param fieldName Field name.
     */
    private String jsonField(String json, String fieldName) throws IOException {
        return JSON_MAPPER.readTree(json).get(fieldName).asText();
    }

    /**
     * @param json JSON content.
     * @param perm Missing permission.
     */
    private void checkFailWithError(String json, SecurityPermission perm) throws IOException {
        assertEquals(ERROR_STATUS, jsonField(json, STATUS_FLD));

        assertTrue(JSON_MAPPER.readTree(json).get(ERROR_FLD).asText().contains("Authorization failed [perm=" + perm));
    }
}
