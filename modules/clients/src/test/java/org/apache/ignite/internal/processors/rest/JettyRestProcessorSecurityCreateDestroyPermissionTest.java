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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SECURITY_CHECK_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/** */
public class JettyRestProcessorSecurityCreateDestroyPermissionTest extends JettyRestProcessorCommonSelfTest {
    /** Admin client. */
    private static final String ADMIN = "admin";

    /** Allowed client with cache permission. */
    private static final String ALLOWED_CLNT_CACHE = "allowed_clnt_cache";

    /** Unallowed client without cache permission. */
    private static final String UNALLOWED_CLNT_CACHE = "unallowed_clnt_cache";

    /** Allowed client with system permission. */
    private static final String ALLOWED_CLNT_SYSTEM = "allowed_clnt_system";

    /** Unallowed client without system permission. */
    private static final String UNALLOWED_CLNT_SYSTEM = "unallowed_clnt_system";

    /** Default password. */
    private static final String DFLT_PWD = "ignite";

    /** Status. */
    private static final String STATUS = "successStatus";

    /** Success status. */
    private static final String SUCCESS_STATUS = String.valueOf(STATUS_SUCCESS);

    /** Error status. */
    private static final String ERROR_STATUS = String.valueOf(STATUS_SECURITY_CHECK_FAILED);

    /** Cache name for tests. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** Not declared cache name for tests. */
    private static final String NOT_DECLARED_CACHE = "NOT_DECLARED_CACHE";

    /** Empty permission. */
    private static final SecurityPermission[] EMPTY_PERM = new SecurityPermission[0];

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        TestSecurityData[] clientData = new TestSecurityData[] {
            new TestSecurityData(ADMIN,
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

        PluginProvider pluginProvider = new TestSecurityPluginProvider(igniteInstanceName, DFLT_PWD,
            ALLOW_ALL, false, clientData);

        cfg.setPluginProviders(pluginProvider);
        cfg.setCacheConfiguration(null);


        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteEx server = grid(0);

        server.cacheNames().forEach(server::destroyCache);

        super.afterTest();
    }

    /** */
    @Test
    public void testGetOrCreateWithAdminPerms() throws Exception {
        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(NOT_DECLARED_CACHE));

        checkSucces(restGetOrCreate(ADMIN, CACHE_NAME));
        checkSucces(restGetOrCreate(ADMIN, NOT_DECLARED_CACHE));

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(NOT_DECLARED_CACHE));
    }

    /** */
    @Test
    public void testGetOrCreateWithCachePermission() throws Exception {
        checkFailWithError(restGetOrCreate(UNALLOWED_CLNT_CACHE, CACHE_NAME), CACHE_CREATE);
        checkFailWithError(restGetOrCreate(UNALLOWED_CLNT_CACHE, NOT_DECLARED_CACHE), CACHE_CREATE);

        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(NOT_DECLARED_CACHE));

        checkSucces(restGetOrCreate(ALLOWED_CLNT_CACHE, CACHE_NAME));

        checkFailWithError(restGetOrCreate(ALLOWED_CLNT_CACHE, NOT_DECLARED_CACHE), CACHE_CREATE);

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(NOT_DECLARED_CACHE));
    }

    /** */
    @Test
    public void testGetOrCreateWithSystemPermission() throws Exception {
        checkFailWithError(restGetOrCreate(UNALLOWED_CLNT_SYSTEM, CACHE_NAME), CACHE_CREATE);
        checkFailWithError(restGetOrCreate(UNALLOWED_CLNT_SYSTEM, NOT_DECLARED_CACHE), CACHE_CREATE);

        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(NOT_DECLARED_CACHE));

        checkSucces(restGetOrCreate(ALLOWED_CLNT_SYSTEM, CACHE_NAME));
        checkSucces(restGetOrCreate(ALLOWED_CLNT_SYSTEM, NOT_DECLARED_CACHE));

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(NOT_DECLARED_CACHE));
    }

    /**
     * @param login Login.
     * @param cacheName Cache name.
     */
    private String restGetOrCreate(String login, String cacheName) throws Exception {
        return content(login, cacheName, GridRestCommand.GET_OR_CREATE_CACHE);
    }

    /** */
    @Test
    public void testDestroyCacheWithAdminPerms() throws Exception {
        assertNotNull(grid(0).getOrCreateCache(CACHE_NAME));
        assertNotNull(grid(0).getOrCreateCache(NOT_DECLARED_CACHE));

        checkSucces(restDestroy(ADMIN, CACHE_NAME));
        checkSucces(restDestroy(ADMIN, NOT_DECLARED_CACHE));

        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(NOT_DECLARED_CACHE));
    }

    /** */
    @Test
    public void testDestroyCacheWithCachePermissions() throws Exception {
        assertNotNull(grid(0).getOrCreateCache(CACHE_NAME));
        assertNotNull(grid(0).getOrCreateCache(NOT_DECLARED_CACHE));

        checkFailWithError(restDestroy(UNALLOWED_CLNT_CACHE, CACHE_NAME), CACHE_DESTROY);
        checkFailWithError(restDestroy(UNALLOWED_CLNT_CACHE, NOT_DECLARED_CACHE), CACHE_DESTROY);

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(NOT_DECLARED_CACHE));

        checkSucces(restDestroy(ALLOWED_CLNT_CACHE, CACHE_NAME));

        checkFailWithError(restDestroy(ALLOWED_CLNT_CACHE, NOT_DECLARED_CACHE), CACHE_DESTROY);

        assertNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(NOT_DECLARED_CACHE));
    }

    /** */
    @Test
    public void testDestroyCacheWithSystemPermissions() throws Exception {
        assertNotNull(grid(0).getOrCreateCache(CACHE_NAME));
        assertNotNull(grid(0).getOrCreateCache(NOT_DECLARED_CACHE));

        checkFailWithError(restDestroy(UNALLOWED_CLNT_SYSTEM, CACHE_NAME), CACHE_DESTROY);
        checkFailWithError(restDestroy(UNALLOWED_CLNT_SYSTEM, NOT_DECLARED_CACHE), CACHE_DESTROY);

        assertNotNull(grid(0).cache(CACHE_NAME));
        assertNotNull(grid(0).cache(NOT_DECLARED_CACHE));

        checkSucces(restDestroy(ALLOWED_CLNT_SYSTEM, CACHE_NAME));
        checkSucces(restDestroy(ALLOWED_CLNT_SYSTEM, NOT_DECLARED_CACHE));

        assertNull(grid(0).cache(CACHE_NAME));
        assertNull(grid(0).cache(NOT_DECLARED_CACHE));
    }

    /**
     * @param login Login.
     * @param cacheName Cache name.
     */
    private String restDestroy(String login, String cacheName) throws Exception {
        return content(login, cacheName, GridRestCommand.DESTROY_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected String signature() throws Exception {
        return null;
    }

    /**
     * @param login Login.
     * @param cacheName Cache name.
     * @param cmd Command.
     * @param params Params.
     */
    protected String content(String login, String cacheName, GridRestCommand cmd, String... params) throws Exception {
        Map<String, String> paramsMap = new LinkedHashMap<>();

        if (cacheName != null)
            paramsMap.put("cacheName", cacheName);

        paramsMap.put("cmd", cmd.key());

        if (params != null) {
            assertEquals(0, params.length % 2);

            for (int i = 0; i < params.length; i += 2)
                paramsMap.put(params[i], params[i + 1]);
        }

        return content(login, paramsMap);
    }


    /**
     * Execute REST command and return result.
     *
     * @param login Login.
     * @param params Params.
     */
    protected String content(String login, Map<String, String> params) throws Exception {
        SB sb = new SB(restUrl());
        sb.a("ignite.login=").a(login).a("&ignite.password=&");

        for (Map.Entry<String, String> e : params.entrySet())
            sb.a(e.getKey()).a('=').a(e.getValue()).a('&');

        URL url = new URL(sb.toString());

        URLConnection conn = url.openConnection();

        InputStream in = conn.getInputStream();

        StringBuilder buf = new StringBuilder(256);

        try (LineNumberReader rdr = new LineNumberReader(new InputStreamReader(in, "UTF-8"))) {
            for (String line = rdr.readLine(); line != null; line = rdr.readLine())
                buf.append(line);
        }

        return buf.toString();
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * @param json JSON content.
     * @param perm Missing permission.
     * @throws IOException If failed.
     */
    private void checkFailWithError(String json, SecurityPermission perm) throws IOException {
        assertEquals(ERROR_STATUS, jsonField(json, STATUS));

        assertTrue(jsonField(json, "error").contains("Authorization failed [perm=" + perm));
    }

    /**
     * @param json JSON content.
     * @throws IOException If failed.
     */
    private void checkSucces(String json) throws IOException {
        assertEquals(SUCCESS_STATUS, jsonField(json, STATUS));
    }
}
