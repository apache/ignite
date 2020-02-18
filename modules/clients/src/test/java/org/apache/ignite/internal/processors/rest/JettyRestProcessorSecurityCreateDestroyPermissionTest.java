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
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/** */
public class JettyRestProcessorSecurityCreateDestroyPermissionTest extends JettyRestProcessorCommonSelfTest {
    /** Admin client. */
    private static final String ADMIN = "admin";

    /** Allowed client. */
    private static final String ALLOWED_CLNT = "allowed_clnt";

    /** Unallowed client. */
    private static final String UNALLOWED_CLNT = "unallowed_clnt";

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        TestSecurityData[] clientData = new TestSecurityData[] {
            new TestSecurityData(ADMIN,
                SecurityPermissionSetBuilder.create()
                    .build()
            ),
            new TestSecurityData(ALLOWED_CLNT,
                SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                    .appendSystemPermissions(ADMIN_CACHE)
                    .build()
            ),
            new TestSecurityData(UNALLOWED_CLNT,
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

    /** */
    @Test
    public void testGetOrCreate() throws Exception {
        checkFailWithError(restGetOrCreate(UNALLOWED_CLNT, CACHE_NAME), ADMIN_CACHE);

        assertNull(grid(0).cache(CACHE_NAME));

        checkSucces(restGetOrCreate(ALLOWED_CLNT, CACHE_NAME));

        assertNotNull(grid(0).cache(CACHE_NAME));
    }

    /**
     * @param cacheName Cache name.
     */
    private String restGetOrCreate(String login, String cacheName) throws Exception {
        return content(login, cacheName, GridRestCommand.GET_OR_CREATE_CACHE);
    }

    /** */
    @Test
    public void testDestroyCache() throws Exception {
        restGetOrCreate(ADMIN, CACHE_NAME);

        checkFailWithError(restDestroy(UNALLOWED_CLNT, CACHE_NAME), ADMIN_CACHE);

        assertNotNull(grid(0).cache(CACHE_NAME));

        checkSucces(restDestroy(ALLOWED_CLNT, CACHE_NAME));

        assertNull(grid(0).cache(CACHE_NAME));
    }

    /**
     * @param cacheName Cache name.
     */
    private String restDestroy(String login, String cacheName) throws Exception {
        return content(login, cacheName, GridRestCommand.DESTROY_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected String signature() throws Exception {
        return null;
    }

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

        return content(paramsMap,login);
    }


    /**
     * Execute REST command and return result.
     *
     * @param params Params.
     * @param login Login.
     */
    protected String content(Map<String, String> params, String login) throws Exception {
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
