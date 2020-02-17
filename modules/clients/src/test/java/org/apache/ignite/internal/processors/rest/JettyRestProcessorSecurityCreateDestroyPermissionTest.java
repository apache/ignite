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
import java.util.Arrays;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;

/** */
public class JettyRestProcessorSecurityCreateDestroyPermissionTest extends JettyRestProcessorCommonSelfTest {
    /** Default user. */
    private static final String DFLT_USER = "ignite";

    /** Default password. */
    private static final String DFLT_PWD = "ignite";

    /** Status. */
    private static final String STATUS = "successStatus";

    /** Success status. */
    private static final String SUCCESS_STATUS = "0";

    /** Error status. */
    private static final String ERROR_STATUS = "1";

    /** Empty permission. */
    private static final SecurityPermission[] EMPTY_PERM = new SecurityPermission[0];

    /** Cache name for tests. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** Create cache name. */
    private static final String NOT_DELETE_CACHE_NAME = "NOT_DELETE_CACHE_NAME";

    /** Forbidden cache. */
    private static final String NOT_CREATE_CACHE_NAME = "NOT_CREATE_CACHE_NAME";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        PluginProvider pluginProvider = new TestSecurityPluginProvider(DFLT_USER, DFLT_PWD,
            SecurityPermissionSetBuilder.create()
                .defaultAllowAll(false)
                .appendCachePermissions(CACHE_NAME, CACHE_CREATE, CACHE_DESTROY)
                .appendCachePermissions(NOT_DELETE_CACHE_NAME, CACHE_CREATE)
                .appendCachePermissions(NOT_CREATE_CACHE_NAME, EMPTY_PERM)
                .appendSystemPermissions(JOIN_AS_SERVER, ADMIN_CACHE, ADMIN_OPS)
                .build(), true);

        cfg.setPluginProviders(pluginProvider);
        cfg.setCacheConfiguration(null);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetOrCreate() throws Exception {
        getOrCreateCaches();

        assertTrue(Arrays.asList(CACHE_NAME, NOT_DELETE_CACHE_NAME).containsAll(grid(0).cacheNames()));

        assertFalse(grid(0).cacheNames().contains(NOT_CREATE_CACHE_NAME));
    }

    /** */
    private void getOrCreateCaches() throws Exception {
        assertEquals(SUCCESS_STATUS, jsonField(content(CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE), STATUS));

        assertEquals(SUCCESS_STATUS,
            jsonField(content(NOT_DELETE_CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE), STATUS));

        checkFailWithError(content(NOT_CREATE_CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE), CACHE_CREATE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDestroyCache() throws Exception {
        getOrCreateCaches();

        assertTrue(Arrays.asList(CACHE_NAME, NOT_DELETE_CACHE_NAME).containsAll(grid(0).cacheNames()));

        assertEquals(SUCCESS_STATUS, jsonField(content(CACHE_NAME, GridRestCommand.DESTROY_CACHE), STATUS));

        checkFailWithError(content(NOT_DELETE_CACHE_NAME, GridRestCommand.DESTROY_CACHE), CACHE_DESTROY);

        assertFalse(grid(0).cacheNames().contains(CACHE_NAME));

        assertTrue(grid(0).cacheNames().contains(NOT_DELETE_CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected String restUrl() {
        String url = super.restUrl();

        url += "ignite.login=" + DFLT_USER + "&ignite.password=" + DFLT_PWD + "&";

        return url;
    }

    /** {@inheritDoc} */
    @Override protected String signature() throws Exception {
        return null;
    }

    /**
     * @param json JSON content.
     * @param perm Missing permission.
     * @throws IOException If failed.
     */
    private void checkFailWithError(String json, SecurityPermission perm) throws IOException {
        assertEquals(ERROR_STATUS, jsonField(json, STATUS));

        assertTrue(jsonField(json, "error").contains("err=Authorization failed [perm=" + perm));
    }
}
