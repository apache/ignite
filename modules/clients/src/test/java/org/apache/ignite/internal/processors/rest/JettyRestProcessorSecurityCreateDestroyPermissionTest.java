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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.configuration.WALMode.NONE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;

/** */
public class JettyRestProcessorSecurityCreateDestroyPermissionTest extends JettyRestProcessorCommonSelfTest {
    /** Default user. */
    protected static final String DFLT_USER = "ignite";

    /** Default password. */
    protected static final String DFLT_PWD = "ignite";

    /** Status. */
    protected static final String STATUS = "successStatus";

    /** Success status. */
    protected static final String SUCCESS_STATUS = "0";

    /** Error status. */
    protected static final String ERROR_STATUS = "1";

    /** Empty permission. */
    protected static final SecurityPermission[] EMPTY_PERM = new SecurityPermission[0];

    /** Cache name for tests. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** Create cache name. */
    protected static final String CREATE_CACHE_NAME = "CREATE_TEST_CACHE";

    /** Forbidden cache. */
    protected static final String FORBIDDEN_CACHE_NAME = "FORBIDDEN_TEST_CACHE";

    /** New cache. */
    protected static final String NEW_TEST_CACHE = "NEW_TEST_CACHE";

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        grid(0).cluster().active(true);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalMode(NONE);

        DataRegionConfiguration testDataRegionCfg = new DataRegionConfiguration();
        testDataRegionCfg.setPersistenceEnabled(true);

        dsCfg.setDefaultDataRegionConfiguration(testDataRegionCfg);

        PluginProvider pluginProvider = new TestSecurityPluginProvider(DFLT_USER, DFLT_PWD,
            SecurityPermissionSetBuilder.create()
                .defaultAllowAll(true)
                .appendCachePermissions(CACHE_NAME, CACHE_CREATE, CACHE_DESTROY)
                .appendCachePermissions(CREATE_CACHE_NAME, CACHE_CREATE)
                .appendCachePermissions(FORBIDDEN_CACHE_NAME, EMPTY_PERM)
                .appendSystemPermissions(JOIN_AS_SERVER, ADMIN_CACHE, ADMIN_OPS)
                .build(), true);

        cfg.setDataStorageConfiguration(dsCfg);
        cfg.setAuthenticationEnabled(true);
        cfg.setPluginProviders(pluginProvider);
        cfg.setCacheConfiguration(null);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetOrCreateDestroyCache() throws Exception {
        // This won't fail since defaultAllowAll is true.
        assertEquals(SUCCESS_STATUS,
            (jsonField(content(NEW_TEST_CACHE, GridRestCommand.GET_OR_CREATE_CACHE), STATUS)));

        assertEquals(SUCCESS_STATUS,
            (jsonField(content(CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE), STATUS)));
        assertEquals(SUCCESS_STATUS,
            (jsonField(content(CREATE_CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE), STATUS)));

        checkFailWithError(content(FORBIDDEN_CACHE_NAME, GridRestCommand.GET_OR_CREATE_CACHE), CACHE_CREATE);

        // This won't fail since defaultAllowAll is true.
        assertEquals(SUCCESS_STATUS,
            (jsonField(content(NEW_TEST_CACHE, GridRestCommand.DESTROY_CACHE), STATUS)));

        assertEquals(SUCCESS_STATUS,
            (jsonField(content(CACHE_NAME, GridRestCommand.DESTROY_CACHE), STATUS)));

        checkFailWithError(content(CREATE_CACHE_NAME, GridRestCommand.DESTROY_CACHE), CACHE_DESTROY);
        checkFailWithError(content(FORBIDDEN_CACHE_NAME, GridRestCommand.DESTROY_CACHE), CACHE_DESTROY);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected String restUrl() {
        String url = super.restUrl();

        url += "ignite.login=" + DFLT_USER + "&ignite.password=" + DFLT_PWD + "&";

        return url;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected String signature() throws Exception {
        return null;
    }

    /**
     * @param json JSON content.
     * @param perm Missing permission.
     * @throws IOException If failed.
     */
    protected void checkFailWithError(String json, SecurityPermission perm) throws IOException {
        assertEquals(ERROR_STATUS, jsonField(json, STATUS));
        assertTrue(jsonField(json, "error").contains("err=Authorization failed [perm=" + perm));
    }
}
