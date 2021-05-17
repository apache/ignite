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
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.security.client.CommonSecurityCheckTest;
import org.apache.ignite.internal.processors.security.impl.TestAuthorizationContextSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestAuthorizationContextSecurityPluginProvider.TestAuthorizationContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheGetRemoveSkipStoreTest.TEST_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Tests REST processor configuration via Ignite plugins functionality.
 */
public class RestProcessorAuthorizationTest extends CommonSecurityCheckTest {
    /** */
    private final List<TestAuthorizationContext> hndlr = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected PluginProvider<?> getPluginProvider(String name) {
        return new TestAuthorizationContextSecurityPluginProvider(name, null, ALLOW_ALL,
            globalAuth, hndlr::add, clientData());
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void cacheCreateDestroyPermissionTest() throws Exception {
        String login = "login";
        String pwd = "pwd";

        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        assertEquals(ignite.context().rest().getClass(), GridRestProcessor.class);

        assertNull(ignite.cache(TEST_CACHE));

        executeCommand(GridRestCommand.GET_OR_CREATE_CACHE, login, pwd);

        TestAuthorizationContext ctx = hndlr.get(0);

        assertEquals(TEST_CACHE, ctx.getName());
        assertEquals(SecurityPermission.CACHE_CREATE, ctx.getPerm());
        assertEquals(login, ctx.getSecurityCtx().subject().login());

        assertNotNull(ignite.cache(TEST_CACHE));

        hndlr.clear();

        executeCommand(GridRestCommand.DESTROY_CACHE, login, pwd);

        ctx = hndlr.get(0);

        assertEquals(TEST_CACHE, ctx.getName());
        assertEquals(SecurityPermission.CACHE_DESTROY, ctx.getPerm());
        assertEquals(login, ctx.getSecurityCtx().subject().login());

        assertNull(ignite.cache(TEST_CACHE));
    }

    /** */
    private void executeCommand(GridRestCommand cmd, String login, String pwd) throws IOException {
        String addr = "http://127.0.0.1:8080/ignite?cmd=" + cmd.key()
            + "&cacheName=" + TEST_CACHE
            + "&ignite.login=" + login + "&ignite.password=" + pwd;

        URL url = new URL(addr);

        URLConnection conn = url.openConnection();

        conn.connect();

        assertEquals(200, ((HttpURLConnection)conn).getResponseCode());
    }
}
