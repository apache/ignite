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
import java.security.Permissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.client.CommonSecurityCheckTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheGetRemoveSkipStoreTest.TEST_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Tests REST processor authorization commands GET_OR_CREATE_CACHE / DESTROY_CACHE.
 */
public class RestProcessorAuthorizationTest extends CommonSecurityCheckTest {
    /** */
    private static final String LOGIN = "login";

    /** */
    private static final String PWD = "pwd";

    /** */
    private final List<GridTuple3<String, SecurityPermission, SecurityContext>> authorizationCtxList = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected PluginProvider<?> getPluginProvider(String name) {
        return new TestSecurityPluginProvider(
            name,
            null,
            ALLOW_ALL,
            globalAuth,
            clientData()
        ) {
            /** {@inheritDoc} */
            @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
                return new TestSecurityProcessor(
                    ctx,
                    new TestSecurityData(login, pwd, perms, new Permissions()),
                    Arrays.asList(clientData),
                    globalAuth
                ) {
                    /** {@inheritDoc} */
                    @Override public void authorize(
                        String name,
                        SecurityPermission perm,
                        SecurityContext securityCtx
                    ) throws SecurityException {
                        if (perm.name().startsWith("CACHE_"))
                            authorizationCtxList.add(F.t(name, perm, securityCtx));

                        super.authorize(name, perm, securityCtx);
                    }
                };
            }
        };
    }

    /** @throws Exception if failed. */
    @Test
    public void testCacheCreateDestroyPermission() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        assertNull(ignite.cache(TEST_CACHE));

        executeCommand(GridRestCommand.GET_OR_CREATE_CACHE, LOGIN, PWD);

        GridTuple3<String, SecurityPermission, SecurityContext> ctx = authorizationCtxList.get(0);

        assertEquals(TEST_CACHE, ctx.get1());
        assertEquals(SecurityPermission.CACHE_CREATE, ctx.get2());
        assertEquals(LOGIN, ctx.get3().subject().login());

        assertNotNull(ignite.cache(TEST_CACHE));

        authorizationCtxList.clear();

        executeCommand(GridRestCommand.DESTROY_CACHE, LOGIN, PWD);

        ctx = authorizationCtxList.get(0);

        assertEquals(TEST_CACHE, ctx.get1());
        assertEquals(SecurityPermission.CACHE_DESTROY, ctx.get2());
        assertEquals(LOGIN, ctx.get3().subject().login());

        assertNull(ignite.cache(TEST_CACHE));
    }

    /** */
    private void executeCommand(GridRestCommand cmd, String login, String pwd) throws IOException {
        String addr = "http://localhost:8080/ignite?cmd=" + cmd.key()
            + "&cacheName=" + TEST_CACHE
            + "&ignite.login=" + login + "&ignite.password=" + pwd;

        URL url = new URL(addr);

        URLConnection conn = url.openConnection();

        conn.connect();

        assertEquals(200, ((HttpURLConnection)conn).getResponseCode());
    }
}
