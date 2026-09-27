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

import java.security.Permissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.client.CommonSecurityCheckTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestSecurityProcessor;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.junit.Test;


import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.CacheGetRemoveSkipStoreTest.TEST_CACHE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_ACTIVATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLUSTER_SET_STATE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.DESTROY_CACHE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.GET_OR_CREATE_CACHE;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.RestSetupSimpleTest.execute;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

/**
 * Tests REST processor authorization commands GET_OR_CREATE_CACHE / DESTROY_CACHE.
 */
public class RestProcessorAuthorizationTest extends CommonSecurityCheckTest {
    /** */
    private static final String LOGIN = "login";

    /** */
    private static final String LOGIN_NO_PERMISSIONS = "login_no_permissions";

    /** */
    private static final String PWD = "pwd";

    /** */
    private final List<GridTuple3<String, SecurityPermission, SecurityContext>> authorizationCtxList = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected PluginProvider<?> getPluginProvider(String name) {
        return new TestSecurityPluginProvider(
            name,
            null,
            ALL_PERMISSIONS,
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

        ignite.cluster().state(ACTIVE);

        assertNull(ignite.cache(TEST_CACHE));

        executeCommand(LOGIN, GET_OR_CREATE_CACHE, new T2<>("cacheName", TEST_CACHE));

        GridTuple3<String, SecurityPermission, SecurityContext> ctx = authorizationCtxList.get(0);

        assertEquals(TEST_CACHE, ctx.get1());
        assertEquals(SecurityPermission.CACHE_CREATE, ctx.get2());
        assertEquals(LOGIN, ctx.get3().subject().login());

        assertNotNull(ignite.cache(TEST_CACHE));

        authorizationCtxList.clear();

        executeCommand(LOGIN, DESTROY_CACHE, new T2<>("cacheName", TEST_CACHE));

        ctx = authorizationCtxList.get(0);

        assertEquals(TEST_CACHE, ctx.get1());
        assertEquals(SecurityPermission.CACHE_DESTROY, ctx.get2());
        assertEquals(LOGIN, ctx.get3().subject().login());

        assertNull(ignite.cache(TEST_CACHE));
    }

    /** @throws Exception if failed. */
    @Test
    public void testClusterStateChange() throws Exception {
        IgniteEx ignite = startGrid(0);

        assertEquals(ClusterState.INACTIVE, ignite.cluster().state());

        GridRestResponse res = executeCommand(LOGIN_NO_PERMISSIONS, CLUSTER_SET_STATE, new T2<>("state", ACTIVE.name()));

        assertEquals(GridRestResponse.STATUS_SECURITY_CHECK_FAILED, res.getSuccessStatus());

        assertEquals(ClusterState.INACTIVE, ignite.cluster().state());

        res = executeCommand(LOGIN, CLUSTER_SET_STATE, new T2<>("state", ACTIVE.name()));

        assertEquals(GridRestResponse.STATUS_SUCCESS, res.getSuccessStatus());

        assertEquals(ACTIVE, ignite.cluster().state());
    }

    /** @throws Exception if failed. */
    @Test
    public void testOldClusterStateChange() throws Exception {
        IgniteEx ignite = startGrid(0);

        assertEquals(ClusterState.INACTIVE, ignite.cluster().state());

        GridRestResponse res = executeCommand(LOGIN_NO_PERMISSIONS, CLUSTER_ACTIVATE);

        assertEquals(GridRestResponse.STATUS_SECURITY_CHECK_FAILED, res.getSuccessStatus());

        assertEquals(ClusterState.INACTIVE, ignite.cluster().state());

        res = executeCommand(LOGIN, CLUSTER_ACTIVATE);

        assertEquals(GridRestResponse.STATUS_SUCCESS, res.getSuccessStatus());

        assertEquals(ACTIVE, ignite.cluster().state());
    }


    /** @return Session token. */
    private String authenticate(String login) throws Exception {
        String res = execute("/ignite",
            new T2<>("cmd", "authenticate"),
            new T2<>("ignite.login", login),
            new T2<>("ignite.password", PWD));

        return new ObjectMapper().readTree(res).get("sessionToken").asText();
    }

    /** */
    @SafeVarargs
    private GridRestResponse executeCommand(
        String login,
        GridRestCommand cmd,
        T2<String, String>... params
    ) throws Exception {
        T2<String, String>[] allParams = new T2[params.length + 3];

        allParams[0] = new T2<>("cmd", cmd.key());
        allParams[1] = new T2<>("ignite.login", login);
        allParams[2] = new T2<>("ignite.password", PWD);

        System.arraycopy(params, 0, allParams, 3, params.length);

        String res = execute("/ignite", allParams);

        return new ObjectMapper().readValue(res, GridRestResponse.class);
    }

    /** {@inheritDoc} */
    @Override protected TestSecurityData[] clientData() {
        return new TestSecurityData[] {new TestSecurityData(LOGIN, PWD, ALL_PERMISSIONS, new Permissions()),
            new TestSecurityData(LOGIN_NO_PERMISSIONS, PWD, NO_PERMISSIONS, new Permissions())};
    }
}
