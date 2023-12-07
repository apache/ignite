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

import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestTaskRequest;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/**
 * Tests REST processor configuration via Ignite plugins functionality.
 */
public class RestProcessorInitializationTest extends AbstractSecurityTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testDefaultRestProcessorInitialization() throws Exception {
        IgniteEx ignite = startGrid(0);

        assertEquals(ignite.context().rest().getClass(), GridRestProcessor.class);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCustomRestProcessorInitialization() throws Exception {
        IgniteEx ignite = startGrid(configuration(0));

        assertEquals(ignite.context().rest().getClass(), TestGridRestProcessorImpl.class);

        TestGridRestProcessorImpl rest = (TestGridRestProcessorImpl)ignite.context().rest();

        GridRestTaskRequest req = new GridRestTaskRequest();

        req.credentials(new SecurityCredentials("client", ""));
        req.command(GridRestCommand.NOOP);

        GridRestResponse res = rest.handleAsync0(req).get();

        assertEquals(STATUS_SUCCESS, res.getSuccessStatus());
        assertEquals(req.clientId(), res.getSecuritySubjectId());

        IgniteBiTuple<GridRestRequest, IgniteInternalFuture<GridRestResponse>> entry = rest.getTuple();

        assertEquals(req, entry.get1());
        assertEquals(res, entry.get2().get());
    }

    /**
     * Test implementation of {@link PluginProvider} for obtaining {@link TestGridRestProcessorImpl}.
     */
    private static class TestRestProcessorProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TEST_REST_PROCESSOR";
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
            if (cls.equals(IgniteRestProcessor.class))
                return new TestGridRestProcessorImpl(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /**
     * Test no-op implementation of {@link IgniteRestProcessor}.
     */
    private static class TestGridRestProcessorImpl extends GridRestProcessor {
        /** */
        private final IgniteBiTuple<GridRestRequest, IgniteInternalFuture<GridRestResponse>> tuple = new IgniteBiTuple<>();

        /**
         * @param ctx Kernal context.
         */
        protected TestGridRestProcessorImpl(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override protected IgniteInternalFuture<GridRestResponse> handleAsync0(GridRestRequest req) {
            IgniteInternalFuture<GridRestResponse> fut = super.handleAsync0(req);

            fut.listen(() -> tuple.set(req, fut));

            return fut;
        }

        /** */
        public IgniteBiTuple<GridRestRequest, IgniteInternalFuture<GridRestResponse>> getTuple() {
            return tuple;
        }
    }

    /** */
    private IgniteConfiguration configuration(int idx) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(
            login,
            new TestSecurityPluginProvider(
                login,
                "",
                systemPermissions(JOIN_AS_SERVER),
                null,
                false,
                new TestSecurityData("client", ALL_PERMISSIONS)));

        return cfg
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setPluginProviders(F.concat(cfg.getPluginProviders(), new TestRestProcessorProvider()));
    }
}
