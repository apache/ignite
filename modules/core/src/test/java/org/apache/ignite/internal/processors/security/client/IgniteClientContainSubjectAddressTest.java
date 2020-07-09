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

package org.apache.ignite.internal.processors.security.client;

import java.security.Permissions;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityPluginProvider;
import org.apache.ignite.internal.processors.security.impl.TestAdditionalSecurityProcessor;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Test AuthenticationContext contain subject address when subject is IgniteClient.
 */
public class IgniteClientContainSubjectAddressTest extends CommonSecurityCheckTest {
    /** */
    private boolean containsAddr = false;

    /** */
    @Test
    public void testAuthenticate() throws Exception {
        startGrid();

        try (IgniteClient client = Ignition.startClient(getClientConfiguration())) {
            client.cluster().state(ACTIVE);
        }

        Assert.assertTrue(containsAddr);
    }

    /** {@inheritDoc} */
    @Override protected PluginProvider<?> getPluginProvider(String name) {
        return new TestSubjectAddressSecurityPluginProvider(name, null, ALLOW_ALL,
            globalAuth, true, clientData());
    }

    /** */
    private class TestSubjectAddressSecurityPluginProvider extends TestAdditionalSecurityPluginProvider {
        /** */
        public TestSubjectAddressSecurityPluginProvider(String login, String pwd,
            SecurityPermissionSet perms, boolean globalAuth, boolean checkAddPass,
            TestSecurityData... clientData) {
            super(login, pwd, perms, globalAuth, checkAddPass, clientData);
        }

        /** {@inheritDoc} */
        @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
            return new TestSubjectAddressSecurityProcessor(ctx,
                new TestSecurityData(login, pwd, perms, new Permissions()),
                Arrays.asList(clientData), globalAuth, checkAddPass);
        }
    }

    /** */
    private class TestSubjectAddressSecurityProcessor extends TestAdditionalSecurityProcessor {
        /** */
        public TestSubjectAddressSecurityProcessor(GridKernalContext ctx,
            TestSecurityData nodeSecData,
            Collection<TestSecurityData> predefinedAuthData, boolean globalAuth, boolean checkSslCerts) {
            super(ctx, nodeSecData, predefinedAuthData, globalAuth, checkSslCerts);
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticate(AuthenticationContext authCtx) throws IgniteCheckedException {
            SecurityContext secCtx = super.authenticate(authCtx);

            containsAddr = secCtx.subject().address() != null;

            return secCtx;
        }
    }
}
