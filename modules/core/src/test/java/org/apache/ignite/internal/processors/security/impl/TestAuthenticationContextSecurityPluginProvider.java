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

package org.apache.ignite.internal.processors.security.impl;

import java.security.Permissions;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 *
 */
public class TestAuthenticationContextSecurityPluginProvider extends TestAdditionalSecurityPluginProvider {
    /** Authentication handler. */
    private Consumer<AuthenticationContext> hndlr;

    /** */
    public TestAuthenticationContextSecurityPluginProvider(String login, String pwd, SecurityPermissionSet perms,
        boolean globalAuth, boolean checkAddPass, Consumer<AuthenticationContext> hndlr,
        TestSecurityData... clientData) {
        super(login, pwd, perms, globalAuth, checkAddPass, clientData);

        this.hndlr = hndlr;
    }

    /** {@inheritDoc} */
    @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
        return new TestAuthenticationContextSecurityProcessor(ctx,
            new TestSecurityData(login, pwd, perms, new Permissions()),
            Arrays.asList(clientData),
            globalAuth,
            checkAddPass,
            hndlr);
    }

    /**
     * Security processor for test AuthenticationContext with user attributes.
     */
    private static class TestAuthenticationContextSecurityProcessor extends TestAdditionalSecurityProcessor {
        /** Authentication context handler. */
        private Consumer<AuthenticationContext> hndlr;

        /**
         * Constructor.
         */
        public TestAuthenticationContextSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
            Collection<TestSecurityData> predefinedAuthData, boolean globalAuth, boolean checkSslCerts,
            Consumer<AuthenticationContext> hndlr) {
            super(ctx, nodeSecData, predefinedAuthData, globalAuth, checkSslCerts);

            this.hndlr = hndlr;
        }

        /** {@inheritDoc} */
        @Override public SecurityContext authenticate(AuthenticationContext authCtx) throws IgniteCheckedException {
            hndlr.accept(authCtx);

            return super.authenticate(authCtx);
        }
    }
}
