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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 *
 */
public class TestAuthorizationContextSecurityPluginProvider extends TestSecurityPluginProvider {
    /** Authorization handler. */
    private final Consumer<TestAuthorizationContext> hndlr;

    /** */
    public TestAuthorizationContextSecurityPluginProvider(String login, String pwd, SecurityPermissionSet perms,
        boolean globalAuth, Consumer<TestAuthorizationContext> hndlr,
        TestSecurityData... clientData) {
        super(login, pwd, perms, globalAuth, clientData);

        this.hndlr = hndlr;
    }

    /** {@inheritDoc} */
    @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
        return new TestAuthorizationContextSecurityProcessor(ctx,
            new TestSecurityData(login, pwd, perms, new Permissions()),
            Arrays.asList(clientData),
            globalAuth,
            hndlr);
    }

    /**
     *
     */
    public static class TestAuthorizationContext {
        /** */
        public final String name;

        /** */
        public final SecurityPermission perm;

        /** */
        public final SecurityContext securityCtx;

        /** */
        public TestAuthorizationContext(String name, SecurityPermission perm,
            SecurityContext securityCtx) {
            this.name = name;
            this.perm = perm;
            this.securityCtx = securityCtx;
        }
    }

    /**
     * Security processor for test AuthorizationContext.
     */
    private static class TestAuthorizationContextSecurityProcessor extends TestSecurityProcessor {
        /** Authorization context handler. */
        private final Consumer<TestAuthorizationContext> hndlr;

        /** */
        public TestAuthorizationContextSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
            Collection<TestSecurityData> predefinedAuthData, boolean globalAuth,
            Consumer<TestAuthorizationContext> hndlr) {
            super(ctx, nodeSecData, predefinedAuthData, globalAuth);

            this.hndlr = hndlr;
        }

        /** {@inheritDoc} */
        @Override public void authorize(String name, SecurityPermission perm,
            SecurityContext securityCtx) throws SecurityException {
            hndlr.accept(new TestAuthorizationContext(name, perm, securityCtx));

            super.authorize(name, perm, securityCtx);
        }
    }
}
