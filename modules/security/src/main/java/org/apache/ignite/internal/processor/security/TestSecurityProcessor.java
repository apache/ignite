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

package org.apache.ignite.internal.processor.security;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * Security processor for tests.
 */
public class TestSecurityProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /** Consumer for {@link #authorize(String, SecurityPermission, SecurityContext)} method. */
    private TriConsumer<String, SecurityPermission, SecurityContext> authorize;

    /**
     * @param ctx Grid kernal context.
     */
    public TestSecurityProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Setup consumer for {@link #authorize(String, SecurityPermission, SecurityContext)} method.
     * @param authorize Authorize.
     */
    public void authorizeConsumer(TriConsumer<String, SecurityPermission, SecurityContext> authorize){
        this.authorize = authorize;
    }

    /**
     * Remove all consumers.
     */
    public void clear() {
        authorize = null;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
        return new TestSecurityContext(
            new TestSecuritySubject(
                node.id(),
                node.consistentId(),
                null,
                SecurityPermissionSetBuilder.create().build()
            )
        );
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx)
        throws SecurityException {
        if(authorize != null)
            authorize.accept(name, perm, securityCtx);
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {

    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, new SecurityCredentials());
    }
}
