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

package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.sandbox.IgniteSandbox;
import org.apache.ignite.internal.processors.security.sandbox.NoOpSandbox;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.security.IgniteSecurityProcessor.ATTR_GRID_SEC_PROC_CLASS;
import static org.apache.ignite.internal.processors.security.SecurityUtils.MSG_SEC_PROC_CLS_IS_INVALID;

/**
 * No operation IgniteSecurity.
 */
public class NoOpIgniteSecurityProcessor extends GridProcessorAdapter implements IgniteSecurity {
    /** No operation security context. */
    private final OperationSecurityContext opSecCtx = new OperationSecurityContext(this, null);

    /** Instance of IgniteSandbox. */
    private final IgniteSandbox sandbox = new NoOpSandbox();

    /**
     * @param ctx Grid kernal context.
     */
    public NoOpIgniteSecurityProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(SecurityContext secCtx) {
        return opSecCtx;
    }

    /** {@inheritDoc} */
    @Override public OperationSecurityContext withContext(UUID nodeId) {
        return opSecCtx;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext securityContext() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
        return null;
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
        return null;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm) throws SecurityException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteSandbox sandbox() {
        return sandbox;
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node) {
        return validateSecProcClass(node);
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteNodeValidationResult validateNode(ClusterNode node,
        DiscoveryDataBag.JoiningNodeDiscoveryData discoData) {
        return validateSecProcClass(node);
    }

    /**
     * Validates that remote the node's grid security processor class is undefined.
     *
     * @param node Joining node.
     * @return Validation result or {@code null} in case of success.
     */
    private IgniteNodeValidationResult validateSecProcClass(ClusterNode node) {
        String rmtCls = node.attribute(ATTR_GRID_SEC_PROC_CLASS);

        if (rmtCls != null) {
            ClusterNode locNode = ctx.discovery().localNode();

            return new IgniteNodeValidationResult(
                node.id(),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, locNode.id(), node.id(), "undefined", rmtCls),
                String.format(MSG_SEC_PROC_CLS_IS_INVALID, node.id(), locNode.id(), rmtCls, "undefined")
            );
        }

        return null;
    }
}
