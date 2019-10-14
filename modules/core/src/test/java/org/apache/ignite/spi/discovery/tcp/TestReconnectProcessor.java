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

package org.apache.ignite.spi.discovery.tcp;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.plugin.security.SecuritySubjectType;
import org.jetbrains.annotations.Nullable;

/**
 * Updates node attributes on disconnect.
 */
public class TestReconnectProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /**
     * @param ctx Kernal context.
     */
    public TestReconnectProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, new SecurityCredentials());
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node,
        SecurityCredentials cred) throws IgniteCheckedException {
        return new TestSecurityContext(new TestSecuritySubject(ctx.localNodeId()));
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm,
        @Nullable SecurityContext securityCtx) throws SecurityException {

    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {

    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        ctx.addNodeAttribute("test", "2");
    }

    /**
     *
     */
    private static class TestSecuritySubject implements SecuritySubject {

        /** Id. */
        private final UUID id;

        /**
         * @param id Id.
         */
        public TestSecuritySubject(UUID id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public UUID id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public SecuritySubjectType type() {
            return SecuritySubjectType.REMOTE_NODE;
        }

        /** {@inheritDoc} */
        @Override public Object login() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public InetSocketAddress address() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet permissions() {
            return null;
        }
    }

    /**
     *
     */
    private static class TestSecurityContext implements SecurityContext, Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Subj. */
        final SecuritySubject subj;

        /**
         * @param subj Subj.
         */
        public TestSecurityContext(SecuritySubject subj) {
            this.subj = subj;
        }

        /** {@inheritDoc} */
        @Override public SecuritySubject subject() {
            return subj;
        }

        /** {@inheritDoc} */
        @Override public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean systemOperationAllowed(SecurityPermission perm) {
            return true;
        }
    }
}
