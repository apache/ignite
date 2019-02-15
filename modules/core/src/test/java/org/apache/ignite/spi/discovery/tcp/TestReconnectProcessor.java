/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery.tcp;

import java.io.Serializable;
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
import org.apache.ignite.plugin.security.SecuritySubject;
import org.jetbrains.annotations.Nullable;

/**
 * Updates node attributes on disconnect.
 */
public class TestReconnectProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /** Enabled flag. */
    public static boolean enabled;

    /**
     * @param ctx Kernal context.
     */
    protected TestReconnectProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, new SecurityCredentials());
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node,
        SecurityCredentials cred) throws IgniteCheckedException {
        return new TestSecurityContext();
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
        return enabled;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        ctx.addNodeAttribute("test", "2");
    }

    /**
     *
     */
    private static class TestSecurityContext implements SecurityContext, Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public SecuritySubject subject() {
            return null;
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
