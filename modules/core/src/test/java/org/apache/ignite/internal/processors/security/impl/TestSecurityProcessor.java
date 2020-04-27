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

import java.net.InetSocketAddress;
import java.security.Permissions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityBasicPermissionSet;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecuritySubject;

import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_NODE;

/**
 * Security processor for test.
 */
public class TestSecurityProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /** Permissions. */
    public static final Map<SecurityCredentials, SecurityPermissionSet> PERMS = new ConcurrentHashMap<>();

    /** Sandbox permissions. */
    private static final Map<SecurityCredentials, Permissions> SANDBOX_PERMS = new ConcurrentHashMap<>();

    /** */
    private static final Map<UUID, SecurityContext> SECURITY_CONTEXTS = new ConcurrentHashMap<>();

    /** Node security data. */
    private final TestSecurityData nodeSecData;

    /** Users security data. */
    private final Collection<TestSecurityData> predefinedAuthData;

    /** Global authentication. */
    private final boolean globalAuth;

    /**
     * Constructor.
     */
    public TestSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
        Collection<TestSecurityData> predefinedAuthData, boolean globalAuth) {
        super(ctx);

        this.nodeSecData = nodeSecData;
        this.predefinedAuthData = predefinedAuthData.isEmpty()
            ? Collections.emptyList()
            : new ArrayList<>(predefinedAuthData);
        this.globalAuth = globalAuth;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred)
        throws IgniteCheckedException {
        if (!PERMS.containsKey(cred))
            return null;

        return new TestSecurityContext(
            new TestSecuritySubject()
                .setType(REMOTE_NODE)
                .setId(node.id())
                .setAddr(new InetSocketAddress(F.first(node.addresses()), 0))
                .setLogin(cred.getLogin())
                .setPerms(PERMS.get(cred))
                .sandboxPermissions(SANDBOX_PERMS.get(cred))
        );
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return globalAuth;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException {
        if (ctx.credentials() == null || ctx.credentials().getLogin() == null)
            return null;

        SecurityPermissionSet perms = PERMS.get(ctx.credentials());

        if (perms == null) {
            perms = new SecurityBasicPermissionSet();
            ((SecurityBasicPermissionSet) perms).setDefaultAllowAll(true);
        }

        SecurityContext res = new TestSecurityContext(
            new TestSecuritySubject()
                .setType(ctx.subjectType())
                .setId(ctx.subjectId())
                .setAddr(ctx.address())
                .setLogin(ctx.credentials().getLogin())
                .setPerms(perms)
                .setCerts(ctx.certificates())
                .sandboxPermissions(SANDBOX_PERMS.get(ctx.credentials()))
        );

        SECURITY_CONTEXTS.put(res.subject().id(), res);

        return res;
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
    @Override public SecurityContext securityContext(UUID subjId) {
        return SECURITY_CONTEXTS.get(subjId);
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx)
        throws SecurityException {
        if (!((TestSecurityContext)securityCtx).operationAllowed(name, perm))
            throw new SecurityException("Authorization failed [perm=" + perm +
                ", name=" + name +
                ", subject=" + securityCtx.subject() + ']');
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        PERMS.put(nodeSecData.credentials(), nodeSecData.getPermissions());
        SANDBOX_PERMS.put(nodeSecData.credentials(), nodeSecData.sandboxPermissions());

        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, nodeSecData.credentials());

        for (TestSecurityData data : predefinedAuthData) {
            PERMS.put(data.credentials(), data.getPermissions());
            SANDBOX_PERMS.put(nodeSecData.credentials(), data.sandboxPermissions());
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        PERMS.remove(nodeSecData.credentials());
        SANDBOX_PERMS.remove(nodeSecData.credentials());

        for (TestSecurityData data : predefinedAuthData) {
            PERMS.remove(data.credentials());
            SANDBOX_PERMS.remove(data.credentials());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean sandboxEnabled() {
        return true;
    }
}
