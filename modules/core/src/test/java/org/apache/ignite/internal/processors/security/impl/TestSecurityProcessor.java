/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.security.impl;

import java.net.InetSocketAddress;
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
    private static final Map<SecurityCredentials, SecurityPermissionSet> PERMS = new ConcurrentHashMap<>();

    /** Node security data. */
    private final TestSecurityData nodeSecData;

    /** Users security data. */
    private final Collection<TestSecurityData> predefinedAuthData;

    /**
     * Constructor.
     */
    public TestSecurityProcessor(GridKernalContext ctx, TestSecurityData nodeSecData,
        Collection<TestSecurityData> predefinedAuthData) {
        super(ctx);

        this.nodeSecData = nodeSecData;
        this.predefinedAuthData = predefinedAuthData.isEmpty()
            ? Collections.emptyList()
            : new ArrayList<>(predefinedAuthData);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
        return new TestSecurityContext(
            new TestSecuritySubject()
                .setType(REMOTE_NODE)
                .setId(node.id())
                .setAddr(new InetSocketAddress(F.first(node.addresses()), 0))
                .setLogin(cred.getLogin())
                .setPerms(PERMS.get(cred))
        );
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext ctx) {
        return new TestSecurityContext(
            new TestSecuritySubject()
                .setType(ctx.subjectType())
                .setId(ctx.subjectId())
                .setAddr(ctx.address())
                .setLogin(ctx.credentials().getLogin())
                .setPerms(PERMS.get(ctx.credentials()))
        );
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

        ctx.addNodeAttribute(IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS, nodeSecData.credentials());

        for (TestSecurityData data : predefinedAuthData)
            PERMS.put(data.credentials(), data.getPermissions());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        PERMS.remove(nodeSecData.credentials());

        for (TestSecurityData data : predefinedAuthData)
            PERMS.remove(data.credentials());
    }
}
