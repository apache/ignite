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

package org.apache.ignite.internal.managers.security.os;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.*;
import org.apache.ignite.internal.managers.security.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.spi.authentication.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;

/**
 * No-op implementation for {@link GridSecurityProcessor}.
 */
public class GridOsSecurityProcessor extends GridNoopManagerAdapter implements GridSecurityProcessor {
    /**
     * @param ctx Kernal context.
     */
    public GridOsSecurityProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** Allow all permissions. */
    private static final GridSecurityPermissionSet ALLOW_ALL = new GridAllowAllPermissionSet();

    /** {@inheritDoc} */
    @Override public GridSecurityContext authenticateNode(ClusterNode node, GridSecurityCredentials cred)
        throws IgniteCheckedException {
        GridSecuritySubjectAdapter s = new GridSecuritySubjectAdapter(GridSecuritySubjectType.REMOTE_NODE, node.id());

        s.address(new InetSocketAddress(F.first(node.addresses()), 0));

        s.permissions(ALLOW_ALL);

        return new GridSecurityContext(s);
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridSecurityContext authenticate(AuthenticationContext authCtx) throws IgniteCheckedException {
        GridSecuritySubjectAdapter s = new GridSecuritySubjectAdapter(authCtx.subjectType(), authCtx.subjectId());

        s.permissions(ALLOW_ALL);
        s.address(authCtx.address());

        if (authCtx.credentials() != null)
            s.login(authCtx.credentials().getLogin());

        return new GridSecurityContext(s);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridSecuritySubject> authenticatedSubjects() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public GridSecuritySubject authenticatedSubject(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, GridSecurityPermission perm, @Nullable GridSecurityContext securityCtx)
        throws GridSecurityException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return false;
    }
}
