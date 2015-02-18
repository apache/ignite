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

package org.apache.ignite.internal.processors.security.os;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.security.*;
import org.apache.ignite.plugin.security.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * No-op implementation for {@link GridSecurityProcessor}.
 */
public class GridOsSecurityProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /**
     * @param ctx Kernal context.
     */
    public GridOsSecurityProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, GridSecurityCredentials cred)
        throws IgniteCheckedException {
        throw new UnsupportedOperationException("GridOsSecurityProcessor.authenticateNode()");
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        throw new UnsupportedOperationException("GridOsSecurityProcessor.isGlobalNodeAuthentication()");
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext authCtx) throws IgniteCheckedException {
        throw new UnsupportedOperationException("GridOsSecurityProcessor.authenticate()");
    }

    /** {@inheritDoc} */
    @Override public Collection<GridSecuritySubject> authenticatedSubjects() {
        throw new UnsupportedOperationException("GridOsSecurityProcessor.authenticatedSubjects()");
    }

    /** {@inheritDoc} */
    @Override public GridSecuritySubject authenticatedSubject(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void authorize(String name, GridSecurityPermission perm, @Nullable SecurityContext securityCtx)
        throws GridSecurityException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public SecurityContext createSecurityContext(GridSecuritySubject subj) {
        throw new UnsupportedOperationException("GridOsSecurityProcessor.createSecurityContext()");
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
