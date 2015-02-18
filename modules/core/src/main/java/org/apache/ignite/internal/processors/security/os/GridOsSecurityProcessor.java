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
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.security.*;
import org.jetbrains.annotations.*;

import java.net.*;
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

    /** Allow all permissions. */
    private static final GridSecurityPermissionSet ALLOW_ALL = new GridSecurityPermissionSet() {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean defaultAllowAll() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<GridSecurityPermission>> taskPermissions() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Map<String, Collection<GridSecurityPermission>> cachePermissions() {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Nullable @Override public Collection<GridSecurityPermission> systemPermissions() {
            return null;
        }
    };

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node, GridSecurityCredentials cred)
        throws IgniteCheckedException {
        GridSecuritySubjectAdapter s = new GridSecuritySubjectAdapter(GridSecuritySubjectType.REMOTE_NODE, node.id());

        s.address(new InetSocketAddress(F.first(node.addresses()), 0));

        s.permissions(ALLOW_ALL);

        return new SecurityContextImpl(s);
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext authCtx) throws IgniteCheckedException {
        GridSecuritySubjectAdapter s = new GridSecuritySubjectAdapter(authCtx.subjectType(), authCtx.subjectId());

        s.permissions(ALLOW_ALL);
        s.address(authCtx.address());

        if (authCtx.credentials() != null)
            s.login(authCtx.credentials().getLogin());

        return new SecurityContextImpl(s);
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
    @Override public void authorize(String name, GridSecurityPermission perm, @Nullable SecurityContext securityCtx)
        throws GridSecurityException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public SecurityContext createSecurityContext(GridSecuritySubject subj) {
        return new SecurityContextImpl(subj);
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return false;
    }

     /**
     * Authenticated security subject.
     */
     private class GridSecuritySubjectAdapter implements GridSecuritySubject {
        /** */
        private static final long serialVersionUID = 0L;

        /** Subject ID. */
        private UUID id;

        /** Subject type. */
        private GridSecuritySubjectType subjType;

        /** Address. */
        private InetSocketAddress addr;

        /** Permissions assigned to a subject. */
        private GridSecurityPermissionSet permissions;

        /** Login. */
        @GridToStringInclude
        private Object login;

        /**
         * @param subjType Subject type.
         * @param id Subject ID.
         */
        public GridSecuritySubjectAdapter(GridSecuritySubjectType subjType, UUID id) {
            this.subjType = subjType;
            this.id = id;
        }

        /**
         * @return Subject ID.
         */
        @Override public UUID id() {
            return id;
        }

        /**
         * @return Subject type.
         */
        @Override public GridSecuritySubjectType type() {
            return subjType;
        }

        /**
         * @return Subject address.
         */
        @Override public InetSocketAddress address() {
            return addr;
        }

        /**
         * @param addr Subject address.
         */
        public void address(InetSocketAddress addr) {
            this.addr = addr;
        }

        /**
         * @return Security permissions.
         */
        @Override public GridSecurityPermissionSet permissions() {
            return permissions;
        }

        /** {@inheritDoc} */
        @Override public Object login() {
            return login;
        }

        /**
         * @param login Login.
         */
        public void login(Object login) {
            this.login = login;
        }

        /**
         * @param permissions Permissions.
         */
        public void permissions(GridSecurityPermissionSet permissions) {
            this.permissions = permissions;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(GridSecuritySubjectAdapter.class, this);
        }
    }
}
