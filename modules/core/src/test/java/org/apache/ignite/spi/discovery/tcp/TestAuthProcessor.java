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
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
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
public class TestAuthProcessor extends GridProcessorAdapter implements GridSecurityProcessor {
    /** Enabled flag. */
    public static boolean enabled;
    /** admin user name */
    public String user = "user";
    /** admin user password */
    public String password = "password";

    /**
     * @param ctx Kernal context.
     */
    public TestAuthProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticateNode(ClusterNode node,
        SecurityCredentials cred) {
        return new AllowAll();
    }

    /** {@inheritDoc} */
    @Override public boolean isGlobalNodeAuthentication() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public SecurityContext authenticate(AuthenticationContext authCtx) {
        SecurityCredentials credentials = authCtx.credentials();
        if (credentials != null && user.equals(credentials.getLogin())) {
            if (credentials.getPassword().toString().equals(password)) {
                log.info("Admin access granted by " + authCtx.subjectType() + " from " + authCtx.address());
                return new AllowAll();
            }
            else {
                log.warning("Admin password error by " + authCtx.subjectType() + " from " + authCtx.address());
            }
        }
        UUID uuid = authCtx.subjectId();
        SecuritySubjectType securitySubjectType = authCtx.subjectType();
        return new AllowSome(
            new Subject(uuid,
                securitySubjectType,
                credentials != null ? credentials.getLogin() : null,
                authCtx.address())
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
    @Override public void authorize(String name, SecurityPermission perm,
        @Nullable SecurityContext securityCtx) throws SecurityException {
        if (securityCtx != null) {
            if (AllowSome.class == securityCtx.getClass()) {
                if (perm != SecurityPermission.TASK_EXECUTE) {
                    SecuritySubject s = securityCtx.subject();
                    log.info("Access denied on "
                        + perm
                        + "@"
                        + name
                        + " by "
                        + s.type()
                        + " from "
                        + s.address()
                        + ". Login "
                        + s.login());
                    throw new SecurityException("Access denied");
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onSessionExpired(UUID subjId) {

    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return true;
    }

    /**
     * Security context for admin users and internal node communications
     */
    private static class AllowAll implements SecurityContext, Serializable {
        /**
         * Serial version uid.
         */
        private static final long serialVersionUID = 0L;

        /**
         * {@inheritDoc}
         */
        @Override
        public SecuritySubject subject() {
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean systemOperationAllowed(SecurityPermission perm) {
            return true;
        }
    }

    /**
     * Security context for default users without provided authentications
     */
    private static class AllowSome implements SecurityContext, Serializable {
        /**
         * Serial version uid.
         */
        private static final long serialVersionUID = 0L;

        /** Security Subject */
        private final SecuritySubject subject;

        /**
         * Constructor to store SecuritySubject instance
         *
         * @param subject SecuritySubject instance
         */
        AllowSome(SecuritySubject subject) {
            this.subject = subject;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SecuritySubject subject() {
            return subject;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean systemOperationAllowed(SecurityPermission perm) {
            return true;
        }
    }

    /**
     * Security Subject implementation for test
     */
    private static class Subject implements SecuritySubject {
        private static final long serialVersionUID = 0L;
        private final UUID id;
        private final SecuritySubjectType type;
        private final Object login;
        private final InetSocketAddress address;

        /**
         * @param id id
         * @param type type
         * @param login login
         * @param address address
         */
        Subject(UUID id, SecuritySubjectType type, Object login, InetSocketAddress address) {
            this.id = id;
            this.type = type;
            this.login = login;
            this.address = address;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public UUID id() {
            return id;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SecuritySubjectType type() {
            return type;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object login() {
            return login;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public InetSocketAddress address() {
            return address;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public SecurityPermissionSet permissions() {
            return null;
        }
    }
}
