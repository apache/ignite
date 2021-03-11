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

package org.apache.ignite.internal.processors.authentication;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * Ignite authentication context.
 */
public class AuthorizationContext implements SecurityContext, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** User. */
    private final User user;

    /** */
    private SecuritySubject subj;

    /** Current authorization context. */
    private static ThreadLocal<AuthorizationContext> actx = new ThreadLocal<>();

    /**
     * Creates authentication context.
     *
     * @param user Authorized user.
     */
    public AuthorizationContext(User user) {
        this.user = user;
    }

    /** */
    public AuthorizationContext(UUID id, User user) {
        this.user = user;

        subj = new IgniteSecuritySubject(id, null, user == null ? null : user.name(), null, null);
    }

    /**
     * @return Authorized user.
     */
    public String userName() {
        return user == null ? null : user.name();
    }

    /**
     * @param op User operation to check.
     * @throws IgniteAccessControlException If operation check fails: user hasn't permissions for user management
     *      or try to remove default user.
     */
    public void checkUserOperation(UserManagementOperation op) throws IgniteAccessControlException {
        assert op != null;

        if (user == null)
            throw new IgniteAccessControlException("Operation not allowed: authorized context is empty.");

        if (!User.DFAULT_USER_NAME.equals(user.name())
            && !(UserManagementOperation.OperationType.UPDATE == op.type() && user.name().equals(op.user().name())))
            throw new IgniteAccessControlException("User management operations are not allowed for user. " +
                "[curUser=" + user.name() + ']');

        if (op.type() == UserManagementOperation.OperationType.REMOVE
            && User.DFAULT_USER_NAME.equals(op.user().name()))
            throw new IgniteAccessControlException("Default user cannot be removed.");
    }

    /**
     * @param actx Authorization context to set.
     */
    public static void context(AuthorizationContext actx) {
        AuthorizationContext.actx.set(actx);
    }

    /**
     * Clear authentication context.
     */
    public static void clear() {
        actx.set(null);
    }

    /**
     * @return Current authorization context.
     */
    public static AuthorizationContext context() {
        return actx.get();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject subject() {
        return subj;
    }

    /** {@inheritDoc} */
    @Override public boolean taskOperationAllowed(String taskClsName, SecurityPermission perm) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean cacheOperationAllowed(String cacheName, SecurityPermission perm) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean serviceOperationAllowed(String srvcName, SecurityPermission perm) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean systemOperationAllowed(SecurityPermission perm) {
        return true;
    }
}
