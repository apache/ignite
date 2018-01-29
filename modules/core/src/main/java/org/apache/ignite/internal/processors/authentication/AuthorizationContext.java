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

/**
 * Ignite authentication context.
 */
public class AuthorizationContext {
    /** User. */
    private final User user;

    /**
     * Creates authentication context.
     *
     * @param user Authorized user.
     */
    public AuthorizationContext(User user) {
        this.user = user;
    }

    /**
     * @return Authorized user.
     */
    public String userName() {
        return user.name();
    }

    /**
     * @param op User operation to check.
     * @throws IgniteAccessControlException If operation check fails: user hasn't permissions for user management
     *      or try to remove default user.
     */
    public void checkUserOperation(UserManagementOperation op) throws IgniteAccessControlException {
        assert op != null;

        if (UserManagementOperation.OperationType.UPDATE != op.type() && !User.DFAULT_USER_NAME.equals(user.name()))
            throw new IgniteAccessControlException("Add / remove user is not allowed for user. [curUser= " + user.name() + ']');

        if (op.type() == UserManagementOperation.OperationType.REMOVE
            && User.DFAULT_USER_NAME.equals(op.user().name()))
            throw new IgniteAccessControlException("Default user cannot be removed.");
    }
}