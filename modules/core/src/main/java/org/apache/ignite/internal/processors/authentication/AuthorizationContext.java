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

package org.apache.ignite.internal.processors.authentication;

/**
 * Ignite authentication context.
 */
public class AuthorizationContext {
    /** User. */
    private final User user;

    /** Current authorization context. */
    private static ThreadLocal<AuthorizationContext> actx = new ThreadLocal<>();

    /**
     * Creates authentication context.
     *
     * @param user Authorized user.
     */
    public AuthorizationContext(User user) {
        assert user != null;

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
}