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

package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.security.sandbox.IgniteSandbox;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * This interface is responsible for:
 * <ul>
 *     <li>Node authentication;</li>
 *     <li>Thin client authentication;</li>
 *     <li>Providing configuration info whether global node authentication is enabled;</li>
 *     <li>Keeping and propagating all authenticated security subjects;</li>
 *     <li>Providing configuration info whether security mode is enabled at all;</li>
 *     <li>Handling expired sessions;</li>
 *     <li>Providing configuration info whether sandbox is enabled;</li>
 *     <li>Keeping and propagating authenticated security subject for thin clients;</li>
 *     <li>Keeping and propagating authenticated security contexts for nodes and thin clients;</li>
 *     <li>Authorizing specific operations (cache put, task execute, so on) when session security context is set.</li>
 * </ul>
 */
public interface GridSecurityProcessor extends GridProcessor {
    /**
     * Authenticates grid node with it's attributes via underlying Authenticator.
     *
     * @param node Node id to authenticate.
     * @param cred Security credentials.
     * @return {@code True} if succeeded, {@code false} otherwise.
     * @throws IgniteCheckedException If error occurred.
     */
    public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) throws IgniteCheckedException;

    /**
     * Gets flag indicating whether all nodes or coordinator only should run the authentication for joining node.
     *
     * @return {@code True} if all nodes should run authentication process, {@code false} otherwise.
     */
    public boolean isGlobalNodeAuthentication();

    /**
     * Authenticates subject via underlying Authenticator.
     *
     * @param ctx Authentication context.
     * @return {@code True} if succeeded, {@code false} otherwise.
     * @throws IgniteCheckedException If error occurred.
     */
    public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException;

    /**
     * Gets collection of authenticated nodes.
     *
     * @return Collection of authenticated nodes.
     * @throws IgniteCheckedException If error occurred.
     */
    public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException;

    /**
     * Gets authenticated node subject.
     *
     * @param subjId Subject ID.
     * @return Security subject.
     * @throws IgniteCheckedException If error occurred.
     */
    public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException;

    /**
     * Gets security context for authenticated nodes and thin clients.
     *
     * @param subjId Security subject id.
     * @return Security context or null if not found.
     */
    public default SecurityContext securityContext(UUID subjId) {
        throw new UnsupportedOperationException();
    }

    /**
     * Authorizes grid operation.
     *
     * @param name Cache name or task class name.
     * @param perm Permission to authorize.
     * @param securityCtx Optional security context.
     * @throws SecurityException If security check failed.
     */
    public void authorize(String name, SecurityPermission perm, SecurityContext securityCtx)
        throws SecurityException;

    /**
     * Callback invoked when subject session got expired.
     *
     * @param subjId Subject ID.
     */
    public void onSessionExpired(UUID subjId);

    /**
     * @return GridSecurityProcessor is enable.
     * @deprecated To determine the security mode use {@link IgniteSecurity#enabled()}.
     */
    @Deprecated
    public boolean enabled();

    /**
     * If this method returns true and {@link SecurityManager} is installed,
     * then the user-defined code will be run inside the Sandbox.
     *
     * @return True if sandbox is enabled.
     * @see IgniteSandbox
     */
    public default boolean sandboxEnabled() {
        return false;
    }

    /**
     * Creates user with the specified login and password.
     *
     * @param login Login of the user to be created.
     * @param pwd User password.
     * @throws IgniteCheckedException If error occurred.
     */
    public default void createUser(String login, char[] pwd) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Alters password of user with the specified login.
     *
     * @param login Login of the user which password should be altered.
     * @param pwd User password to alter.
     * @throws IgniteCheckedException If error occurred.
     */
    public default void alterUser(String login, char[] pwd) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Drops user with the specified login.
     *
     * @param login Login of the user to be dropped.
     * @throws IgniteCheckedException If error occurred.
     */
    public default void dropUser(String login) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }
}
