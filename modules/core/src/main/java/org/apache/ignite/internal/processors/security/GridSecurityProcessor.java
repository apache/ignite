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

package org.apache.ignite.internal.processors.security;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.jetbrains.annotations.Nullable;

/**
 * This interface defines a grid authentication processor.
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
     * Authorizes grid operation.
     *
     * @param name Cache name or task class name.
     * @param perm Permission to authorize.
     * @param securityCtx Optional security context.
     * @throws SecurityException If security check failed.
     */
    public void authorize(String name, SecurityPermission perm, @Nullable SecurityContext securityCtx)
        throws SecurityException;

    /**
     * Callback invoked when subject session got expired.
     *
     * @param subjId Subject ID.
     */
    public void onSessionExpired(UUID subjId);

    /**
     * @return GridSecurityProcessor is enable.
     */
    public boolean enabled();
}