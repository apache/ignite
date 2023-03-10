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
import org.apache.ignite.internal.processors.security.sandbox.IgniteSandbox;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * Ignite Security Processor.
 * <p>
 * The differences between {@code IgniteSecurity} and {@code GridSecurityProcessor} are:
 * <ul>
 * <li>{@code IgniteSecurity} allows to define a current security context by
 * {@link #withContext(SecurityContext)} or {@link #withContext(UUID)} methods.
 * <li>{@code IgniteSecurity} doesn't require to pass {@code SecurityContext} to authorize operations.
 * <li>{@code IgniteSecurity} doesn't extend {@code GridProcessor} interface
 * sequentially it doesn't have any methods of the lifecycle of {@code GridProcessor}.
 * </ul>
 */
public interface IgniteSecurity {
    /**
     * Creates {@link OperationSecurityContext}. All calls of methods {@link #authorize(String, SecurityPermission)} or {@link
     * #authorize(SecurityPermission)} will be processed into the context of passed {@link SecurityContext} until
     * holder {@link OperationSecurityContext} will be closed.
     *
     * @param secCtx Security Context.
     * @return Security context holder.
     */
    public OperationSecurityContext withContext(SecurityContext secCtx);

    /**
     * Creates {@link OperationSecurityContext}. All calls of methods {@link #authorize(String, SecurityPermission)} or {@link
     * #authorize(SecurityPermission)} will be processed into the context of {@link SecurityContext} that is owned by
     * the node with given nodeId until holder {@link OperationSecurityContext} will be closed.
     *
     * @param nodeId Node id.
     * @return Security context holder.
     */
    public OperationSecurityContext withContext(UUID nodeId);

    /** @return {@code True} if current thread executed in default security context. */
    public boolean isDefaultContext();

    /**
     * @return SecurityContext of holder {@link OperationSecurityContext}.
     */
    public SecurityContext securityContext();

    /**
     * Delegates call to {@link GridSecurityProcessor#authenticateNode(org.apache.ignite.cluster.ClusterNode,
     * org.apache.ignite.plugin.security.SecurityCredentials)}
     */
    public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) throws IgniteCheckedException;

    /**
     * Delegates call to {@link GridSecurityProcessor#isGlobalNodeAuthentication()}
     */
    public boolean isGlobalNodeAuthentication();

    /**
     * Delegates call to {@link GridSecurityProcessor#authenticate(AuthenticationContext)}
     */
    public SecurityContext authenticate(AuthenticationContext ctx) throws IgniteCheckedException;

    /**
     * Delegates call to {@link GridSecurityProcessor#authenticatedSubjects()}
     */
    public Collection<SecuritySubject> authenticatedSubjects() throws IgniteCheckedException;

    /**
     * Delegates call to {@link GridSecurityProcessor#authenticatedSubject(UUID)}
     */
    public SecuritySubject authenticatedSubject(UUID subjId) throws IgniteCheckedException;

    /**
     * Delegates call to {@link GridSecurityProcessor#onSessionExpired(UUID)}
     */
    public void onSessionExpired(UUID subjId);

    /**
     * Authorizes grid operation.
     *
     * @param name Cache name or task class name.
     * @param perm Permission to authorize.
     * @throws SecurityException If security check failed.
     */
    public void authorize(String name, SecurityPermission perm) throws SecurityException;

    /**
     * Authorizes grid system operation.
     *
     * @param perm Permission to authorize.
     * @throws SecurityException If security check failed.
     */
    public default void authorize(SecurityPermission perm) throws SecurityException {
        authorize(null, perm);
    }

    /**
     * @return Instance of IgniteSandbox.
     */
    public IgniteSandbox sandbox();

    /**
     * @return True if IgniteSecurity is a plugin implementation,
     * false if it's used a default NoOp implementation.
     */
    public boolean enabled();

    /**
     * Creates user with the specified login and password.
     *
     * @param login Login of the user to be created.
     * @param pwd User password.
     * @throws IgniteCheckedException If error occurred.
     */
    public void createUser(String login, char[] pwd) throws IgniteCheckedException;

    /**
     * Alters password of user with the specified login.
     *
     * @param login Login of the user which password should be altered.
     * @param pwd User password to alter.
     * @throws IgniteCheckedException If error occurred.
     */
    public void alterUser(String login, char[] pwd) throws IgniteCheckedException;

    /**
     * Drops user with the specified login.
     *
     * @param login Login of the user to be dropped.
     * @throws IgniteCheckedException If error occurred.
     */
    public void dropUser(String login) throws IgniteCheckedException;

    /**
     * Callback for local join events for which the regular events are not generated.
     * <p/>
     * Local join event is expected in cases of joining to topology or client reconnect.
     */
    public void onLocalJoin();

    /**
     * @param cls The class for which the check is to be performed.
     * @return Whether the specified class can be considered system. System classes are classes whose source code
     * can be considered controlled by the Ignite administrator and to which less stringent security checks can be
     * applied. By default, Ignite considers only classes from its own codebase as system but their pool can be extended
     * by custom Security Plugin.
     */
    public boolean isSystemType(Class<?> cls);
}
