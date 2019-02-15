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
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;

/**
 * Ignite Security Processor.
 */
public interface IgniteSecurityProcessor {
    /**
     * Creates {@link IgniteSecuritySession}. All calls of methods {@link #authorize(String, SecurityPermission)} or {@link
     * #authorize(SecurityPermission)} will be processed into the context of passed {@link SecurityContext} until
     * session {@link IgniteSecuritySession} will be closed.
     *
     * @param secCtx Security Context.
     * @return Grid security Session.
     */
    public IgniteSecuritySession startSession(SecurityContext secCtx);

    /**
     * Creates {@link IgniteSecuritySession}. All calls of methods {@link #authorize(String, SecurityPermission)} or {@link
     * #authorize(SecurityPermission)} will be processed into the context of {@link SecurityContext} that is owned by
     * node with given nodeId until session {@link IgniteSecuritySession} will be closed.
     *
     * @param nodeId Node id.
     * @return Grid security Session.
     */
    public IgniteSecuritySession startSession(UUID nodeId);

    /**
     * @return SecurityContext of opened session {@link IgniteSecuritySession}.
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
     * @return True if IgniteSecurityProcessor is a plugin implementation,
     * false if it's used a default NoOp implementation.
     */
    public boolean enabled();
}
