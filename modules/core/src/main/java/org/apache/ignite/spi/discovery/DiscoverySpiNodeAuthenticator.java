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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.SecurityCredentials;

/**
 * Node authenticator.
 */
public interface DiscoverySpiNodeAuthenticator {
    /**
     * Security credentials.
     *
     * @param node Node to authenticate.
     * @param cred Security credentials.
     * @return Security context if authentication succeeded or {@code null} if authentication failed.
     * @throws IgniteException If authentication process failed
     *      (invalid credentials should not lead to this exception).
     */
    public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) throws IgniteException;

    /**
     * Gets global node authentication flag.
     *
     * @return {@code True} if all nodes in topology should authenticate joining node, {@code false} if only
     *      coordinator should do the authentication.
     */
    public boolean isGlobalNodeAuthentication();
}