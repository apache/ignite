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

package org.apache.ignite.internal.ssl;

import java.security.cert.X509Certificate;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * A node component whose TLS certificates can be replaced at runtime, without a node restart.
 * <p>
 * A node registers one of these per configured SSL context factory once it has set SSL up, so an empty registry
 * means the node does not use SSL at all. Each registers under the names of the transports it serves, which is
 * also how the reload command reports it, so they are part of what an operator sees and scripts against.
 */
public interface SslContextReloadable {
    /**
     * @return Transports served, as the reload command reports them.
     */
    public Collection<String> users();

    /** */
    public static final String COMMUNICATION = "communication";

    /** */
    public static final String DISCOVERY = "discovery";

    /** */
    public static final String CLIENT_CONNECTOR = "client connector";

    /** */
    public static final String BINARY_REST = "binary REST";

    /** */
    public static final String HTTP_REST = "HTTP REST";

    /**
     * Rebuilds the SSL context from the configured factory and applies it. Connections opened afterwards use the
     * updated certificates, already established sessions are not interrupted.
     *
     * @return {@code True} if a new context was built and applied. {@code False} if the factory returned the context
     *      already in use: a custom {@link javax.cache.configuration.Factory} caching the context internally cannot
     *      rebuild it, so the certificates stay as they were.
     * @throws IgniteCheckedException If the context could not be rebuilt. The active context is kept.
     */
    public boolean reloadSslContext() throws IgniteCheckedException;

    /**
     * Rebuilds the SSL context and puts it through the same checks as {@link #reloadSslContext()}, but leaves the
     * component running on the context it already has. Lets an operator verify the stores on disk before changing
     * anything.
     *
     * @return Same as {@link #reloadSslContext()}.
     * @throws IgniteCheckedException If the context could not be rebuilt or did not pass the checks.
     */
    public boolean checkSslContext() throws IgniteCheckedException;

    /**
     * @return Certificate this component presents on new connections, or {@code null} if it cannot be told without
     *      a peer, which is the case for the transports a client connects to.
     */
    public default @Nullable X509Certificate servedCertificate() {
        return null;
    }
}
