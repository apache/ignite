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
 * <p>
 * Every node reloads on its own. Nothing is coordinated between nodes, so a reload that fails on some of them leaves
 * the others on the new certificates.
 */
public interface SslContextReloadable {
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
     * @return Transports served, as the reload command reports them.
     */
    public Collection<String> users();

    /**
     * Builds the certificates that are on disk now, checks them and puts them in use. Connections opened afterwards
     * use the new certificates, established ones are not interrupted.
     *
     * @return {@code True} if new certificates were put in use. {@code False} if the source handed back the context
     *      already in use, so there is nothing to read again.
     * @throws IgniteCheckedException If the certificates could not be built or would not be accepted. The ones in
     *      use stay.
     */
    public boolean reload() throws IgniteCheckedException;

    /**
     * Builds the certificates that are on disk now and checks them, without putting them in use.
     *
     * @return {@code True} if there is something new to put in use, {@code false} if the source would hand back the
     *      context already in use.
     * @throws IgniteCheckedException If the certificates could not be built or would not be accepted.
     */
    public boolean check() throws IgniteCheckedException;

    /**
     * @return Certificate this component presents on new connections, or {@code null} if it cannot be told without
     *      a peer, which is the case for the transports a client connects to.
     */
    public default @Nullable X509Certificate servedCertificate() {
        return null;
    }
}
