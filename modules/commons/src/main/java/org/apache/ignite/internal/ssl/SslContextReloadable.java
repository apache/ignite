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
import java.util.UUID;
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
     * Builds the certificates that are on disk now, checks them, and keeps the result aside without touching what
     * is in use. Everything that can fail happens here, so that the phase which does put them in use cannot leave
     * the cluster on two different certificates.
     *
     * @param token Identifies this attempt; {@link #commit(UUID)} applies only what was prepared under the same one.
     * @return {@code True} if there is something new to put in use. {@code False} if the source handed back the
     *      context already in use and there is nothing to apply.
     * @throws IgniteCheckedException If the certificates could not be built or would not be accepted. Nothing is
     *      kept aside in that case, and what is in use stays.
     */
    public boolean prepare(UUID token) throws IgniteCheckedException;

    /** What {@link #commit(UUID)} found to do. */
    public enum Commit {
        /** The prepared certificates are now in use. */
        APPLIED,

        /** This attempt prepared and found the certificates already in use, so there was nothing to apply. */
        NOTHING_TO_APPLY,

        /** This attempt prepared nothing here, which is what a node that joined between the phases looks like. */
        NOT_PREPARED
    }

    /**
     * Puts in use what {@link #prepare(UUID)} kept aside under the same token. Connections opened afterwards use the
     * new certificates, established sessions are not interrupted.
     *
     * @param token Attempt whose result is to be applied.
     * @return What there was to do.
     */
    public Commit commit(UUID token);

    /** Drops whatever was kept aside, leaving what is in use alone. */
    public void discard();

    /**
     * @return Certificate this component presents on new connections, or {@code null} if it cannot be told without
     *      a peer, which is the case for the transports a client connects to.
     */
    public default @Nullable X509Certificate servedCertificate() {
        return null;
    }
}
