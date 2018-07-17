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

package org.apache.ignite.internal.processors.platform.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.odbc.ClientListenerConnectionContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;

import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Thin Client connection context.
 */
public class ClientConnectionContext implements ClientListenerConnectionContext {
    /** Version 1.0.0. */
    public static final ClientListenerProtocolVersion VER_1_0_0 = ClientListenerProtocolVersion.create(1, 0, 0);

    /** Version 1.1.0. */
    public static final ClientListenerProtocolVersion VER_1_1_0 = ClientListenerProtocolVersion.create(1, 1, 0);

    /** Version 1.2.0. */
    public static final ClientListenerProtocolVersion VER_1_2_0 = ClientListenerProtocolVersion.create(1, 2, 0);

    /** Supported versions. */
    private static final Collection<ClientListenerProtocolVersion> SUPPORTED_VERS = Arrays.asList(VER_1_2_0, VER_1_1_0, VER_1_0_0);

    /** Message parser. */
    private final ClientMessageParser parser;

    /** Request handler. */
    private ClientRequestHandler handler;

    /** Handle registry. */
    private final ClientResourceRegistry resReg = new ClientResourceRegistry();

    /** Kernal context. */
    private final GridKernalContext kernalCtx;

    /** Max cursors. */
    private final int maxCursors;

    /** Cursor counter. */
    private final AtomicLong curCnt = new AtomicLong();

    /** Security context or {@code null} if security is disabled. */
    private SecurityContext secCtx = null;

    /**
     * Ctor.
     *
     * @param ctx Kernal context.
     * @param maxCursors Max active cursors.
     */
    public ClientConnectionContext(GridKernalContext ctx, int maxCursors) {
        assert ctx != null;

        kernalCtx = ctx;

        parser = new ClientMessageParser(ctx);

        this.maxCursors = maxCursors;
    }

    /**
     * Gets the handle registry.
     *
     * @return Handle registry.
     */
    public ClientResourceRegistry resources() {
        return resReg;
    }

    /**
     * Gets the kernal context.
     *
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return kernalCtx;
    }

    /** {@inheritDoc} */
    @Override public boolean isVersionSupported(ClientListenerProtocolVersion ver) {
        return SUPPORTED_VERS.contains(ver);
    }

    /** {@inheritDoc} */
    @Override public ClientListenerProtocolVersion currentVersion() {
        return VER_1_1_0;
    }

    /** {@inheritDoc} */
    @Override public void initializeFromHandshake(ClientListenerProtocolVersion ver, BinaryReaderExImpl reader)
        throws IgniteCheckedException {
        boolean hasMore;

        String user = null;
        String pwd = null;
        AuthorizationContext authCtx = null;

        if (ver.compareTo(VER_1_1_0) >= 0) {
            try {
                hasMore = reader.available() > 0;
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Handshake error: " + e.getMessage(), e);
            }

            if (hasMore) {
                user = reader.readString();
                pwd = reader.readString();
            }
        }

        if (kernalCtx.security().enabled())
            authCtx = thirdPartyAuthentication(user, pwd).authorizationContext();
        else if (kernalCtx.authentication().enabled()) {
            if (user == null || user.length() == 0)
                throw new IgniteAccessControlException("Unauthenticated sessions are prohibited.");

            authCtx = kernalCtx.authentication().authenticate(user, pwd);

            if (authCtx == null)
                throw new IgniteAccessControlException("Unknown authentication error.");
        }

        handler = new ClientRequestHandler(this, authCtx);
    }

    /** {@inheritDoc} */
    @Override public ClientListenerRequestHandler handler() {
        return handler;
    }

    /** {@inheritDoc} */
    @Override public ClientListenerMessageParser parser() {
        return parser;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected() {
        resReg.clean();
    }

    /**
     * Increments the cursor count.
     */
    public void incrementCursors() {
        long curCnt0 = curCnt.get();

        if (curCnt0 >= maxCursors) {
            throw new IgniteClientException(ClientStatus.TOO_MANY_CURSORS,
                "Too many open cursors (either close other open cursors or increase the " +
                "limit through ClientConnectorConfiguration.maxOpenCursorsPerConnection) [maximum=" + maxCursors +
                ", current=" + curCnt0 + ']');
        }

        curCnt.incrementAndGet();
    }

    /**
     * Increments the cursor count.
     */
    public void decrementCursors() {
        curCnt.decrementAndGet();
    }

    /**
     * @return Security context or {@code null} if security is disabled.
     */
    public SecurityContext securityContext() {
        return secCtx;
    }

    /**
     * Do 3-rd party authentication.
     */
    private AuthenticationContext thirdPartyAuthentication(String user, String pwd) throws IgniteCheckedException {
        SecurityCredentials cred = new SecurityCredentials(user, pwd);

        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(UUID.randomUUID());
        authCtx.nodeAttributes(Collections.emptyMap());
        authCtx.credentials(cred);

        secCtx = kernalCtx.security().authenticate(authCtx);

        if (secCtx == null)
            throw new IgniteAccessControlException(
                String.format("The user name or password is incorrect [userName=%s]", user)
            );

        return authCtx;
    }
}
