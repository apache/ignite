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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.UUID;

import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Base connection context.
 */
public abstract class ClientListenerAbstractConnectionContext implements ClientListenerConnectionContext {
    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Security context or {@code null} if security is disabled. */
    private SecurityContext secCtx;

    /** Connection ID. */
    private long connId;

    /** Authorization context. */
    private AuthorizationContext authCtx;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    protected ClientListenerAbstractConnectionContext(GridKernalContext ctx, long connId) {
        this.ctx = ctx;
        this.connId = connId;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public SecurityContext securityContext() {
        return secCtx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public AuthorizationContext authorizationContext() {
        return authCtx;
    }

    /** {@inheritDoc} */
    @Override public long connectionId() {
        return connId;
    }

    /**
     * Perform authentication.
     *
     * @return Auth context.
     * @throws IgniteCheckedException If failed.
     */
    protected AuthorizationContext authenticate(String user, String pwd) throws IgniteCheckedException {
        if (ctx.security().enabled())
            authCtx = authenticateExternal(user, pwd).authorizationContext();
        else if (ctx.authentication().enabled()) {
            if (F.isEmpty(user))
                throw new IgniteAccessControlException("Unauthenticated sessions are prohibited.");

            authCtx = ctx.authentication().authenticate(user, pwd);

            if (authCtx == null)
                throw new IgniteAccessControlException("Unknown authentication error.");
        }
        else
            authCtx = null;

        return authCtx;
    }

    /**
     * Do 3-rd party authentication.
     */
    private AuthenticationContext authenticateExternal(String user, String pwd) throws IgniteCheckedException {
        SecurityCredentials cred = new SecurityCredentials(user, pwd);

        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.subjectType(REMOTE_CLIENT);
        authCtx.subjectId(UUID.randomUUID());
        authCtx.nodeAttributes(Collections.emptyMap());
        authCtx.credentials(cred);

        secCtx = ctx.security().authenticate(authCtx);

        if (secCtx == null)
            throw new IgniteAccessControlException(
                String.format("The user name or password is incorrect [userName=%s]", user)
            );

        return authCtx;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected() {
        if (ctx.security().enabled())
            ctx.security().onSessionExpired(secCtx.subject().id());
    }
}
