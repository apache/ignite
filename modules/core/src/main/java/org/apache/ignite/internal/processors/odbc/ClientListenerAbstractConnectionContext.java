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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.managers.systemview.SystemViewMap;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.spi.systemview.ReadOnlySystemViewRegistry;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.UUID;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

/**
 * Base connection context.
 */
public abstract class ClientListenerAbstractConnectionContext
    implements ClientListenerConnectionContext {
    /** */
    public static final String CLI_CONN_SYS_VIEW = metricName("client", "connections");

    /** */
    public static final String CLI_CONN_SYS_VIEW_DESC = "Client connections";

    /** Kernal context. */
    protected final GridKernalContext ctx;

    /** Security context or {@code null} if security is disabled. */
    private SecurityContext secCtx;

    /** Connection ID. */
    private long connId;

    /** Session. */
    protected final GridNioSession ses;

    /** Client version. */
    private ClientListenerProtocolVersion ver;

    /** Authorization context. */
    protected AuthorizationContext authCtx;

    /**
     * Client connections system view.
     *
     * @see ReadOnlySystemViewRegistry
     * @see GridSystemViewManager
     */
    @Nullable protected SystemViewMap<Long, ClientConnectionView> connMonList;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param connId Connection id.
     * @param ses Grid NIO session.
     */
    protected ClientListenerAbstractConnectionContext(GridKernalContext ctx, long connId, GridNioSession ses) {
        this.ctx = ctx;
        this.connId = connId;
        this.ses = ses;

        connMonList = (SystemViewMap<Long, ClientConnectionView>)
            ctx.systemView().<ClientConnectionView>view(CLI_CONN_SYS_VIEW);
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

        connMonList.remove(connId);
    }

    public GridNioSession session() {
        return ses;
    }

    /** @return Currently used protocol version. */
    public ClientListenerProtocolVersion clientVersion() {
        return ver;
    }

    /**
     * Sets client version.
     *
     * @param ver Client version.
     */
    protected void version(ClientListenerProtocolVersion ver) {
        this.ver = ver;
    }
}
