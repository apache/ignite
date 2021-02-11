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

package org.apache.ignite.spi.systemview.view;

import java.net.InetSocketAddress;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerConnectionContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.odbc.ClientListenerNioListener.CONN_CTX_META_KEY;

/**
 * Client connection system view row.
 */
public class ClientConnectionView {
    /** Nio session. */
    private final GridNioSession ses;

    /** Connection context. */
    @Nullable private final ClientListenerConnectionContext ctx;

    /** @param ses Nio session. */
    public ClientConnectionView(GridNioSession ses) {
        this.ses = ses;
        this.ctx = ses.meta(CONN_CTX_META_KEY);
    }

    /** @return Connection id. */
    public long connectionId() {
        if (ctx == null)
            return -1;

        return ctx.connectionId();
    }

    /** @return Connection type. */
    public String type() {
        if (ctx == null)
            return null;

        if (ctx instanceof OdbcConnectionContext)
            return "ODBC";
        else if (ctx instanceof JdbcConnectionContext)
            return "JDBC";
        else if (ctx instanceof ClientConnectionContext)
            return "THIN";

        return "unknown";
    }

    /** @return Connection local address. */
    public InetSocketAddress localAddress() {
        return ses.localAddress();
    }

    /** @return Connection remote address. */
    public InetSocketAddress remoteAddress() {
        return ses.remoteAddress();
    }

    /** @return User name. */
    public String user() {
        if (ctx == null)
            return null;

        AuthorizationContext authCtx = ctx.authorizationContext();

        return authCtx == null ? null : authCtx.userName();
    }

    /** @return Protocol version. */
    public String version() {
        if (ctx == null)
            return null;

        ClientListenerRequestHandler hnd = ctx.handler();

        if (hnd == null)
            return null;

        ClientListenerProtocolVersion ver = hnd.protocolVersion();

        return ver == null ? null : ver.asString();
    }
}

