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
import org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;

/**
 * Client connection system view row.
 */
public class ClientConnectionView {
    /** Type of the connection. */
    public enum ConnectionType {
        ODBC, JDBC, THIN;
    }

    /** Connection context. */
    private final ClientListenerAbstractConnectionContext ctx;

    /** Connection type. */
    private final ConnectionType type;

    public ClientConnectionView(ClientListenerAbstractConnectionContext ctx, ConnectionType type) {
        this.ctx = ctx;
        this.type = type;
    }

    /** @return Connection id. */
    public long connectionId() {
        return ctx.connectionId();
    }

    /** @return Connection type. */
    public ConnectionType type() {
        return type;
    }

    /** @return Connection local address. */
    public InetSocketAddress localAddress() {
        return ctx.session().localAddress();
    }

    /** @return Connection remote address. */
    public InetSocketAddress remoteAddress() {
        return ctx.session().remoteAddress();
    }

    /** @return User name. */
    public String user() {
        AuthorizationContext authCtx = ctx.authorizationContext();

        return authCtx == null ? null : authCtx.userName();
    }

    /** @return Protocol version. */
    public String version() {
        ClientListenerProtocolVersion ver = ctx.clientVersion();

        return ver == null ? null : ver.asString();
    }
}

