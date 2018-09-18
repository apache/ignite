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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;

/**
 * JDBC Connection Context.
 */
public class JdbcConnectionContext extends ClientListenerAbstractConnectionContext {
    /** Version 2.1.0. */
    private static final ClientListenerProtocolVersion VER_2_1_0 = ClientListenerProtocolVersion.create(2, 1, 0);

    /** Version 2.1.5: added "lazy" flag. */
    private static final ClientListenerProtocolVersion VER_2_1_5 = ClientListenerProtocolVersion.create(2, 1, 5);

    /** Version 2.3.1: added "multiple statements query" feature. */
    static final ClientListenerProtocolVersion VER_2_3_0 = ClientListenerProtocolVersion.create(2, 3, 0);

    /** Version 2.4.0: adds default values for columns feature. */
    static final ClientListenerProtocolVersion VER_2_4_0 = ClientListenerProtocolVersion.create(2, 4, 0);

    /** Version 2.5.0: adds precision and scale for columns feature. */
    static final ClientListenerProtocolVersion VER_2_5_0 = ClientListenerProtocolVersion.create(2, 5, 0);

    /** Version 2.7.0: adds maximum length for columns feature.*/
    static final ClientListenerProtocolVersion VER_2_7_0 = ClientListenerProtocolVersion.create(2, 7, 0);

    /** Current version. */
    private static final ClientListenerProtocolVersion CURRENT_VER = VER_2_7_0;

    /** Supported versions. */
    private static final Set<ClientListenerProtocolVersion> SUPPORTED_VERS = new HashSet<>();

    /** Session. */
    private final GridNioSession ses;

    /** Shutdown busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Logger. */
    private final IgniteLogger log;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Message parser. */
    private JdbcMessageParser parser = null;

    /** Request handler. */
    private JdbcRequestHandler handler = null;

    static {
        SUPPORTED_VERS.add(CURRENT_VER);
        SUPPORTED_VERS.add(VER_2_7_0);
        SUPPORTED_VERS.add(VER_2_5_0);
        SUPPORTED_VERS.add(VER_2_4_0);
        SUPPORTED_VERS.add(VER_2_3_0);
        SUPPORTED_VERS.add(VER_2_1_5);
        SUPPORTED_VERS.add(VER_2_1_0);
    }

    /**
     * Constructor.
     *  @param ctx Kernal Context.
     * @param ses Session.
     * @param busyLock Shutdown busy lock.
     * @param connId
     * @param maxCursors Maximum allowed cursors.
     */
    public JdbcConnectionContext(GridKernalContext ctx, GridNioSession ses, GridSpinBusyLock busyLock, long connId,
        int maxCursors) {
        super(ctx, connId);

        this.ses = ses;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public boolean isVersionSupported(ClientListenerProtocolVersion ver) {
        return SUPPORTED_VERS.contains(ver);
    }

    /** {@inheritDoc} */
    @Override public ClientListenerProtocolVersion currentVersion() {
        return CURRENT_VER;
    }

    /** {@inheritDoc} */
    @Override public void initializeFromHandshake(ClientListenerProtocolVersion ver, BinaryReaderExImpl reader)
        throws IgniteCheckedException {
        assert SUPPORTED_VERS.contains(ver): "Unsupported JDBC protocol version.";

        boolean distributedJoins = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();
        boolean collocated = reader.readBoolean();
        boolean replicatedOnly = reader.readBoolean();
        boolean autoCloseCursors = reader.readBoolean();

        boolean lazyExec = false;
        boolean skipReducerOnUpdate = false;

        NestedTxMode nestedTxMode = NestedTxMode.DEFAULT;
        AuthorizationContext actx = null;

        if (ver.compareTo(VER_2_1_5) >= 0)
            lazyExec = reader.readBoolean();

        if (ver.compareTo(VER_2_3_0) >= 0)
            skipReducerOnUpdate = reader.readBoolean();

        if (ver.compareTo(VER_2_7_0) >= 0) {
            String nestedTxModeName = reader.readString();

            if (!F.isEmpty(nestedTxModeName)) {
                try {
                    nestedTxMode = NestedTxMode.valueOf(nestedTxModeName);
                }
                catch (IllegalArgumentException e) {
                    throw new IgniteCheckedException("Invalid nested transactions handling mode: " + nestedTxModeName);
                }
            }
        }

        if (ver.compareTo(VER_2_5_0) >= 0) {
            String user = null;
            String passwd = null;

            try {
                if (reader.available() > 0) {
                    user = reader.readString();
                    passwd = reader.readString();
                }
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Handshake error: " + e.getMessage(), e);
            }

            actx = authenticate(user, passwd);
        }

        parser = new JdbcMessageParser(ctx, ver);

        JdbcResponseSender sender = new JdbcResponseSender() {
            @Override public void send(ClientListenerResponse resp) {
                if (resp != null) {
                    if (log.isDebugEnabled())
                        log.debug("Async response: [resp=" + resp.status() + ']');

                    byte[] outMsg = parser.encode(resp);

                    ses.send(outMsg);
                }
            }
        };

        handler = new JdbcRequestHandler(ctx, busyLock, sender, maxCursors, distributedJoins, enforceJoinOrder,
            collocated, replicatedOnly, autoCloseCursors, lazyExec, skipReducerOnUpdate, nestedTxMode, actx, ver);

        handler.start();
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
        handler.onDisconnect();
    }
}
