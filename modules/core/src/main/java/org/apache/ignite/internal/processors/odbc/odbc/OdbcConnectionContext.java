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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerAbstractConnectionContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponseSender;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioSession;

import java.util.HashSet;
import java.util.Set;

/**
 * ODBC Connection Context.
 */
public class OdbcConnectionContext extends ClientListenerAbstractConnectionContext {
    /** Version 2.1.0. */
    public static final ClientListenerProtocolVersion VER_2_1_0 = ClientListenerProtocolVersion.create(2, 1, 0);

    /** Version 2.1.5: added "lazy" flag. */
    public static final ClientListenerProtocolVersion VER_2_1_5 = ClientListenerProtocolVersion.create(2, 1, 5);

    /** Version 2.3.0: added "skipReducerOnUpdate" flag. */
    public static final ClientListenerProtocolVersion VER_2_3_0 = ClientListenerProtocolVersion.create(2, 3, 0);

    /** Version 2.3.2: added multiple statements support. */
    public static final ClientListenerProtocolVersion VER_2_3_2 = ClientListenerProtocolVersion.create(2, 3, 2);

    /** Version 2.5.0: added authentication. */
    public static final ClientListenerProtocolVersion VER_2_5_0 = ClientListenerProtocolVersion.create(2, 5, 0);

    /** Version 2.7.0: added precision and scale. */
    public static final ClientListenerProtocolVersion VER_2_7_0 = ClientListenerProtocolVersion.create(2, 7, 0);

    /** Current version. */
    private static final ClientListenerProtocolVersion CURRENT_VER = VER_2_7_0;

    /** Supported versions. */
    private static final Set<ClientListenerProtocolVersion> SUPPORTED_VERS = new HashSet<>();

    /** Session. */
    private final GridNioSession ses;

    /** Shutdown busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Message parser. */
    private OdbcMessageParser parser = null;

    /** Request handler. */
    private OdbcRequestHandler handler = null;

    /** Logger. */
    private final IgniteLogger log;

    static {
        SUPPORTED_VERS.add(CURRENT_VER);
        SUPPORTED_VERS.add(VER_2_5_0);
        SUPPORTED_VERS.add(VER_2_3_0);
        SUPPORTED_VERS.add(VER_2_3_2);
        SUPPORTED_VERS.add(VER_2_1_5);
        SUPPORTED_VERS.add(VER_2_1_0);
    }

    /**
     * Constructor.
     * @param ctx Kernal Context.
     * @param ses Session.
     * @param busyLock Shutdown busy lock.
     * @param connId Connection ID.
     * @param maxCursors Maximum allowed cursors.
     */
    public OdbcConnectionContext(GridKernalContext ctx, GridNioSession ses, GridSpinBusyLock busyLock, long connId, int maxCursors) {
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
        assert SUPPORTED_VERS.contains(ver): "Unsupported ODBC protocol version.";

        boolean distributedJoins = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();
        boolean replicatedOnly = reader.readBoolean();
        boolean collocated = reader.readBoolean();

        boolean lazy = false;

        if (ver.compareTo(VER_2_1_5) >= 0)
            lazy = reader.readBoolean();

        boolean skipReducerOnUpdate = false;

        if (ver.compareTo(VER_2_3_0) >= 0)
            skipReducerOnUpdate = reader.readBoolean();

        String user = null;
        String passwd = null;

        NestedTxMode nestedTxMode = NestedTxMode.DEFAULT;

        if (ver.compareTo(VER_2_5_0) >= 0) {
            user = reader.readString();
            passwd = reader.readString();

            byte nestedTxModeVal = reader.readByte();

            nestedTxMode = NestedTxMode.fromByte(nestedTxModeVal);
        }

        AuthorizationContext actx = authenticate(user, passwd);

        ClientListenerResponseSender sender = new ClientListenerResponseSender() {
            @Override public void send(ClientListenerResponse resp) {
                if (resp != null) {
                    if (log.isDebugEnabled())
                        log.debug("Async response: [resp=" + resp.status() + ']');

                    byte[] outMsg = parser.encode(resp);

                    ses.send(outMsg);
                }
            }
        };

        handler = new OdbcRequestHandler(ctx, busyLock, sender, maxCursors, distributedJoins, enforceJoinOrder,
            replicatedOnly, collocated, lazy, skipReducerOnUpdate, actx, nestedTxMode, ver);

        parser = new OdbcMessageParser(ctx, ver);

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
