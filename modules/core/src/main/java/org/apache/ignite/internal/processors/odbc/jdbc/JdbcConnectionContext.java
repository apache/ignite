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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerConnectionContext;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.util.GridSpinBusyLock;

/**
 * ODBC Connection Context.
 */
public class JdbcConnectionContext implements ClientListenerConnectionContext {
    /** Version 2.1.0. */
    private static final ClientListenerProtocolVersion VER_2_1_0 = ClientListenerProtocolVersion.create(2, 1, 0);

    /** Version 2.1.5: added "lazy" flag. */
    private static final ClientListenerProtocolVersion VER_2_1_5 = ClientListenerProtocolVersion.create(2, 1, 5);

    /** Version 2.3.1: added "multiple statements query" feature. */
    public static final ClientListenerProtocolVersion VER_2_3_1 = ClientListenerProtocolVersion.create(2, 3, 1);

    /** Current version. */
    private static final ClientListenerProtocolVersion CURRENT_VER = VER_2_3_1;

    /** Supported versions. */
    private static final Set<ClientListenerProtocolVersion> SUPPORTED_VERS = new HashSet<>();

    /** Context. */
    private final GridKernalContext ctx;

    /** Shutdown busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Message parser. */
    private JdbcMessageParser parser = null;

    /** Request handler. */
    private JdbcRequestHandler handler = null;

    static {
        SUPPORTED_VERS.add(CURRENT_VER);
        SUPPORTED_VERS.add(VER_2_1_5);
        SUPPORTED_VERS.add(VER_2_1_0);
    }

    /**
     * Constructor.
     * @param ctx Kernal Context.
     * @param busyLock Shutdown busy lock.
     * @param maxCursors Maximum allowed cursors.
     */
    public JdbcConnectionContext(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;
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
    @Override public void initializeFromHandshake(ClientListenerProtocolVersion ver, BinaryReaderExImpl reader) {
        assert SUPPORTED_VERS.contains(ver): "Unsupported JDBC protocol version.";

        boolean distributedJoins = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();
        boolean collocated = reader.readBoolean();
        boolean replicatedOnly = reader.readBoolean();
        boolean autoCloseCursors = reader.readBoolean();

        boolean lazyExec = false;

        if (ver.compareTo(VER_2_1_5) >= 0)
            lazyExec = reader.readBoolean();

        handler = new JdbcRequestHandler(ctx, busyLock, maxCursors, distributedJoins, enforceJoinOrder,
            collocated, replicatedOnly, autoCloseCursors, lazyExec, ver);

        parser = new JdbcMessageParser(ctx);
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
