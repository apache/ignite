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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerConnectionContext;
import org.apache.ignite.internal.processors.odbc.SqlListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.SqlListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequestHandler;
import org.apache.ignite.internal.util.GridSpinBusyLock;

/**
 * ODBC Connection Context.
 */
public class OdbcConnectionContext implements SqlListenerConnectionContext {
    /** Version 2.1.0. */
    private static final SqlListenerProtocolVersion VER_2_1_0 = SqlListenerProtocolVersion.create(2, 1, 0);

    /** Version 2.1.5: added "lazy" flag. */
    private static final SqlListenerProtocolVersion VER_2_1_5 = SqlListenerProtocolVersion.create(2, 1, 5);

    /** Current version. */
    private static final SqlListenerProtocolVersion CURRENT_VER = VER_2_1_5;

    /** Supported versions. */
    private static final Set<SqlListenerProtocolVersion> SUPPORTED_VERS = new HashSet<>();

    /** Context. */
    private final GridKernalContext ctx;

    /** Shutdown busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Message parser. */
    private OdbcMessageParser parser = null;

    /** Request handler. */
    private OdbcRequestHandler handler = null;

    static {
        SUPPORTED_VERS.add(CURRENT_VER);
        SUPPORTED_VERS.add(VER_2_1_0);
    }

    /**
     * Constructor.
     * @param ctx Kernal Context.
     * @param busyLock Shutdown busy lock.
     * @param maxCursors Maximum allowed cursors.
     */
    public OdbcConnectionContext(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;
    }

    /** {@inheritDoc} */
    @Override public boolean isVersionSupported(SqlListenerProtocolVersion ver) {
        return SUPPORTED_VERS.contains(ver);
    }

    /** {@inheritDoc} */
    @Override public SqlListenerProtocolVersion currentVersion() {
        return CURRENT_VER;
    }

    /** {@inheritDoc} */
    @Override public void initializeFromHandshake(SqlListenerProtocolVersion ver, BinaryReaderExImpl reader) {
        assert SUPPORTED_VERS.contains(ver): "Unsupported ODBC protocol version.";

        boolean distributedJoins = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();
        boolean replicatedOnly = reader.readBoolean();
        boolean collocated = reader.readBoolean();
        boolean lazy = false;

        if (ver.compareTo(VER_2_1_5) >= 0)
            lazy = reader.readBoolean();

        handler = new OdbcRequestHandler(ctx, busyLock, maxCursors, distributedJoins,
                enforceJoinOrder, replicatedOnly, collocated, lazy);

        parser = new OdbcMessageParser(ctx);
    }

    /** {@inheritDoc} */
    @Override public SqlListenerRequestHandler handler() {
        return handler;
    }

    /** {@inheritDoc} */
    @Override public SqlListenerMessageParser parser() {
        return parser;
    }
}
