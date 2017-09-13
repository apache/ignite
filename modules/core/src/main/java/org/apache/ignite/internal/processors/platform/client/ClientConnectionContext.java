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

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerConnectionContext;
import org.apache.ignite.internal.processors.odbc.SqlListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.SqlListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequestHandler;

/**
 * Thin Client connection context.
 */
public class ClientConnectionContext implements SqlListenerConnectionContext {
    /** Version 1.0.0. */
    private static final SqlListenerProtocolVersion VER_1_0_0 = SqlListenerProtocolVersion.create(1, 0, 0);

    /** Message parser. */
    private final ClientMessageParser parser;

    /** Request handler. */
    private final ClientRequestHandler handler;

    /**
     * Ctor.
     *
     * @param ctx Kernal context.
     */
    public ClientConnectionContext(GridKernalContext ctx) {
        assert ctx != null;

        parser = new ClientMessageParser(ctx);
        handler = new ClientRequestHandler(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isVersionSupported(SqlListenerProtocolVersion ver) {
        return VER_1_0_0.equals(ver);
    }

    /** {@inheritDoc} */
    @Override public SqlListenerProtocolVersion currentVersion() {
        return VER_1_0_0;
    }

    /** {@inheritDoc} */
    @Override public void initializeFromHandshake(SqlListenerProtocolVersion ver, BinaryReaderExImpl reader) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public SqlListenerRequestHandler handler() {
        return handler;
    }

    /** {@inheritDoc} */
    @Override public SqlListenerMessageParser parser() {
        return parser;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected() {
        // No-op.
    }
}
