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

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;

/**
 * JDBC message parser.
 */
public class JdbcMessageParser implements ClientListenerMessageParser {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Client protocol version. */
    private final ClientListenerProtocolVersion ver;

    /** Initial output stream capacity. */
    protected static final int INIT_CAP = 1024;

    /**
     * @param ctx Context.
     * @param ver Client protocol version.
     */
    public JdbcMessageParser(GridKernalContext ctx,
        ClientListenerProtocolVersion ver) {
        this.ctx = ctx;
        this.ver = ver;
    }

    /**
     * @param msg Message.
     * @return Reader.
     */
    protected BinaryReaderExImpl createReader(byte[] msg) {
        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        return new BinaryReaderExImpl(null, stream, ctx.config().getClassLoader(), true);
    }

    /**
     * @param cap Capacisty
     * @return Writer.
     */
    protected BinaryWriterExImpl createWriter(int cap) {
        return new BinaryWriterExImpl(null, new BinaryHeapOutputStream(cap), null, null);
    }

    /** {@inheritDoc} */
    @Override public ClientListenerRequest decode(byte[] msg) {
        assert msg != null;

        BinaryReaderExImpl reader = createReader(msg);

        return JdbcRequest.readRequest(reader, ver);
    }

    /** {@inheritDoc} */
    @Override public byte[] encode(ClientListenerResponse msg) {
        assert msg != null;

        assert msg instanceof JdbcResponse;

        JdbcResponse res = (JdbcResponse)msg;

        BinaryWriterExImpl writer = createWriter(INIT_CAP);

        res.writeBinary(writer, ver);

        return writer.array();
    }}
