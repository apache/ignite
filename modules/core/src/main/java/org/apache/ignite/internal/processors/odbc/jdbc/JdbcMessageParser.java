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
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryStreams;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.ClientMessage;

/**
 * JDBC message parser.
 */
public class JdbcMessageParser implements ClientListenerMessageParser {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Protocol context. */
    private final JdbcProtocolContext protoCtx;

    /** Initial output stream capacity. */
    protected static final int INIT_CAP = 1024;

    /** Binary context. */
    private BinaryContext binCtx;

    /**
     * @param ctx Context.
     * @param protoCtx Protocol context.
     */
    public JdbcMessageParser(GridKernalContext ctx, JdbcProtocolContext protoCtx) {
        this.ctx = ctx;
        this.protoCtx = protoCtx;
        this.binCtx = ((CacheObjectBinaryProcessorImpl)ctx.cacheObjects()).marshaller().context();
    }

    /**
     * @param msg Message.
     * @return Reader.
     */
    protected BinaryRawReaderEx createReader(ClientMessage msg) {
        BinaryInputStream stream = BinaryStreams.inputStream(msg.payload());

        return new BinaryReaderExImpl(binCtx, stream, ctx.config().getClassLoader(), true);
    }

    /**
     * @param cap Capacisty
     * @return Writer.
     */
    protected BinaryWriterExImpl createWriter(int cap) {
        return new BinaryWriterExImpl(binCtx, BinaryStreams.outputStream(cap), null);
    }

    /** {@inheritDoc} */
    @Override public ClientListenerRequest decode(ClientMessage msg) {
        assert msg != null;

        BinaryRawReaderEx reader = createReader(msg);

        return JdbcRequest.readRequest(reader, protoCtx);
    }

    /** {@inheritDoc} */
    @Override public ClientMessage encode(ClientListenerResponse msg) {
        assert msg != null;

        assert msg instanceof JdbcResponse;

        JdbcResponse res = (JdbcResponse)msg;

        BinaryWriterExImpl writer = createWriter(INIT_CAP);

        res.writeBinary(writer, protoCtx);

        return new ClientMessage(writer.array());
    }

    /** {@inheritDoc} */
    @Override public int decodeCommandType(ClientMessage msg) {
        assert msg != null;

        return JdbcRequest.readType(msg.payload());
    }

    /** {@inheritDoc} */
    @Override public long decodeRequestId(ClientMessage msg) {
        assert msg != null;

        return JdbcRequest.readRequestId(msg.payload());
    }
}
