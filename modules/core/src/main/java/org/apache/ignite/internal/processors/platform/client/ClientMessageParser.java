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
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.platform.client.binary.ClientBinaryTypeGetRequest;
import org.apache.ignite.internal.processors.platform.client.binary.ClientBinaryTypeNameGetRequest;
import org.apache.ignite.internal.processors.platform.client.binary.ClientBinaryTypeNamePutRequest;
import org.apache.ignite.internal.processors.platform.client.binary.ClientBinaryTypePutRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheScanQueryNextPageRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheScanQueryRequest;

/**
 * Thin client message parser.
 */
public class ClientMessageParser implements ClientListenerMessageParser {
    /** */
    private static final short OP_CACHE_GET = 1;

    /** */
    private static final short OP_GET_BINARY_TYPE_NAME = 2;

    /** */
    private static final short OP_GET_BINARY_TYPE = 3;

    /** */
    private static final short OP_CACHE_PUT = 4;

    /** */
    private static final short OP_REGISTER_BINARY_TYPE_NAME = 5;

    /** */
    private static final short OP_PUT_BINARY_TYPE = 6;

    /** */
    private static final short OP_QUERY_SCAN = 7;

    /** */
    private static final short OP_QUERY_SCAN_CURSOR_GET_PAGE = 8;

    /** */
    private static final short OP_RESOURCE_CLOSE = 9;

    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /**
     * Ctor.
     *
     * @param ctx Kernal context.
     */
    ClientMessageParser(GridKernalContext ctx) {
        assert ctx != null;

        CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();
        marsh = cacheObjProc.marshaller();
    }

    /** {@inheritDoc} */
    @Override public ClientListenerRequest decode(byte[] msg) {
        assert msg != null;

        BinaryInputStream inStream = new BinaryHeapInputStream(msg);
        BinaryRawReaderEx reader = marsh.reader(inStream);

        return decode(reader);
    }

    /**
     * Decodes the request.
     *
     * @param reader Reader.
     * @return Request.
     */
    public ClientListenerRequest decode(BinaryRawReaderEx reader) {
        short opCode = reader.readShort();

        switch (opCode) {
            case OP_CACHE_GET:
                return new ClientCacheGetRequest(reader);

            case OP_GET_BINARY_TYPE_NAME:
                return new ClientBinaryTypeNameGetRequest(reader);

            case OP_GET_BINARY_TYPE:
                return new ClientBinaryTypeGetRequest(reader);

            case OP_CACHE_PUT:
                return new ClientCachePutRequest(reader);

            case OP_REGISTER_BINARY_TYPE_NAME:
                return new ClientBinaryTypeNamePutRequest(reader);

            case OP_PUT_BINARY_TYPE:
                return new ClientBinaryTypePutRequest(reader);

            case OP_QUERY_SCAN:
                return new ClientCacheScanQueryRequest(reader);

            case OP_QUERY_SCAN_CURSOR_GET_PAGE:
                return new ClientCacheScanQueryNextPageRequest(reader);

            case OP_RESOURCE_CLOSE:
                return new ClientResourceCloseRequest(reader);
        }

        return new ClientRawRequest(reader.readLong(), ClientStatus.INVALID_OP_CODE,
            "Invalid request op code: " + opCode);
    }

    /** {@inheritDoc} */
    @Override public byte[] encode(ClientListenerResponse resp) {
        assert resp != null;

        BinaryHeapOutputStream outStream = new BinaryHeapOutputStream(32);

        BinaryRawWriterEx writer = marsh.writer(outStream);

        ((ClientResponse)resp).encode(writer);

        return outStream.arrayCopy();
    }
}
