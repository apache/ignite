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
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheClearKeyRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheClearKeysRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheClearRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheContainsKeyRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheContainsKeysRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAllRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndPutIfAbsentRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndPutRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndRemoveRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndReplaceRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetSizeRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutAllRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutIfAbsentRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveIfEqualsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveAllRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveKeysRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveKeyRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheReplaceIfEqualsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheReplaceRequest;
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

    /** */
    private static final short OP_CACHE_CONTAINS_KEY = 10;

    /** */
    private static final short OP_CACHE_CONTAINS_KEYS = 11;

    /** */
    private static final short OP_CACHE_GET_ALL = 12;

    /** */
    private static final short OP_CACHE_GET_AND_PUT = 13;

    /** */
    private static final short OP_CACHE_GET_AND_REPLACE = 14;

    /** */
    private static final short OP_CACHE_GET_AND_REMOVE = 15;

    /** */
    private static final short OP_CACHE_PUT_IF_ABSENT = 16;

    /** */
    private static final short OP_CACHE_GET_AND_PUT_IF_ABSENT = 17;

    /** */
    private static final short OP_CACHE_REPLACE = 18;

    /** */
    private static final short OP_CACHE_REPLACE_IF_EQUALS = 19;

    /** */
    private static final short OP_CACHE_PUT_ALL = 20;

    /** */
    private static final short OP_CACHE_CLEAR = 21;

    /** */
    private static final short OP_CACHE_CLEAR_KEY = 22;

    /** */
    private static final short OP_CACHE_CLEAR_KEYS = 23;

    /** */
    private static final short OP_CACHE_REMOVE_KEY = 24;

    /** */
    private static final short OP_CACHE_REMOVE_IF_EQUALS = 25;

    /** */
    private static final short OP_CACHE_GET_SIZE = 26;

    /** */
    private static final short OP_CACHE_REMOVE_KEYS = 27;

    /** */
    private static final short OP_CACHE_REMOVE_ALL = 28;

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

            case OP_CACHE_CONTAINS_KEY:
                return new ClientCacheContainsKeyRequest(reader);

            case OP_CACHE_CONTAINS_KEYS:
                return new ClientCacheContainsKeysRequest(reader);

            case OP_CACHE_GET_ALL:
                return new ClientCacheGetAllRequest(reader);

            case OP_CACHE_GET_AND_PUT:
                return new ClientCacheGetAndPutRequest(reader);

            case OP_CACHE_GET_AND_REPLACE:
                return new ClientCacheGetAndReplaceRequest(reader);

            case OP_CACHE_GET_AND_REMOVE:
                return new ClientCacheGetAndRemoveRequest(reader);

            case OP_CACHE_PUT_IF_ABSENT:
                return new ClientCachePutIfAbsentRequest(reader);

            case OP_CACHE_GET_AND_PUT_IF_ABSENT:
                return new ClientCacheGetAndPutIfAbsentRequest(reader);

            case OP_CACHE_REPLACE:
                return new ClientCacheReplaceRequest(reader);

            case OP_CACHE_REPLACE_IF_EQUALS:
                return new ClientCacheReplaceIfEqualsRequest(reader);

            case OP_CACHE_PUT_ALL:
                return new ClientCachePutAllRequest(reader);

            case OP_CACHE_CLEAR:
                return new ClientCacheClearRequest(reader);

            case OP_CACHE_CLEAR_KEY:
                return new ClientCacheClearKeyRequest(reader);

            case OP_CACHE_CLEAR_KEYS:
                return new ClientCacheClearKeysRequest(reader);

            case OP_CACHE_REMOVE_KEY:
                return new ClientCacheRemoveKeyRequest(reader);

            case OP_CACHE_REMOVE_IF_EQUALS:
                return new ClientCacheRemoveIfEqualsRequest(reader);

            case OP_CACHE_GET_SIZE:
                return new ClientCacheGetSizeRequest(reader);

            case OP_CACHE_REMOVE_KEYS:
                return new ClientCacheRemoveKeysRequest(reader);

            case OP_CACHE_REMOVE_ALL:
                return new ClientCacheRemoveAllRequest(reader);
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
