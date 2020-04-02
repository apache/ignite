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

import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
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
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheCreateWithConfigurationRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheCreateWithNameRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheDestroyRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAllRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndPutIfAbsentRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndPutRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndRemoveRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndReplaceRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetConfigurationRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetNamesRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetOrCreateWithConfigurationRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetOrCreateWithNameRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetSizeRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheLocalPeekRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheNodePartitionsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePartitionsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutAllRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutIfAbsentRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheQueryNextPageRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveAllRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveIfEqualsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveKeyRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveKeysRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheReplaceIfEqualsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheReplaceRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheScanQueryRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheSqlFieldsQueryRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheSqlQueryRequest;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxEndRequest;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxStartRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterChangeStateRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterIsActiveRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterWalChangeStateRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterWalGetStateRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterGroupGetNodeIdsRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterGroupGetNodesDetailsRequest;

/**
 * Thin client message parser.
 */
public class ClientMessageParser implements ClientListenerMessageParser {
    /* General-purpose operations. */
    /** */
    private static final short OP_RESOURCE_CLOSE = 0;

    /* Cache operations */
    /** */
    private static final short OP_CACHE_GET = 1000;

    /** */
    private static final short OP_CACHE_PUT = 1001;

    /** */
    private static final short OP_CACHE_PUT_IF_ABSENT = 1002;

    /** */
    private static final short OP_CACHE_GET_ALL = 1003;

    /** */
    private static final short OP_CACHE_PUT_ALL = 1004;

    /** */
    private static final short OP_CACHE_GET_AND_PUT = 1005;

    /** */
    private static final short OP_CACHE_GET_AND_REPLACE = 1006;

    /** */
    private static final short OP_CACHE_GET_AND_REMOVE = 1007;

    /** */
    private static final short OP_CACHE_GET_AND_PUT_IF_ABSENT = 1008;

    /** */
    private static final short OP_CACHE_REPLACE = 1009;

    /** */
    private static final short OP_CACHE_REPLACE_IF_EQUALS = 1010;

    /** */
    private static final short OP_CACHE_CONTAINS_KEY = 1011;

    /** */
    private static final short OP_CACHE_CONTAINS_KEYS = 1012;

    /** */
    private static final short OP_CACHE_CLEAR = 1013;

    /** */
    private static final short OP_CACHE_CLEAR_KEY = 1014;

    /** */
    private static final short OP_CACHE_CLEAR_KEYS = 1015;

    /** */
    private static final short OP_CACHE_REMOVE_KEY = 1016;

    /** */
    private static final short OP_CACHE_REMOVE_IF_EQUALS = 1017;

    /** */
    private static final short OP_CACHE_REMOVE_KEYS = 1018;

    /** */
    private static final short OP_CACHE_REMOVE_ALL = 1019;

    /** */
    private static final short OP_CACHE_GET_SIZE = 1020;

    /** */
    private static final short OP_CACHE_LOCAL_PEEK = 1021;

    /* Cache create / destroy, configuration. */
    /** */
    private static final short OP_CACHE_GET_NAMES = 1050;

    /** */
    private static final short OP_CACHE_CREATE_WITH_NAME = 1051;

    /** */
    private static final short OP_CACHE_GET_OR_CREATE_WITH_NAME = 1052;

    /** */
    private static final short OP_CACHE_CREATE_WITH_CONFIGURATION = 1053;

    /** */
    private static final short OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION = 1054;

    /** */
    private static final short OP_CACHE_GET_CONFIGURATION = 1055;

    /** */
    private static final short OP_CACHE_DESTROY = 1056;

    /* Cache service info. */

    /** Deprecated since 1.3.0. Replaced by OP_CACHE_PARTITIONS. */
    private static final short OP_CACHE_NODE_PARTITIONS = 1100;

    /** */
    private static final short OP_CACHE_PARTITIONS = 1101;

    /* Query operations. */
    /** */
    private static final short OP_QUERY_SCAN = 2000;

    /** */
    private static final short OP_QUERY_SCAN_CURSOR_GET_PAGE = 2001;

    /** */
    private static final short OP_QUERY_SQL = 2002;

    /** */
    private static final short OP_QUERY_SQL_CURSOR_GET_PAGE = 2003;

    /** */
    private static final short OP_QUERY_SQL_FIELDS = 2004;

    /** */
    private static final short OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE = 2005;

    /* Binary metadata operations. */
    /** */
    private static final short OP_BINARY_TYPE_NAME_GET = 3000;

    /** */
    private static final short OP_BINARY_TYPE_NAME_PUT = 3001;

    /** */
    private static final short OP_BINARY_TYPE_GET = 3002;

    /** */
    private static final short OP_BINARY_TYPE_PUT = 3003;

    /** Start new transaction. */
    private static final short OP_TX_START = 4000;

    /** Commit transaction. */
    private static final short OP_TX_END = 4001;

    /* Cluster operations. */
    /** */
    private static final short OP_CLUSTER_IS_ACTIVE = 5000;

    /** */
    private static final short OP_CLUSTER_CHANGE_STATE = 5001;

    /** */
    private static final short OP_CLUSTER_CHANGE_WAL_STATE = 5002;

    /** */
    private static final short OP_CLUSTER_GET_WAL_STATE = 5003;

    /** */
    private static final short OP_CLUSTER_GROUP_GET_NODE_IDS = 5100;

    /** */
    private static final short OP_CLUSTER_GROUP_GET_NODE_INFO = 5101;

    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /** Client connection context */
    private final ClientConnectionContext ctx;

    /** Client protocol context */
    private final ClientProtocolContext protocolContext;

    /**
     * @param ctx Client connection context.
     */
    ClientMessageParser(ClientConnectionContext ctx, ClientProtocolContext protocolContext) {
        assert ctx != null;
        assert protocolContext != null;

        this.ctx = ctx;
        this.protocolContext = protocolContext;

        CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.kernalContext().cacheObjects();
        marsh = cacheObjProc.marshaller();
    }

    /** {@inheritDoc} */
    @Override public ClientListenerRequest decode(byte[] msg) {
        assert msg != null;

        BinaryInputStream inStream = new BinaryHeapInputStream(msg);

        // skipHdrCheck must be true (we have 103 op code).
        BinaryRawReaderEx reader = new BinaryReaderExImpl(marsh.context(), inStream,
                null, null, true, true);

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

            case OP_BINARY_TYPE_NAME_GET:
                return new ClientBinaryTypeNameGetRequest(reader);

            case OP_BINARY_TYPE_GET:
                return new ClientBinaryTypeGetRequest(reader);

            case OP_CACHE_PUT:
                return new ClientCachePutRequest(reader);

            case OP_BINARY_TYPE_NAME_PUT:
                return new ClientBinaryTypeNamePutRequest(reader);

            case OP_BINARY_TYPE_PUT:
                return new ClientBinaryTypePutRequest(reader);

            case OP_QUERY_SCAN:
                return new ClientCacheScanQueryRequest(reader);

            case OP_QUERY_SCAN_CURSOR_GET_PAGE:

            case OP_QUERY_SQL_CURSOR_GET_PAGE:
                return new ClientCacheQueryNextPageRequest(reader);

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

            case OP_CACHE_LOCAL_PEEK:
                return new ClientCacheLocalPeekRequest(reader);

            case OP_CACHE_REMOVE_ALL:
                return new ClientCacheRemoveAllRequest(reader);

            case OP_CACHE_CREATE_WITH_NAME:
                return new ClientCacheCreateWithNameRequest(reader);

            case OP_CACHE_GET_OR_CREATE_WITH_NAME:
                return new ClientCacheGetOrCreateWithNameRequest(reader);

            case OP_CACHE_DESTROY:
                return new ClientCacheDestroyRequest(reader);

            case OP_CACHE_NODE_PARTITIONS:
                return new ClientCacheNodePartitionsRequest(reader);

            case OP_CACHE_PARTITIONS:
                return new ClientCachePartitionsRequest(reader);

            case OP_CACHE_GET_NAMES:
                return new ClientCacheGetNamesRequest(reader);

            case OP_CACHE_GET_CONFIGURATION:
                return new ClientCacheGetConfigurationRequest(reader, protocolContext);

            case OP_CACHE_CREATE_WITH_CONFIGURATION:
                return new ClientCacheCreateWithConfigurationRequest(reader, protocolContext);

            case OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION:
                return new ClientCacheGetOrCreateWithConfigurationRequest(reader, protocolContext);

            case OP_QUERY_SQL:
                return new ClientCacheSqlQueryRequest(reader);

            case OP_QUERY_SQL_FIELDS:
                return new ClientCacheSqlFieldsQueryRequest(reader);

            case OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE:
                return new ClientCacheQueryNextPageRequest(reader);

            case OP_TX_START:
                return new ClientTxStartRequest(reader);

            case OP_TX_END:
                return new ClientTxEndRequest(reader);

            case OP_CLUSTER_IS_ACTIVE:
                return new ClientClusterIsActiveRequest(reader);

            case OP_CLUSTER_CHANGE_STATE:
                return new ClientClusterChangeStateRequest(reader);

            case OP_CLUSTER_CHANGE_WAL_STATE:
                return new ClientClusterWalChangeStateRequest(reader);

            case OP_CLUSTER_GET_WAL_STATE:
                return new ClientClusterWalGetStateRequest(reader);

            case OP_CLUSTER_GROUP_GET_NODE_IDS:
                return new ClientClusterGroupGetNodeIdsRequest(reader);

            case OP_CLUSTER_GROUP_GET_NODE_INFO:
                return new ClientClusterGroupGetNodesDetailsRequest(reader);
        }

        return new ClientRawRequest(reader.readLong(), ClientStatus.INVALID_OP_CODE,
            "Invalid request op code: " + opCode);
    }

    /** {@inheritDoc} */
    @Override public byte[] encode(ClientListenerResponse resp) {
        assert resp != null;

        BinaryHeapOutputStream outStream = new BinaryHeapOutputStream(32);

        BinaryRawWriterEx writer = marsh.writer(outStream);

        ((ClientResponse)resp).encode(ctx, writer);

        return outStream.arrayCopy();
    }

    /** {@inheritDoc} */
    @Override public int decodeCommandType(byte[] msg) {
        assert msg != null;

        BinaryInputStream inStream = new BinaryHeapInputStream(msg);

        return inStream.readShort();
    }

    /** {@inheritDoc} */
    @Override public long decodeRequestId(byte[] msg) {
        return 0;
    }
}
