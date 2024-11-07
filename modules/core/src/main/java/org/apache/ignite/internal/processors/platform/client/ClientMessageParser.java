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

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryMemoryAllocator;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.ClientMessage;
import org.apache.ignite.internal.processors.platform.client.binary.ClientBinaryConfigurationGetRequest;
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
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheIndexQueryRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheInvokeAllRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheInvokeRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheLocalPeekRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheNodePartitionsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePartitionsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutAllConflictRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutAllRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutIfAbsentRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheQueryContinuousRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheQueryNextPageRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveAllConflictRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveAllRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveIfEqualsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveKeyRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveKeysRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheReplaceIfEqualsRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheReplaceRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheScanQueryRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheSqlFieldsQueryRequest;
import org.apache.ignite.internal.processors.platform.client.cache.ClientCacheSqlQueryRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterChangeStateRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterGetStateRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterGroupGetNodeIdsRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterGroupGetNodesDetailsRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterGroupGetNodesEndpointsRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterWalChangeStateRequest;
import org.apache.ignite.internal.processors.platform.client.cluster.ClientClusterWalGetStateRequest;
import org.apache.ignite.internal.processors.platform.client.compute.ClientExecuteTaskRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientAtomicLongCreateRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientAtomicLongExistsRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientAtomicLongRemoveRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientAtomicLongValueAddAndGetRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientAtomicLongValueCompareAndSetAndGetRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientAtomicLongValueCompareAndSetRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientAtomicLongValueGetAndSetRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientAtomicLongValueGetRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetClearRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetCloseRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetExistsRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetGetOrCreateRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetIteratorGetPageRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetIteratorStartRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetSizeRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetValueAddAllRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetValueAddRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetValueContainsAllRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetValueContainsRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetValueRemoveAllRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetValueRemoveRequest;
import org.apache.ignite.internal.processors.platform.client.datastructures.ClientIgniteSetValueRetainAllRequest;
import org.apache.ignite.internal.processors.platform.client.service.ClientServiceGetDescriptorRequest;
import org.apache.ignite.internal.processors.platform.client.service.ClientServiceGetDescriptorsRequest;
import org.apache.ignite.internal.processors.platform.client.service.ClientServiceInvokeRequest;
import org.apache.ignite.internal.processors.platform.client.service.ClientServiceTopologyRequest;
import org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerAddDataRequest;
import org.apache.ignite.internal.processors.platform.client.streamer.ClientDataStreamerStartRequest;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxEndRequest;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxStartRequest;

/**
 * Thin client message parser.
 */
public class ClientMessageParser implements ClientListenerMessageParser {
    /* General-purpose operations. */
    /** */
    private static final short OP_RESOURCE_CLOSE = 0;

    /** */
    private static final short OP_HEARTBEAT = 1;

    /** */
    private static final short OP_GET_IDLE_TIMEOUT = 2;

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

    /** */
    private static final short OP_CACHE_PUT_ALL_CONFLICT = 1022;

    /** */
    private static final short OP_CACHE_REMOVE_ALL_CONFLICT = 1023;

    /** */
    private static final short OP_CACHE_INVOKE = 1024;

    /** */
    private static final short OP_CACHE_INVOKE_ALL = 1025;

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

    /** */
    private static final short OP_QUERY_CONTINUOUS = 2006;

    /** */
    public static final short OP_QUERY_CONTINUOUS_EVENT_NOTIFICATION = 2007;

    /** */
    private static final short OP_QUERY_INDEX = 2008;

    /** */
    private static final short OP_QUERY_INDEX_CURSOR_GET_PAGE = 2009;

    /* Binary metadata operations. */
    /** */
    private static final short OP_BINARY_TYPE_NAME_GET = 3000;

    /** */
    private static final short OP_BINARY_TYPE_NAME_PUT = 3001;

    /** */
    private static final short OP_BINARY_TYPE_GET = 3002;

    /** */
    private static final short OP_BINARY_TYPE_PUT = 3003;

    /** */
    private static final short OP_BINARY_CONFIGURATION_GET = 3004;

    /** Start new transaction. */
    private static final short OP_TX_START = 4000;

    /** Commit transaction. */
    private static final short OP_TX_END = 4001;

    /* Cluster operations. */
    /** */
    private static final short OP_CLUSTER_GET_STATE = 5000;

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

    /** */
    private static final short OP_CLUSTER_GROUP_GET_NODE_ENDPOINTS = 5102;

    /* Compute operations. */
    /** */
    private static final short OP_COMPUTE_TASK_EXECUTE = 6000;

    /** */
    public static final short OP_COMPUTE_TASK_FINISHED = 6001;

    /** Service invocation. */
    private static final short OP_SERVICE_INVOKE = 7000;

    /** Get service descriptors. */
    private static final short OP_SERVICE_GET_DESCRIPTORS = 7001;

    /** Get service descriptor. */
    private static final short OP_SERVICE_GET_DESCRIPTOR = 7002;

    /** Data streamers. */
    /** */
    private static final short OP_DATA_STREAMER_START = 8000;

    /** */
    private static final short OP_DATA_STREAMER_ADD_DATA = 8001;

    /** Data structures. */
    /** Create an AtomicLong. */
    private static final short OP_ATOMIC_LONG_CREATE = 9000;

    /** Remove an AtomicLong. */
    private static final short OP_ATOMIC_LONG_REMOVE = 9001;

    /** Check if AtomicLong exists. */
    private static final short OP_ATOMIC_LONG_EXISTS = 9002;

    /** AtomicLong.get. */
    private static final short OP_ATOMIC_LONG_VALUE_GET = 9003;

    /** AtomicLong.addAndGet (also covers incrementAndGet, getAndIncrement, getAndAdd, decrementAndGet, getAndDecrement).  */
    private static final short OP_ATOMIC_LONG_VALUE_ADD_AND_GET = 9004;

    /** AtomicLong.getAndSet. */
    private static final short OP_ATOMIC_LONG_VALUE_GET_AND_SET = 9005;

    /** AtomicLong.compareAndSet. */
    private static final short OP_ATOMIC_LONG_VALUE_COMPARE_AND_SET = 9006;

    /** AtomicLong.compareAndSetAndGet. */
    private static final short OP_ATOMIC_LONG_VALUE_COMPARE_AND_SET_AND_GET = 9007;

    /** Create an IgniteSet. */
    private static final short OP_SET_GET_OR_CREATE = 9010;

    /** Remove an IgniteSet. */
    private static final short OP_SET_CLOSE = 9011;

    /** IgniteSet.removed. */
    private static final short OP_SET_EXISTS = 9012;

    /** IgniteSet.add. */
    private static final short OP_SET_VALUE_ADD = 9013;

    /** IgniteSet.addAll. */
    private static final short OP_SET_VALUE_ADD_ALL = 9014;

    /** IgniteSet.remove. */
    private static final short OP_SET_VALUE_REMOVE = 9015;

    /** IgniteSet.removeAll. */
    private static final short OP_SET_VALUE_REMOVE_ALL = 9016;

    /** IgniteSet.contains. */
    private static final short OP_SET_VALUE_CONTAINS = 9017;

    /** IgniteSet.containsAll. */
    private static final short OP_SET_VALUE_CONTAINS_ALL = 9018;

    /** IgniteSet.retainAll. */
    private static final short OP_SET_VALUE_RETAIN_ALL = 9019;

    /** IgniteSet.size. */
    private static final short OP_SET_SIZE = 9020;

    /** IgniteSet.clear. */
    private static final short OP_SET_CLEAR = 9021;

    /** IgniteSet.iterator. */
    private static final short OP_SET_ITERATOR_START = 9022;

    /** IgniteSet.iterator page. */
    private static final short OP_SET_ITERATOR_GET_PAGE = 9023;

    /** Get service topology. */
    private static final short OP_SERVICE_GET_TOPOLOGY = 7003;

    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /** Client connection context */
    private final ClientConnectionContext ctx;

    /** Client protocol context */
    private final ClientProtocolContext protocolCtx;

    /**
     * @param ctx Client connection context.
     */
    ClientMessageParser(ClientConnectionContext ctx, ClientProtocolContext protocolCtx) {
        assert ctx != null;
        assert protocolCtx != null;

        this.ctx = ctx;
        this.protocolCtx = protocolCtx;

        CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.kernalContext().cacheObjects();
        marsh = cacheObjProc.marshaller();
    }

    /** {@inheritDoc} */
    @Override public ClientListenerRequest decode(ClientMessage msg) {
        assert msg != null;

        BinaryInputStream inStream = new BinaryHeapInputStream(msg.payload());

        // skipHdrCheck must be true (we have 103 op code).
        BinaryReaderExImpl reader = new BinaryReaderExImpl(marsh.context(), inStream,
                null, null, true, true);

        return decode(reader);
    }

    /**
     * Decodes the request.
     *
     * @param reader Reader.
     * @return Request.
     */
    public ClientListenerRequest decode(BinaryReaderExImpl reader) {
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

            case OP_BINARY_CONFIGURATION_GET:
                return new ClientBinaryConfigurationGetRequest(reader);

            case OP_QUERY_SCAN:
                return new ClientCacheScanQueryRequest(reader);

            case OP_QUERY_SCAN_CURSOR_GET_PAGE:

            case OP_QUERY_SQL_CURSOR_GET_PAGE:

            case OP_QUERY_INDEX_CURSOR_GET_PAGE:
                return new ClientCacheQueryNextPageRequest(reader);

            case OP_RESOURCE_CLOSE:
                return new ClientResourceCloseRequest(reader);

            case OP_HEARTBEAT:
                return new ClientRequest(reader);

            case OP_GET_IDLE_TIMEOUT:
                return new ClientGetIdleTimeoutRequest(reader);

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

            case OP_CACHE_PUT_ALL_CONFLICT:
                return new ClientCachePutAllConflictRequest(reader, ctx);

            case OP_CACHE_REMOVE_ALL_CONFLICT:
                return new ClientCacheRemoveAllConflictRequest(reader);

            case OP_CACHE_INVOKE:
                return new ClientCacheInvokeRequest(reader);

            case OP_CACHE_INVOKE_ALL:
                return new ClientCacheInvokeAllRequest(reader);

            case OP_CACHE_CREATE_WITH_NAME:
                return new ClientCacheCreateWithNameRequest(reader);

            case OP_CACHE_GET_OR_CREATE_WITH_NAME:
                return new ClientCacheGetOrCreateWithNameRequest(reader);

            case OP_CACHE_DESTROY:
                return new ClientCacheDestroyRequest(reader);

            case OP_CACHE_NODE_PARTITIONS:
                return new ClientCacheNodePartitionsRequest(reader);

            case OP_CACHE_PARTITIONS:
                return new ClientCachePartitionsRequest(reader, protocolCtx);

            case OP_CACHE_GET_NAMES:
                return new ClientCacheGetNamesRequest(reader);

            case OP_CACHE_GET_CONFIGURATION:
                return new ClientCacheGetConfigurationRequest(reader, protocolCtx);

            case OP_CACHE_CREATE_WITH_CONFIGURATION:
                return new ClientCacheCreateWithConfigurationRequest(reader, protocolCtx);

            case OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION:
                return new ClientCacheGetOrCreateWithConfigurationRequest(reader, protocolCtx);

            case OP_QUERY_SQL:
                return new ClientCacheSqlQueryRequest(reader);

            case OP_QUERY_SQL_FIELDS:
                return new ClientCacheSqlFieldsQueryRequest(reader, protocolCtx);

            case OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE:
                return new ClientCacheQueryNextPageRequest(reader);

            case OP_QUERY_CONTINUOUS:
                return new ClientCacheQueryContinuousRequest(reader);

            case OP_QUERY_INDEX:
                return new ClientCacheIndexQueryRequest(reader, protocolCtx);

            case OP_TX_START:
                return new ClientTxStartRequest(reader);

            case OP_TX_END:
                return new ClientTxEndRequest(reader);

            case OP_CLUSTER_GET_STATE:
                return new ClientClusterGetStateRequest(reader);

            case OP_CLUSTER_CHANGE_STATE:
                return new ClientClusterChangeStateRequest(reader, protocolCtx);

            case OP_CLUSTER_CHANGE_WAL_STATE:
                return new ClientClusterWalChangeStateRequest(reader);

            case OP_CLUSTER_GET_WAL_STATE:
                return new ClientClusterWalGetStateRequest(reader);

            case OP_CLUSTER_GROUP_GET_NODE_IDS:
                return new ClientClusterGroupGetNodeIdsRequest(reader);

            case OP_CLUSTER_GROUP_GET_NODE_INFO:
                return new ClientClusterGroupGetNodesDetailsRequest(reader);

            case OP_CLUSTER_GROUP_GET_NODE_ENDPOINTS:
                return new ClientClusterGroupGetNodesEndpointsRequest(reader);

            case OP_COMPUTE_TASK_EXECUTE:
                return new ClientExecuteTaskRequest(reader);

            case OP_SERVICE_INVOKE:
                return new ClientServiceInvokeRequest(reader, protocolCtx);

            case OP_SERVICE_GET_DESCRIPTORS:
                return new ClientServiceGetDescriptorsRequest(reader);

            case OP_SERVICE_GET_DESCRIPTOR:
                return new ClientServiceGetDescriptorRequest(reader);

            case OP_DATA_STREAMER_START:
                return new ClientDataStreamerStartRequest(reader);

            case OP_DATA_STREAMER_ADD_DATA:
                return new ClientDataStreamerAddDataRequest(reader);

            case OP_ATOMIC_LONG_CREATE:
                return new ClientAtomicLongCreateRequest(reader);

            case OP_ATOMIC_LONG_REMOVE:
                return new ClientAtomicLongRemoveRequest(reader);

            case OP_ATOMIC_LONG_EXISTS:
                return new ClientAtomicLongExistsRequest(reader);

            case OP_ATOMIC_LONG_VALUE_GET:
                return new ClientAtomicLongValueGetRequest(reader);

            case OP_ATOMIC_LONG_VALUE_ADD_AND_GET:
                return new ClientAtomicLongValueAddAndGetRequest(reader);

            case OP_ATOMIC_LONG_VALUE_GET_AND_SET:
                return new ClientAtomicLongValueGetAndSetRequest(reader);

            case OP_ATOMIC_LONG_VALUE_COMPARE_AND_SET:
                return new ClientAtomicLongValueCompareAndSetRequest(reader);

            case OP_ATOMIC_LONG_VALUE_COMPARE_AND_SET_AND_GET:
                return new ClientAtomicLongValueCompareAndSetAndGetRequest(reader);

            case OP_SET_GET_OR_CREATE:
                return new ClientIgniteSetGetOrCreateRequest(reader);

            case OP_SET_CLOSE:
                return new ClientIgniteSetCloseRequest(reader);

            case OP_SET_EXISTS:
                return new ClientIgniteSetExistsRequest(reader);

            case OP_SET_VALUE_ADD:
                return new ClientIgniteSetValueAddRequest(reader);

            case OP_SET_VALUE_ADD_ALL:
                return new ClientIgniteSetValueAddAllRequest(reader);

            case OP_SET_VALUE_REMOVE:
                return new ClientIgniteSetValueRemoveRequest(reader);

            case OP_SET_VALUE_REMOVE_ALL:
                return new ClientIgniteSetValueRemoveAllRequest(reader);

            case OP_SET_VALUE_CONTAINS:
                return new ClientIgniteSetValueContainsRequest(reader);

            case OP_SET_VALUE_CONTAINS_ALL:
                return new ClientIgniteSetValueContainsAllRequest(reader);

            case OP_SET_VALUE_RETAIN_ALL:
                return new ClientIgniteSetValueRetainAllRequest(reader);

            case OP_SET_SIZE:
                return new ClientIgniteSetSizeRequest(reader);

            case OP_SET_CLEAR:
                return new ClientIgniteSetClearRequest(reader);

            case OP_SET_ITERATOR_START:
                return new ClientIgniteSetIteratorStartRequest(reader);

            case OP_SET_ITERATOR_GET_PAGE:
                return new ClientIgniteSetIteratorGetPageRequest(reader);

            case OP_SERVICE_GET_TOPOLOGY:
                return new ClientServiceTopologyRequest(reader);
        }

        return new ClientRawRequest(reader.readLong(), ClientStatus.INVALID_OP_CODE,
            "Invalid request op code: " + opCode);
    }

    /** {@inheritDoc} */
    @Override public ClientMessage encode(ClientListenerResponse resp) {
        assert resp != null;

        BinaryHeapOutputStream outStream = new BinaryHeapOutputStream(32, BinaryMemoryAllocator.POOLED.chunk());

        BinaryRawWriterEx writer = marsh.writer(outStream);

        assert resp instanceof ClientOutgoingMessage : "Unexpected response type: " + resp.getClass();

        ((ClientOutgoingMessage)resp).encode(ctx, writer);

        return new ClientMessage(outStream);
    }

    /** {@inheritDoc} */
    @Override public int decodeCommandType(ClientMessage msg) {
        assert msg != null;

        BinaryInputStream inStream = new BinaryHeapInputStream(msg.payload());

        return inStream.readShort();
    }

    /** {@inheritDoc} */
    @Override public long decodeRequestId(ClientMessage msg) {
        return 0;
    }
}
