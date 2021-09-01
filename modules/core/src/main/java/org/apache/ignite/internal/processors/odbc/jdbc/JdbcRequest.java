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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestNoId;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC request.
 */
public class JdbcRequest extends ClientListenerRequestNoId implements JdbcRawBinarylizable {
    /** Execute sql query request. */
    public static final byte QRY_EXEC = 2;

    /** Fetch query results request. */
    static final byte QRY_FETCH = 3;

    /** Close query request. */
    static final byte QRY_CLOSE = 4;

    /** Get query columns metadata request. */
    public static final byte QRY_META = 5;

    /** Batch queries. */
    public static final byte BATCH_EXEC = 6;

    /** Get tables metadata request. */
    public static final byte META_TABLES = 7;

    /** Get columns metadata request. */
    public static final byte META_COLUMNS = 8;

    /** Get indexes metadata request. */
    public static final byte META_INDEXES = 9;

    /** Get SQL query parameters metadata request. */
    public static final byte META_PARAMS = 10;

    /** Get primary keys metadata request. */
    public static final byte META_PRIMARY_KEYS = 11;

    /** Get schemas metadata request. */
    public static final byte META_SCHEMAS = 12;

    /** Send a batch of a data from client to server. */
    static final byte BULK_LOAD_BATCH = 13;

    /** Ordered batch request. */
    static final byte BATCH_EXEC_ORDERED = 14;

    /** Execute cancel request. */
    static final byte QRY_CANCEL = 15;

    /** Get cache partitions distributions. */
    public static final byte CACHE_PARTITIONS = 16;

    /** Get binary type schema request. */
    public static final byte BINARY_TYPE_GET = 17;

    /** Update binary type schema request. */
    public static final byte BINARY_TYPE_PUT = 18;

    /** Get binary type name request. */
    public static final byte BINARY_TYPE_NAME_GET = 19;

    /** Update binary type name request. */
    public static final byte BINARY_TYPE_NAME_PUT = 20;

    /** Request Id generator. */
    private static final AtomicLong REQ_ID_GENERATOR = new AtomicLong();

    /** Request type. */
    private byte type;

    /** Request id. */
    private long reqId;

    /**
     * @param type Command type.
     */
    public JdbcRequest(byte type) {
        this.type = type;

        reqId = REQ_ID_GENERATOR.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(
        BinaryWriterExImpl writer,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        writer.writeByte(type);

        if (protoCtx.isAffinityAwarenessSupported())
            writer.writeLong(reqId);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {

        if (protoCtx.isAffinityAwarenessSupported())
            reqId = reader.readLong();
    }

    /** {@inheritDoc} */
    @Override public long requestId() {
        return reqId;
    }

    /**
     * @return Request type.
     */
    public byte type() {
        return type;
    }

    /**
     * @param reader Binary reader.
     * @param protoCtx Protocol context.
     * @return Request object.
     * @throws BinaryObjectException On error.
     */
    public static JdbcRequest readRequest(
        BinaryReaderExImpl reader,
        JdbcProtocolContext protoCtx
    ) throws BinaryObjectException {
        int reqType = reader.readByte();

        JdbcRequest req;

        switch (reqType) {
            case QRY_EXEC:
                req = new JdbcQueryExecuteRequest();

                break;

            case QRY_FETCH:
                req = new JdbcQueryFetchRequest();

                break;

            case QRY_META:
                req = new JdbcQueryMetadataRequest();

                break;

            case QRY_CLOSE:
                req = new JdbcQueryCloseRequest();

                break;

            case BATCH_EXEC:
                req = new JdbcBatchExecuteRequest();

                break;

            case META_TABLES:
                req = new JdbcMetaTablesRequest();

                break;

            case META_COLUMNS:
                req = new JdbcMetaColumnsRequest();

                break;

            case META_INDEXES:
                req = new JdbcMetaIndexesRequest();

                break;

            case META_PARAMS:
                req = new JdbcMetaParamsRequest();

                break;

            case META_PRIMARY_KEYS:
                req = new JdbcMetaPrimaryKeysRequest();

                break;

            case META_SCHEMAS:
                req = new JdbcMetaSchemasRequest();

                break;

            case BULK_LOAD_BATCH:
                req = new JdbcBulkLoadBatchRequest();

                break;

            case BATCH_EXEC_ORDERED:
                req = new JdbcOrderedBatchExecuteRequest();

                break;

            case QRY_CANCEL:
                req = new JdbcQueryCancelRequest();

                break;

            case CACHE_PARTITIONS:
                req = new JdbcCachePartitionsRequest();

                break;

            case BINARY_TYPE_NAME_PUT:
                req = new JdbcBinaryTypeNamePutRequest();

                break;

            case BINARY_TYPE_NAME_GET:
                req = new JdbcBinaryTypeNameGetRequest();

                break;

            case BINARY_TYPE_PUT:
                req = new JdbcBinaryTypePutRequest();

                break;

            case BINARY_TYPE_GET:
                req = new JdbcBinaryTypeGetRequest();

                break;

            default:
                throw new IgniteException("Unknown SQL listener request ID: [request ID=" + reqType + ']');
        }

        req.readBinary(reader, protoCtx);

        return req;
    }

    /**
     * Reads JdbcRequest command type.
     *
     * @param msg Jdbc request as byte array.
     * @return Command type.
     */
    public static byte readType(byte[] msg) {
        return msg[0];
    }

    /**
     * Reads JdbcRequest Id.
     *
     * @param msg Jdbc request as byte array.
     * @return Request Id.
     */
    public static long readRequestId(byte[] msg) {
        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        stream.position(1);

        return stream.readLong();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcRequest.class, this);
    }
}
